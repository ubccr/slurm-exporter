// Copyright 2020 University at Buffalo. All rights reserved.
//
// This file is part of SlurmExporter.
//
// SlurmExporter is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// SlurmExporter is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with SlurmExporter. If not, see <https://www.gnu.org/licenses/>.

package main

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"github.com/ubccr/go-slurmrest"
)

type SchedulerCollector struct {
	client            *slurmrest.APIClient
	threads           *prometheus.Desc
	queueSize         *prometheus.Desc
	lastCycle         *prometheus.Desc
	meanCycle         *prometheus.Desc
	cyclePerMinute    *prometheus.Desc
	backfillLastCycle *prometheus.Desc
	backfillMeanCycle *prometheus.Desc
	backfillDepthMean *prometheus.Desc
}

type diagMetrics struct {
	threads           float64
	queueSize         float64
	lastCycle         float64
	meanCycle         float64
	cyclePerMinute    float64
	backfillLastCycle float64
	backfillMeanCycle float64
	backfillDepthMean float64
}

func NewSchedulerCollector(client *slurmrest.APIClient) *SchedulerCollector {
	return &SchedulerCollector{
		client: client,
		threads: prometheus.NewDesc(
			"slurm_scheduler_threads",
			"Information provided by the Slurm sdiag command, number of scheduler threads ",
			nil,
			nil),
		queueSize: prometheus.NewDesc(
			"slurm_scheduler_queue_size",
			"Information provided by the Slurm sdiag command, length of the scheduler queue",
			nil,
			nil),
		lastCycle: prometheus.NewDesc(
			"slurm_scheduler_last_cycle",
			"Information provided by the Slurm sdiag command, scheduler last cycle time in (microseconds)",
			nil,
			nil),
		meanCycle: prometheus.NewDesc(
			"slurm_scheduler_mean_cycle",
			"Information provided by the Slurm sdiag command, scheduler mean cycle time in (microseconds)",
			nil,
			nil),
		cyclePerMinute: prometheus.NewDesc(
			"slurm_scheduler_cycle_per_minute",
			"Information provided by the Slurm sdiag command, number scheduler cycles per minute",
			nil,
			nil),
		backfillLastCycle: prometheus.NewDesc(
			"slurm_scheduler_backfill_last_cycle",
			"Information provided by the Slurm sdiag command, scheduler backfill last cycle time in (microseconds)",
			nil,
			nil),
		backfillMeanCycle: prometheus.NewDesc(
			"slurm_scheduler_backfill_mean_cycle",
			"Information provided by the Slurm sdiag command, scheduler backfill mean cycle time in (microseconds)",
			nil,
			nil),
		backfillDepthMean: prometheus.NewDesc(
			"slurm_scheduler_backfill_depth_mean",
			"Information provided by the Slurm sdiag command, scheduler backfill mean depth",
			nil,
			nil),
	}
}

func (sc *SchedulerCollector) metrics() *diagMetrics {
	var dm diagMetrics

	diag, resp, err := sc.client.DefaultApi.Diag(context.Background())
	if err != nil {
		log.Errorf("Failed to diag from slurm rest api: %s", err)
		return &dm
	} else if resp.StatusCode != 200 {
		log.WithFields(log.Fields{
			"status_code": resp.StatusCode,
		}).Error("HTTP response not OK while fetching diag from slurm rest api")
		return &dm
	}

	dm.threads = float64(diag.Statistics.ServerThreadCount)
	dm.queueSize = float64(diag.Statistics.AgentQueueSize)
	dm.lastCycle = float64(diag.Statistics.ScheduleCycleLast)
	dm.meanCycle = float64(diag.Statistics.ScheduleCycleSum) / float64(diag.Statistics.ScheduleCycleCounter)
	if diag.Statistics.ReqTime-diag.Statistics.ReqTimeStart > 60 {
		dm.cyclePerMinute = float64(diag.Statistics.ScheduleCycleCounter) / ((float64(diag.Statistics.ReqTime) - float64(diag.Statistics.ReqTimeStart)) / float64(60))
	}
	dm.backfillLastCycle = float64(diag.Statistics.BfCycleLast)
	dm.backfillMeanCycle = float64(diag.Statistics.BfCycleSum) / float64(diag.Statistics.BfCycleCounter)
	dm.backfillDepthMean = float64(diag.Statistics.BfDepthSum) / float64(diag.Statistics.BfCycleCounter)

	return &dm
}

func (sc *SchedulerCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- sc.threads
	ch <- sc.queueSize
	ch <- sc.lastCycle
	ch <- sc.meanCycle
	ch <- sc.cyclePerMinute
	ch <- sc.backfillLastCycle
	ch <- sc.backfillMeanCycle
	ch <- sc.backfillDepthMean
}

func (sc *SchedulerCollector) Collect(ch chan<- prometheus.Metric) {
	sm := sc.metrics()
	ch <- prometheus.MustNewConstMetric(sc.threads, prometheus.GaugeValue, sm.threads)
	ch <- prometheus.MustNewConstMetric(sc.queueSize, prometheus.GaugeValue, sm.queueSize)
	ch <- prometheus.MustNewConstMetric(sc.lastCycle, prometheus.GaugeValue, sm.lastCycle)
	ch <- prometheus.MustNewConstMetric(sc.meanCycle, prometheus.GaugeValue, sm.meanCycle)
	ch <- prometheus.MustNewConstMetric(sc.cyclePerMinute, prometheus.GaugeValue, sm.cyclePerMinute)
	ch <- prometheus.MustNewConstMetric(sc.backfillLastCycle, prometheus.GaugeValue, sm.backfillLastCycle)
	ch <- prometheus.MustNewConstMetric(sc.backfillMeanCycle, prometheus.GaugeValue, sm.backfillMeanCycle)
	ch <- prometheus.MustNewConstMetric(sc.backfillDepthMean, prometheus.GaugeValue, sm.backfillDepthMean)
}
