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
	"fmt"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/ubccr/slurmrest"
)

type SchedulerCollector struct {
	client            *slurmrest.APIClient
	logger            log.Logger
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

func NewSchedulerCollector(client *slurmrest.APIClient, logger log.Logger) *SchedulerCollector {
	return &SchedulerCollector{
		client: client,
		logger: log.With(logger, "collector", "scheduler"),
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

func (sc *SchedulerCollector) metrics() (*diagMetrics, error) {
	var dm diagMetrics

	req := sc.client.SlurmApi.SlurmctldDiag(context.Background())
	diag, resp, err := sc.client.SlurmApi.SlurmctldDiagExecute(req)
	if err != nil {
		level.Error(sc.logger).Log("msg", "Failed to diag from slurm rest api", "err", err)
		return &dm, err
	} else if resp.StatusCode != 200 {
		err = fmt.Errorf("HTTP response not OK while fetching diag from slurm rest api")
		level.Error(sc.logger).Log("err", err, "status_code", resp.StatusCode)
		return &dm, err
	} else if len(diag.GetErrors()) > 0 {
		for _, err := range diag.GetErrors() {
			level.Error(sc.logger).Log("err", err.GetError())
		}
		return &dm, fmt.Errorf("HTTP response contained %d errors", len(diag.GetErrors()))
	}

	dm.threads = float64(diag.Statistics.GetServerThreadCount())
	dm.queueSize = float64(diag.Statistics.GetAgentQueueSize())
	dm.lastCycle = float64(diag.Statistics.GetScheduleCycleLast())
	dm.meanCycle = float64(diag.Statistics.GetScheduleCycleMean())
	dm.cyclePerMinute = float64(diag.Statistics.GetScheduleCyclePerMinute())
	dm.backfillLastCycle = float64(diag.Statistics.GetBfCycleLast())
	dm.backfillMeanCycle = float64(diag.Statistics.GetBfCycleMean())
	dm.backfillDepthMean = float64(diag.Statistics.GetBfDepthMean())

	return &dm, nil
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
	var errValue float64
	sm, err := sc.metrics()
	if err != nil {
		errValue = 1
	}
	ch <- prometheus.MustNewConstMetric(sc.threads, prometheus.GaugeValue, sm.threads)
	ch <- prometheus.MustNewConstMetric(sc.queueSize, prometheus.GaugeValue, sm.queueSize)
	ch <- prometheus.MustNewConstMetric(sc.lastCycle, prometheus.GaugeValue, sm.lastCycle)
	ch <- prometheus.MustNewConstMetric(sc.meanCycle, prometheus.GaugeValue, sm.meanCycle)
	ch <- prometheus.MustNewConstMetric(sc.cyclePerMinute, prometheus.GaugeValue, sm.cyclePerMinute)
	ch <- prometheus.MustNewConstMetric(sc.backfillLastCycle, prometheus.GaugeValue, sm.backfillLastCycle)
	ch <- prometheus.MustNewConstMetric(sc.backfillMeanCycle, prometheus.GaugeValue, sm.backfillMeanCycle)
	ch <- prometheus.MustNewConstMetric(sc.backfillDepthMean, prometheus.GaugeValue, sm.backfillDepthMean)
	ch <- prometheus.MustNewConstMetric(collectError, prometheus.GaugeValue, errValue, "scheduler")
}
