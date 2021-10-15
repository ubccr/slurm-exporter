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

const (
	schedulerNamespace = "scheduler"
	rpcNamespace       = "rpc"
	userRpcNamespace   = "user_rpc"
)

type SchedulerCollector struct {
	client                         *slurmrest.APIClient
	logger                         log.Logger
	threads                        *prometheus.Desc
	agentQueueSize                 *prometheus.Desc
	agentCount                     *prometheus.Desc
	agentThreadCount               *prometheus.Desc
	dbdQueueSize                   *prometheus.Desc
	maxCycle                       *prometheus.Desc
	lastCycle                      *prometheus.Desc
	totalCycle                     *prometheus.Desc
	meanCycle                      *prometheus.Desc
	cycleMeanDepth                 *prometheus.Desc
	cyclePerMinute                 *prometheus.Desc
	queueLength                    *prometheus.Desc
	backfillLastCycle              *prometheus.Desc
	backfillMeanCycle              *prometheus.Desc
	backfillMaxCycle               *prometheus.Desc
	backfillDepthMean              *prometheus.Desc
	backfillDepthMeanTrySched      *prometheus.Desc
	backfillLastDepthCycle         *prometheus.Desc
	backfillLastDepthCycleTrySched *prometheus.Desc
	backfillLastQueueLength        *prometheus.Desc
	backfillQueueLengthMean        *prometheus.Desc
	backfillLastTableSize          *prometheus.Desc
	backfillMeanTableSize          *prometheus.Desc
	totalBackfilledJobsSinceStart  *prometheus.Desc
	totalBackfilledJobsSinceCycle  *prometheus.Desc
	totalBackfilledHeterogeneous   *prometheus.Desc
	rpcStatsCount                  *prometheus.Desc
	rpcStatsAvgTime                *prometheus.Desc
	rpcStatsTotalTime              *prometheus.Desc
	userRpcStatsCount              *prometheus.Desc
	userRpcStatsAvgTime            *prometheus.Desc
	userRpcStatsTotalTime          *prometheus.Desc
}

type diagMetrics struct {
	threads                        float64
	agentQueueSize                 float64
	agentCount                     float64
	agentThreadCount               float64
	dbdQueueSize                   float64
	maxCycle                       float64
	lastCycle                      float64
	totalCycle                     float64
	meanCycle                      float64
	cycleMeanDepth                 float64
	cyclePerMinute                 float64
	queueLength                    float64
	backfillLastCycle              float64
	backfillMeanCycle              float64
	backfillMaxCycle               float64
	backfillDepthMean              float64
	backfillDepthMeanTrySched      float64
	backfillLastDepthCycle         float64
	backfillLastDepthCycleTrySched float64
	backfillLastQueueLength        float64
	backfillQueueLengthMean        float64
	backfillLastTableSize          float64
	backfillMeanTableSize          float64
	totalBackfilledJobsSinceStart  float64
	totalBackfilledJobsSinceCycle  float64
	totalBackfilledHeterogeneous   float64
	rpcStats                       map[string]rpcStat
	userRpcStats                   map[string]rpcStat
}

func NewSchedulerCollector(client *slurmrest.APIClient, logger log.Logger) *SchedulerCollector {
	rpcStatsLabels := []string{"operation"}
	userRpcStatsLabels := []string{"user"}
	return &SchedulerCollector{
		client: client,
		logger: log.With(logger, "collector", "scheduler"),
		threads: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, schedulerNamespace, "threads"),
			"Information provided by the Slurm sdiag command, number of scheduler threads ",
			nil,
			nil),
		agentQueueSize: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, schedulerNamespace, "agent_queue_size"),
			"Information provided by the Slurm sdiag command, agent queue size",
			nil,
			nil),
		agentCount: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, schedulerNamespace, "agent_count"),
			"Information provided by the Slurm sdiag command, number of agent threads",
			nil,
			nil),
		agentThreadCount: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, schedulerNamespace, "agent_thread_count"),
			"Information provided by the Slurm sdiag command, active agent threads",
			nil,
			nil),
		dbdQueueSize: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, schedulerNamespace, "dbd_queue_size"),
			"Information provided by the Slurm sdiag command, length of the DBD agent queue",
			nil,
			nil),
		maxCycle: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, schedulerNamespace, "max_cycle"),
			"Information provided by the Slurm sdiag command, scheduler max cycle time in (microseconds)",
			nil,
			nil),
		lastCycle: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, schedulerNamespace, "last_cycle"),
			"Information provided by the Slurm sdiag command, scheduler last cycle time in (microseconds)",
			nil,
			nil),
		totalCycle: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, schedulerNamespace, "total_cycle"),
			"Information provided by the Slurm sdiag command, scheduler total cycle iterations",
			nil,
			nil),
		meanCycle: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, schedulerNamespace, "mean_cycle"),
			"Information provided by the Slurm sdiag command, scheduler mean cycle time in (microseconds)",
			nil,
			nil),
		cycleMeanDepth: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, schedulerNamespace, "cycle_mean_depth"),
			"Information provided by the Slurm sdiag command, average depth of schedule max cycle",
			nil,
			nil),
		cyclePerMinute: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, schedulerNamespace, "cycle_per_minute"),
			"Information provided by the Slurm sdiag command, number scheduler cycles per minute",
			nil,
			nil),
		queueLength: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, schedulerNamespace, "last_queue_length"),
			"Information provided by the Slurm sdiag command, main schedule last queue length",
			nil,
			nil),
		backfillLastCycle: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, schedulerNamespace, "backfill_last_cycle"),
			"Information provided by the Slurm sdiag command, scheduler backfill last cycle time in (microseconds)",
			nil,
			nil),
		backfillMeanCycle: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, schedulerNamespace, "backfill_mean_cycle"),
			"Information provided by the Slurm sdiag command, scheduler backfill mean cycle time in (microseconds)",
			nil,
			nil),
		backfillMaxCycle: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, schedulerNamespace, "backfill_max_cycle"),
			"Information provided by the Slurm sdiag command, scheduler backfill max cycle time in (microseconds)",
			nil,
			nil),
		backfillDepthMean: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, schedulerNamespace, "backfill_depth_mean"),
			"Information provided by the Slurm sdiag command, scheduler backfill mean depth",
			nil,
			nil),
		backfillDepthMeanTrySched: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, schedulerNamespace, "backfill_depth_mean_try_sched"),
			"Information provided by the Slurm sdiag command, scheduler backfill mean depth (try sched)",
			nil,
			nil),
		backfillLastDepthCycle: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, schedulerNamespace, "backfill_last_depth_cycle"),
			"Information provided by the Slurm sdiag command, scheduler backfill last depth cycle",
			nil,
			nil),
		backfillLastDepthCycleTrySched: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, schedulerNamespace, "backfill_last_depth_cycle_try_sched"),
			"Information provided by the Slurm sdiag command, scheduler backfill last depth cycle (try sched)",
			nil,
			nil),
		backfillLastQueueLength: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, schedulerNamespace, "backfill_last_queue_length"),
			"Information provided by the Slurm sdiag command, scheduler backfill last queye length",
			nil,
			nil),
		backfillQueueLengthMean: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, schedulerNamespace, "backfill_queue_length_mean"),
			"Information provided by the Slurm sdiag command, scheduler backfill queue length mean",
			nil,
			nil),
		backfillLastTableSize: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, schedulerNamespace, "backfill_last_table_size"),
			"Information provided by the Slurm sdiag command, scheduler backfill last table size",
			nil,
			nil),
		backfillMeanTableSize: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, schedulerNamespace, "backfill_mean_table_size"),
			"Information provided by the Slurm sdiag command, scheduler backfill mean table size",
			nil,
			nil),
		totalBackfilledJobsSinceStart: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, schedulerNamespace, "backfilled_jobs_since_start_total"),
			"Information provided by the Slurm sdiag command, number of jobs started thanks to backfilling since last slurm start",
			nil,
			nil),
		totalBackfilledJobsSinceCycle: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, schedulerNamespace, "backfilled_jobs_since_cycle_total"),
			"Information provided by the Slurm sdiag command, number of jobs started thanks to backfilling since last time stats where reset",
			nil,
			nil),
		totalBackfilledHeterogeneous: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, schedulerNamespace, "backfilled_heterogeneous_total"),
			"Information provided by the Slurm sdiag command, number of heterogeneous job components started thanks to backfilling since last Slurm start",
			nil,
			nil),
		rpcStatsCount: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, rpcNamespace, "stats_count"),
			"Information provided by the Slurm sdiag command, rpc count statistic",
			rpcStatsLabels,
			nil),
		rpcStatsAvgTime: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, rpcNamespace, "stats_time_avg"),
			"Information provided by the Slurm sdiag command, rpc average time statistic in microseconds",
			rpcStatsLabels,
			nil),
		rpcStatsTotalTime: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, rpcNamespace, "stats_time_total"),
			"Information provided by the Slurm sdiag command, rpc total time statistic in microseconds",
			rpcStatsLabels,
			nil),
		userRpcStatsCount: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, userRpcNamespace, "stats_count"),
			"Information provided by the Slurm sdiag command, rpc count statistic per user",
			userRpcStatsLabels,
			nil),
		userRpcStatsAvgTime: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, userRpcNamespace, "stats_time_avg"),
			"Information provided by the Slurm sdiag command, rpc average time statistic per user in microseconds",
			userRpcStatsLabels,
			nil),
		userRpcStatsTotalTime: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, userRpcNamespace, "stats_time_total"),
			"Information provided by the Slurm sdiag command, rpc total time statistic per user in microseconds",
			userRpcStatsLabels,
			nil),
	}
}

func (sc *SchedulerCollector) metrics() (*diagMetrics, error) {
	var dm diagMetrics
	rpcStats := make(map[string]rpcStat)
	userRpcStats := make(map[string]rpcStat)

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
	dm.agentQueueSize = float64(diag.Statistics.GetAgentQueueSize())
	dm.agentCount = float64(diag.Statistics.GetAgentCount())
	dm.agentThreadCount = float64(diag.Statistics.GetAgentThreadCount())
	dm.dbdQueueSize = float64(diag.Statistics.GetDbdAgentQueueSize())
	dm.maxCycle = float64(diag.Statistics.GetScheduleCycleMax())
	dm.lastCycle = float64(diag.Statistics.GetScheduleCycleLast())
	dm.totalCycle = float64(diag.Statistics.GetScheduleCycleTotal())
	dm.meanCycle = float64(diag.Statistics.GetScheduleCycleMean())
	dm.cycleMeanDepth = float64(diag.Statistics.GetScheduleCycleMeanDepth())
	dm.cyclePerMinute = float64(diag.Statistics.GetScheduleCyclePerMinute())
	dm.queueLength = float64(diag.Statistics.GetScheduleQueueLength())
	dm.backfillLastCycle = float64(diag.Statistics.GetBfCycleLast())
	dm.backfillMeanCycle = float64(diag.Statistics.GetBfCycleMean())
	dm.backfillMaxCycle = float64(diag.Statistics.GetBfCycleMax())
	dm.backfillDepthMean = float64(diag.Statistics.GetBfDepthMean())
	dm.backfillDepthMeanTrySched = float64(diag.Statistics.GetBfDepthMeanTry())
	dm.backfillLastDepthCycle = float64(diag.Statistics.GetBfLastDepth())
	dm.backfillLastDepthCycleTrySched = float64(diag.Statistics.GetBfLastDepthTry())
	dm.backfillLastQueueLength = float64(diag.Statistics.GetBfQueueLen())
	dm.backfillQueueLengthMean = float64(diag.Statistics.GetBfQueueLenMean())
	dm.backfillLastTableSize = float64(diag.Statistics.GetBfTableSize())
	dm.backfillMeanTableSize = float64(diag.Statistics.GetBfTableSizeMean())
	dm.totalBackfilledJobsSinceStart = float64(diag.Statistics.GetBfBackfilledJobs())
	dm.totalBackfilledJobsSinceCycle = float64(diag.Statistics.GetBfLastBackfilledJobs())
	dm.totalBackfilledHeterogeneous = float64(diag.Statistics.GetBfBackfilledHetJobs())

	for _, rpc := range diag.Statistics.GetRpcsMessageType() {
		stat := rpcStat{
			count:     float64(rpc.GetCount()),
			aveTime:   float64(rpc.GetAveTime()),
			totalTime: float64(rpc.GetTotalTime()),
		}
		rpcStats[rpc.GetMessageType()] = stat
	}
	for _, userRpc := range diag.Statistics.GetRpcsUser() {
		stat := rpcStat{
			count:     float64(userRpc.GetCount()),
			aveTime:   float64(userRpc.GetAveTime()),
			totalTime: float64(userRpc.GetTotalTime()),
		}
		userRpcStats[userRpc.GetUser()] = stat
	}
	dm.rpcStats = rpcStats
	dm.userRpcStats = userRpcStats

	return &dm, nil
}

func (sc *SchedulerCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- sc.threads
	ch <- sc.agentQueueSize
	ch <- sc.agentThreadCount
	ch <- sc.dbdQueueSize
	ch <- sc.maxCycle
	ch <- sc.lastCycle
	ch <- sc.totalCycle
	ch <- sc.meanCycle
	ch <- sc.cycleMeanDepth
	ch <- sc.cyclePerMinute
	ch <- sc.queueLength
	ch <- sc.backfillLastCycle
	ch <- sc.backfillMeanCycle
	ch <- sc.backfillMaxCycle
	ch <- sc.backfillDepthMean
	ch <- sc.backfillDepthMeanTrySched
	ch <- sc.backfillLastDepthCycle
	ch <- sc.backfillLastDepthCycleTrySched
	ch <- sc.backfillLastQueueLength
	ch <- sc.backfillQueueLengthMean
	ch <- sc.backfillLastTableSize
	ch <- sc.backfillMeanTableSize
	ch <- sc.totalBackfilledJobsSinceStart
	ch <- sc.totalBackfilledJobsSinceCycle
	ch <- sc.totalBackfilledHeterogeneous
	ch <- sc.rpcStatsCount
	ch <- sc.rpcStatsAvgTime
	ch <- sc.rpcStatsTotalTime
	ch <- sc.userRpcStatsCount
	ch <- sc.userRpcStatsAvgTime
	ch <- sc.userRpcStatsTotalTime
}

func (sc *SchedulerCollector) Collect(ch chan<- prometheus.Metric) {
	var errValue float64
	sm, err := sc.metrics()
	if err != nil {
		errValue = 1
	}
	ch <- prometheus.MustNewConstMetric(sc.threads, prometheus.GaugeValue, sm.threads)
	ch <- prometheus.MustNewConstMetric(sc.agentQueueSize, prometheus.GaugeValue, sm.agentQueueSize)
	ch <- prometheus.MustNewConstMetric(sc.agentCount, prometheus.GaugeValue, sm.agentCount)
	ch <- prometheus.MustNewConstMetric(sc.agentThreadCount, prometheus.GaugeValue, sm.agentThreadCount)
	ch <- prometheus.MustNewConstMetric(sc.dbdQueueSize, prometheus.GaugeValue, sm.dbdQueueSize)
	ch <- prometheus.MustNewConstMetric(sc.maxCycle, prometheus.GaugeValue, sm.maxCycle)
	ch <- prometheus.MustNewConstMetric(sc.lastCycle, prometheus.GaugeValue, sm.lastCycle)
	ch <- prometheus.MustNewConstMetric(sc.totalCycle, prometheus.GaugeValue, sm.totalCycle)
	ch <- prometheus.MustNewConstMetric(sc.meanCycle, prometheus.GaugeValue, sm.meanCycle)
	ch <- prometheus.MustNewConstMetric(sc.cycleMeanDepth, prometheus.GaugeValue, sm.cycleMeanDepth)
	ch <- prometheus.MustNewConstMetric(sc.cyclePerMinute, prometheus.GaugeValue, sm.cyclePerMinute)
	ch <- prometheus.MustNewConstMetric(sc.queueLength, prometheus.GaugeValue, sm.queueLength)
	ch <- prometheus.MustNewConstMetric(sc.backfillLastCycle, prometheus.GaugeValue, sm.backfillLastCycle)
	ch <- prometheus.MustNewConstMetric(sc.backfillMeanCycle, prometheus.GaugeValue, sm.backfillMeanCycle)
	ch <- prometheus.MustNewConstMetric(sc.backfillMaxCycle, prometheus.GaugeValue, sm.backfillMaxCycle)
	ch <- prometheus.MustNewConstMetric(sc.backfillDepthMean, prometheus.GaugeValue, sm.backfillDepthMean)
	ch <- prometheus.MustNewConstMetric(sc.backfillDepthMeanTrySched, prometheus.GaugeValue, sm.backfillDepthMeanTrySched)
	ch <- prometheus.MustNewConstMetric(sc.backfillLastDepthCycle, prometheus.GaugeValue, sm.backfillLastDepthCycle)
	ch <- prometheus.MustNewConstMetric(sc.backfillLastDepthCycleTrySched, prometheus.GaugeValue, sm.backfillLastDepthCycleTrySched)
	ch <- prometheus.MustNewConstMetric(sc.backfillLastQueueLength, prometheus.GaugeValue, sm.backfillLastQueueLength)
	ch <- prometheus.MustNewConstMetric(sc.backfillQueueLengthMean, prometheus.GaugeValue, sm.backfillQueueLengthMean)
	ch <- prometheus.MustNewConstMetric(sc.backfillLastTableSize, prometheus.GaugeValue, sm.backfillLastTableSize)
	ch <- prometheus.MustNewConstMetric(sc.backfillMeanTableSize, prometheus.GaugeValue, sm.backfillMeanTableSize)
	ch <- prometheus.MustNewConstMetric(sc.totalBackfilledJobsSinceStart, prometheus.GaugeValue, sm.totalBackfilledJobsSinceStart)
	ch <- prometheus.MustNewConstMetric(sc.totalBackfilledJobsSinceCycle, prometheus.GaugeValue, sm.totalBackfilledJobsSinceCycle)
	ch <- prometheus.MustNewConstMetric(sc.totalBackfilledHeterogeneous, prometheus.GaugeValue, sm.totalBackfilledHeterogeneous)
	for name, stat := range sm.rpcStats {
		ch <- prometheus.MustNewConstMetric(sc.rpcStatsCount, prometheus.CounterValue, stat.count, name)
		ch <- prometheus.MustNewConstMetric(sc.rpcStatsAvgTime, prometheus.GaugeValue, stat.aveTime, name)
		ch <- prometheus.MustNewConstMetric(sc.rpcStatsTotalTime, prometheus.CounterValue, stat.totalTime, name)
	}
	for name, stat := range sm.userRpcStats {
		ch <- prometheus.MustNewConstMetric(sc.userRpcStatsCount, prometheus.CounterValue, stat.count, name)
		ch <- prometheus.MustNewConstMetric(sc.userRpcStatsAvgTime, prometheus.GaugeValue, stat.aveTime, name)
		ch <- prometheus.MustNewConstMetric(sc.userRpcStatsTotalTime, prometheus.CounterValue, stat.totalTime, name)
	}
	ch <- prometheus.MustNewConstMetric(collectError, prometheus.GaugeValue, errValue, "scheduler")
}
