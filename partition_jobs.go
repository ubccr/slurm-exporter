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
	"regexp"
	"strings"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/montanaflynn/stats"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/ubccr/slurmrest"
)

const (
	partJobsNamespace    = "partition_jobs"
	partGPUJobsNamespace = "partition_jobs_gres_gpu"
)

type PartitionJobsCollector struct {
	client             *slurmrest.APIClient
	logger             log.Logger
	pending            *prometheus.Desc
	pendingDep         *prometheus.Desc
	running            *prometheus.Desc
	suspended          *prometheus.Desc
	cancelled          *prometheus.Desc
	completing         *prometheus.Desc
	completed          *prometheus.Desc
	configuring        *prometheus.Desc
	failed             *prometheus.Desc
	timeout            *prometheus.Desc
	preempted          *prometheus.Desc
	nodeFail           *prometheus.Desc
	total              *prometheus.Desc
	medianWaitTime     *prometheus.Desc
	avgWaitTime        *prometheus.Desc
	medianStartTime    *prometheus.Desc
	avgStartTime       *prometheus.Desc
	gpuPending         *prometheus.Desc
	gpuPendingDep      *prometheus.Desc
	gpuRunning         *prometheus.Desc
	gpuSuspended       *prometheus.Desc
	gpuCancelled       *prometheus.Desc
	gpuCompleting      *prometheus.Desc
	gpuCompleted       *prometheus.Desc
	gpuConfiguring     *prometheus.Desc
	gpuFailed          *prometheus.Desc
	gpuTimeout         *prometheus.Desc
	gpuPreempted       *prometheus.Desc
	gpuNodeFail        *prometheus.Desc
	gpuTotal           *prometheus.Desc
	gpuMedianWaitTime  *prometheus.Desc
	gpuAvgWaitTime     *prometheus.Desc
	gpuMedianStartTime *prometheus.Desc
	gpuAvgStartTime    *prometheus.Desc
}

type partitionJobMetrics struct {
	pending            map[string]float64
	pendingDep         map[string]float64
	running            map[string]float64
	suspended          map[string]float64
	cancelled          map[string]float64
	completing         map[string]float64
	completed          map[string]float64
	configuring        map[string]float64
	failed             map[string]float64
	timeout            map[string]float64
	preempted          map[string]float64
	nodeFail           map[string]float64
	total              map[string]float64
	medianWaitTime     map[string]float64
	avgWaitTime        map[string]float64
	medianStartTime    map[string]float64
	avgStartTime       map[string]float64
	gpuPending         map[string]float64
	gpuPendingDep      map[string]float64
	gpuRunning         map[string]float64
	gpuSuspended       map[string]float64
	gpuCancelled       map[string]float64
	gpuCompleting      map[string]float64
	gpuCompleted       map[string]float64
	gpuConfiguring     map[string]float64
	gpuFailed          map[string]float64
	gpuTimeout         map[string]float64
	gpuPreempted       map[string]float64
	gpuNodeFail        map[string]float64
	gpuTotal           map[string]float64
	gpuMedianWaitTime  map[string]float64
	gpuAvgWaitTime     map[string]float64
	gpuMedianStartTime map[string]float64
	gpuAvgStartTime    map[string]float64
}

func NewPartitionJobsCollector(client *slurmrest.APIClient, logger log.Logger) *PartitionJobsCollector {
	labels := []string{"partition"}
	waitLabels := []string{"partition", "reason"}
	return &PartitionJobsCollector{
		client: client,
		logger: log.With(logger, "collector", "partition_jobs"),
		pending: prometheus.NewDesc(prometheus.BuildFQName(namespace, partJobsNamespace, "pending"),
			"Pending jobs in the partition", labels, nil),
		pendingDep: prometheus.NewDesc(prometheus.BuildFQName(namespace, partJobsNamespace, "pending_dependency"),
			"Pending jobs because of dependency in the partition", labels, nil),
		running: prometheus.NewDesc(prometheus.BuildFQName(namespace, partJobsNamespace, "running"),
			"Running jobs in the partition", labels, nil),
		suspended: prometheus.NewDesc(prometheus.BuildFQName(namespace, partJobsNamespace, "suspended"),
			"Suspended jobs in the partition", labels, nil),
		cancelled: prometheus.NewDesc(prometheus.BuildFQName(namespace, partJobsNamespace, "cancelled"),
			"Cancelled jobs in the partition", labels, nil),
		completing: prometheus.NewDesc(prometheus.BuildFQName(namespace, partJobsNamespace, "completing"),
			"Completing jobs in the partition", labels, nil),
		completed: prometheus.NewDesc(prometheus.BuildFQName(namespace, partJobsNamespace, "completed"),
			"Completed jobs in the partition", labels, nil),
		configuring: prometheus.NewDesc(prometheus.BuildFQName(namespace, partJobsNamespace, "configuring"),
			"Configuring jobs in the partition", labels, nil),
		failed: prometheus.NewDesc(prometheus.BuildFQName(namespace, partJobsNamespace, "failed"),
			"Number of failed jobs in the partition", labels, nil),
		timeout: prometheus.NewDesc(prometheus.BuildFQName(namespace, partJobsNamespace, "timeout"),
			"Jobs stopped by timeout in the partition", labels, nil),
		preempted: prometheus.NewDesc(prometheus.BuildFQName(namespace, partJobsNamespace, "preempted"),
			"Number of preempted jobs in the partition", labels, nil),
		nodeFail: prometheus.NewDesc(prometheus.BuildFQName(namespace, partJobsNamespace, "node_fail"),
			"Number of jobs stopped due to node fail in the partition", labels, nil),
		total: prometheus.NewDesc(prometheus.BuildFQName(namespace, partJobsNamespace, "total"),
			"Total jobs in the partition", labels, nil),
		medianWaitTime: prometheus.NewDesc(prometheus.BuildFQName(namespace, partJobsNamespace, "median_wait_time_seconds"),
			"Median wait time of pending jobs in the partition", waitLabels, nil),
		avgWaitTime: prometheus.NewDesc(prometheus.BuildFQName(namespace, partJobsNamespace, "average_wait_time_seconds"),
			"Average wait time of pending jobs in the partition", waitLabels, nil),
		medianStartTime: prometheus.NewDesc(prometheus.BuildFQName(namespace, partJobsNamespace, "median_start_time_seconds"),
			"Median estimated start time of pending jobs in the partition", waitLabels, nil),
		avgStartTime: prometheus.NewDesc(prometheus.BuildFQName(namespace, partJobsNamespace, "average_start_time_seconds"),
			"Average estimated start time of pending jobs in the partition", waitLabels, nil),
		gpuPending: prometheus.NewDesc(prometheus.BuildFQName(namespace, partGPUJobsNamespace, "pending"),
			"Pending gres/gpu jobs in the partition", labels, nil),
		gpuPendingDep: prometheus.NewDesc(prometheus.BuildFQName(namespace, partGPUJobsNamespace, "pending_dependency"),
			"Pending gres/gpu jobs because of dependency in the partition", labels, nil),
		gpuRunning: prometheus.NewDesc(prometheus.BuildFQName(namespace, partGPUJobsNamespace, "running"),
			"Running gres/gpu jobs in the partition", labels, nil),
		gpuSuspended: prometheus.NewDesc(prometheus.BuildFQName(namespace, partGPUJobsNamespace, "suspended"),
			"Suspended gres/gpu jobs in the partition", labels, nil),
		gpuCancelled: prometheus.NewDesc(prometheus.BuildFQName(namespace, partGPUJobsNamespace, "cancelled"),
			"Cancelled gres/gpu jobs in the partition", labels, nil),
		gpuCompleting: prometheus.NewDesc(prometheus.BuildFQName(namespace, partGPUJobsNamespace, "completing"),
			"Completing gres/gpu jobs in the partition", labels, nil),
		gpuCompleted: prometheus.NewDesc(prometheus.BuildFQName(namespace, partGPUJobsNamespace, "completed"),
			"Completed gres/gpu jobs in the partition", labels, nil),
		gpuConfiguring: prometheus.NewDesc(prometheus.BuildFQName(namespace, partGPUJobsNamespace, "configuring"),
			"Configuring gres/gpu jobs in the partition", labels, nil),
		gpuFailed: prometheus.NewDesc(prometheus.BuildFQName(namespace, partGPUJobsNamespace, "failed"),
			"Number of failed gres/gpu jobs in the partition", labels, nil),
		gpuTimeout: prometheus.NewDesc(prometheus.BuildFQName(namespace, partGPUJobsNamespace, "timeout"),
			"gres/gpu Jobs stopped by timeout in the partition", labels, nil),
		gpuPreempted: prometheus.NewDesc(prometheus.BuildFQName(namespace, partGPUJobsNamespace, "preempted"),
			"Number of preempted gres/gpu jobs in the partition", labels, nil),
		gpuNodeFail: prometheus.NewDesc(prometheus.BuildFQName(namespace, partGPUJobsNamespace, "node_fail"),
			"Number of gres/gpu jobs stopped due to node fail in the partition", labels, nil),
		gpuTotal: prometheus.NewDesc(prometheus.BuildFQName(namespace, partGPUJobsNamespace, "total"),
			"Total gres/gpu jobs in the partition", labels, nil),
		gpuMedianWaitTime: prometheus.NewDesc(prometheus.BuildFQName(namespace, partGPUJobsNamespace, "median_wait_time_seconds"),
			"Median wait time of pending jobs that requested GPU in the partition", waitLabels, nil),
		gpuAvgWaitTime: prometheus.NewDesc(prometheus.BuildFQName(namespace, partGPUJobsNamespace, "average_wait_time_seconds"),
			"Average wait time of pending jobs that requested GPU in the partition", waitLabels, nil),
		gpuMedianStartTime: prometheus.NewDesc(prometheus.BuildFQName(namespace, partGPUJobsNamespace, "median_start_time_seconds"),
			"Median estimated start time of pending jobs that requested GPU in the partition", waitLabels, nil),
		gpuAvgStartTime: prometheus.NewDesc(prometheus.BuildFQName(namespace, partGPUJobsNamespace, "average_start_time_seconds"),
			"Average estimated start time of pending jobs that requested GPU in the partition", waitLabels, nil),
	}
}

func (pjc *PartitionJobsCollector) metrics() (*partitionJobMetrics, error) {
	var pjm partitionJobMetrics
	ignoredPattern := regexp.MustCompile(*ignorePartitions)

	partitions, resp, err := pjc.client.SlurmApi.SlurmctldGetPartitions(context.Background()).Execute()
	if err != nil {
		level.Error(pjc.logger).Log("msg", "Failed to fetch partitions from slurm rest api", "err", err)
		return &pjm, err
	} else if resp.StatusCode != 200 {
		err = fmt.Errorf("HTTP response not OK while fetching partitions from slurm rest api")
		level.Error(pjc.logger).Log("err", err, "status_code", resp.StatusCode)
		return &pjm, err
	} else if len(partitions.GetErrors()) > 0 {
		for _, err := range partitions.GetErrors() {
			level.Error(pjc.logger).Log("err", err.GetError())
		}
		return &pjm, fmt.Errorf("HTTP response contained %d errors", len(partitions.GetErrors()))
	}
	jobs, resp, err := pjc.client.SlurmApi.SlurmctldGetJobs(context.Background()).Execute()
	if err != nil {
		level.Error(pjc.logger).Log("msg", "Failed to fetch jobs from slurm rest api", "err", err)
		return &pjm, err
	} else if resp.StatusCode != 200 {
		err = fmt.Errorf("HTTP response not OK while fetching jobs from slurm rest api")
		level.Error(pjc.logger).Log("err", err, "status_code", resp.StatusCode)
		return &pjm, err
	} else if len(jobs.GetErrors()) > 0 {
		for _, err := range jobs.GetErrors() {
			level.Error(pjc.logger).Log("err", err.GetError())
		}
		return &pjm, fmt.Errorf("HTTP response contained %d errors", len(jobs.GetErrors()))
	}

	pending := make(map[string]float64)
	pendingDep := make(map[string]float64)
	running := make(map[string]float64)
	suspended := make(map[string]float64)
	cancelled := make(map[string]float64)
	completing := make(map[string]float64)
	completed := make(map[string]float64)
	configuring := make(map[string]float64)
	failed := make(map[string]float64)
	timeout := make(map[string]float64)
	preempted := make(map[string]float64)
	nodeFail := make(map[string]float64)
	total := make(map[string]float64)
	medianWaitTime := make(map[string]float64)
	avgWaitTime := make(map[string]float64)
	medianStartTime := make(map[string]float64)
	avgStartTime := make(map[string]float64)
	gpuPending := make(map[string]float64)
	gpuPendingDep := make(map[string]float64)
	gpuRunning := make(map[string]float64)
	gpuSuspended := make(map[string]float64)
	gpuCancelled := make(map[string]float64)
	gpuCompleting := make(map[string]float64)
	gpuCompleted := make(map[string]float64)
	gpuConfiguring := make(map[string]float64)
	gpuFailed := make(map[string]float64)
	gpuTimeout := make(map[string]float64)
	gpuPreempted := make(map[string]float64)
	gpuNodeFail := make(map[string]float64)
	gpuTotal := make(map[string]float64)
	gpuMedianWaitTime := make(map[string]float64)
	gpuAvgWaitTime := make(map[string]float64)
	gpuMedianStartTime := make(map[string]float64)
	gpuAvgStartTime := make(map[string]float64)
	waitTimes := make(map[string][]float64)
	startTimes := make(map[string][]float64)
	gpuWaitTimes := make(map[string][]float64)
	gpuStartTimes := make(map[string][]float64)

	for _, p := range partitions.GetPartitions() {
		if ignoredPattern.MatchString(p.GetName()) {
			continue
		}
		waitKey := fmt.Sprintf("%s|", p.GetName())
		pending[p.GetName()] = 0
		pendingDep[p.GetName()] = 0
		running[p.GetName()] = 0
		suspended[p.GetName()] = 0
		cancelled[p.GetName()] = 0
		completing[p.GetName()] = 0
		completed[p.GetName()] = 0
		configuring[p.GetName()] = 0
		failed[p.GetName()] = 0
		timeout[p.GetName()] = 0
		preempted[p.GetName()] = 0
		nodeFail[p.GetName()] = 0
		total[p.GetName()] = 0
		medianWaitTime[waitKey] = 0
		avgWaitTime[waitKey] = 0
		medianStartTime[waitKey] = 0
		avgStartTime[waitKey] = 0
		gpuPending[p.GetName()] = 0
		gpuPendingDep[p.GetName()] = 0
		gpuRunning[p.GetName()] = 0
		gpuSuspended[p.GetName()] = 0
		gpuCancelled[p.GetName()] = 0
		gpuCompleting[p.GetName()] = 0
		gpuCompleted[p.GetName()] = 0
		gpuConfiguring[p.GetName()] = 0
		gpuFailed[p.GetName()] = 0
		gpuTimeout[p.GetName()] = 0
		gpuPreempted[p.GetName()] = 0
		gpuNodeFail[p.GetName()] = 0
		gpuTotal[p.GetName()] = 0
		gpuMedianWaitTime[waitKey] = 0
		gpuAvgWaitTime[waitKey] = 0
		gpuMedianStartTime[waitKey] = 0
		gpuAvgStartTime[waitKey] = 0
	}

	now := time.Now().Unix()

	for _, j := range jobs.GetJobs() {
		tres := parseTres(j.GetTresReqStr())
		tresAlloc := parseTres(j.GetTresAllocStr())
		partitions := strings.Split(j.GetPartition(), ",")

		for _, p := range partitions {
			if ignoredPattern.MatchString(p) {
				continue
			}
			total[p]++
			if tres.GresGpu > 0 {
				gpuTotal[p]++
			}
			switch j.GetJobState() {
			case "PENDING":
				pending[p]++
				if j.GetStateReason() == "Dependency" {
					pendingDep[p]++
				}
				if tres.GresGpu > 0 {
					gpuPending[p]++
					if j.GetStateReason() == "Dependency" {
						gpuPendingDep[p]++
					}
				}
				waitKey := fmt.Sprintf("%s|%s", p, j.GetStateReason())
				startTime := j.GetStartTime() - now
				if startTime >= 0 {
					startTimes[waitKey] = append(startTimes[waitKey], float64(startTime))
					if tres.GresGpu > 0 {
						gpuStartTimes[waitKey] = append(gpuStartTimes[waitKey], float64(startTime))
					}
				}
				waitTime := now - j.GetSubmitTime()
				waitTimes[waitKey] = append(waitTimes[waitKey], float64(waitTime))
				if tres.GresGpu > 0 {
					gpuWaitTimes[waitKey] = append(gpuWaitTimes[waitKey], float64(waitTime))
				}
			case "RUNNING":
				running[p]++
				if tresAlloc.GresGpu > 0 {
					gpuRunning[p]++
				}
			case "SUSPENDED":
				suspended[p]++
				if tres.GresGpu > 0 {
					gpuSuspended[p]++
				}
			case "CANCELLED":
				cancelled[p]++
				if tres.GresGpu > 0 {
					gpuCancelled[p]++
				}
			case "COMPLETING":
				completing[p]++
				if tres.GresGpu > 0 {
					gpuCompleting[p]++
				}
			case "COMPLETED":
				completed[p]++
				if tres.GresGpu > 0 {
					gpuCompleted[p]++
				}
			case "CONFIGURING":
				configuring[p]++
				if tres.GresGpu > 0 {
					gpuConfiguring[p]++
				}
			case "FAILED":
				failed[p]++
				if tres.GresGpu > 0 {
					gpuFailed[p]++
				}
			case "TIMEOUT":
				timeout[p]++
				if tres.GresGpu > 0 {
					gpuTimeout[p]++
				}
			case "PREEMPTED":
				preempted[p]++
				if tres.GresGpu > 0 {
					gpuPreempted[p]++
				}
			case "NODE_FAIL":
				nodeFail[p]++
				if tres.GresGpu > 0 {
					gpuFailed[p]++
				}
			}
		}
	}

	for p, times := range waitTimes {
		if len(times) == 0 {
			continue
		}
		if time, err := stats.Median(times); err != nil {
			level.Error(pjc.logger).Log("msg", "Unable to calculate median wait time", "key", p, "err", err)
		} else {
			medianWaitTime[p] = time
		}
		if time, err := stats.Mean(times); err != nil {
			level.Error(pjc.logger).Log("msg", "Unable to calculate average wait time", "key", p, "err", err)
		} else {
			avgWaitTime[p] = time
		}
	}
	for p, times := range startTimes {
		if len(times) == 0 {
			continue
		}
		if time, err := stats.Median(times); err != nil {
			level.Error(pjc.logger).Log("msg", "Unable to calculate median start time", "key", p, "err", err)
		} else {
			medianStartTime[p] = time
		}
		if time, err := stats.Mean(times); err != nil {
			level.Error(pjc.logger).Log("msg", "Unable to calculate average start time", "key", p, "err", err)
		} else {
			avgStartTime[p] = time
		}
	}
	for p, times := range gpuWaitTimes {
		if len(times) == 0 {
			continue
		}
		if time, err := stats.Median(times); err != nil {
			level.Error(pjc.logger).Log("msg", "Unable to calculate median GPU wait time", "key", p, "err", err)
		} else {
			gpuMedianWaitTime[p] = time
		}
		if time, err := stats.Mean(times); err != nil {
			level.Error(pjc.logger).Log("msg", "Unable to calculate average GPU wait time", "key", p, "err", err)
		} else {
			gpuAvgWaitTime[p] = time
		}
	}
	for p, times := range gpuStartTimes {
		if len(times) == 0 {
			continue
		}
		if time, err := stats.Median(times); err != nil {
			level.Error(pjc.logger).Log("msg", "Unable to calculate median GPU start time", "key", p, "err", err)
		} else {
			gpuMedianStartTime[p] = time
		}
		if time, err := stats.Mean(times); err != nil {
			level.Error(pjc.logger).Log("msg", "Unable to calculate average GPU start time", "key", p, "err", err)
		} else {
			gpuAvgStartTime[p] = time
		}
	}

	pjm.pending = pending
	pjm.pendingDep = pendingDep
	pjm.running = running
	pjm.suspended = suspended
	pjm.cancelled = suspended
	pjm.completing = completing
	pjm.completed = completed
	pjm.configuring = configuring
	pjm.failed = failed
	pjm.timeout = timeout
	pjm.preempted = preempted
	pjm.nodeFail = nodeFail
	pjm.total = total
	pjm.medianWaitTime = medianWaitTime
	pjm.avgWaitTime = avgWaitTime
	pjm.medianStartTime = medianStartTime
	pjm.avgStartTime = avgStartTime
	pjm.gpuPending = gpuPending
	pjm.gpuPendingDep = gpuPendingDep
	pjm.gpuRunning = gpuRunning
	pjm.gpuSuspended = gpuSuspended
	pjm.gpuCancelled = gpuCancelled
	pjm.gpuCompleting = gpuCompleting
	pjm.gpuCompleted = gpuCompleted
	pjm.gpuConfiguring = gpuConfiguring
	pjm.gpuFailed = gpuFailed
	pjm.gpuTimeout = gpuTimeout
	pjm.gpuPreempted = gpuPreempted
	pjm.gpuNodeFail = gpuNodeFail
	pjm.gpuTotal = gpuTotal
	pjm.gpuMedianWaitTime = gpuMedianWaitTime
	pjm.gpuAvgWaitTime = gpuAvgWaitTime
	pjm.gpuMedianStartTime = gpuMedianStartTime
	pjm.gpuAvgStartTime = gpuAvgStartTime

	return &pjm, err
}

func (pjc *PartitionJobsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- pjc.pending
	ch <- pjc.pendingDep
	ch <- pjc.running
	ch <- pjc.suspended
	ch <- pjc.cancelled
	ch <- pjc.completing
	ch <- pjc.completed
	ch <- pjc.configuring
	ch <- pjc.failed
	ch <- pjc.timeout
	ch <- pjc.preempted
	ch <- pjc.nodeFail
	ch <- pjc.total
	ch <- pjc.medianWaitTime
	ch <- pjc.avgWaitTime
	ch <- pjc.medianStartTime
	ch <- pjc.avgStartTime
	ch <- pjc.gpuPending
	ch <- pjc.gpuPendingDep
	ch <- pjc.gpuRunning
	ch <- pjc.gpuSuspended
	ch <- pjc.gpuCancelled
	ch <- pjc.gpuCompleting
	ch <- pjc.gpuCompleted
	ch <- pjc.gpuConfiguring
	ch <- pjc.gpuFailed
	ch <- pjc.gpuTimeout
	ch <- pjc.gpuPreempted
	ch <- pjc.gpuNodeFail
	ch <- pjc.gpuTotal
	ch <- pjc.gpuMedianWaitTime
	ch <- pjc.gpuAvgWaitTime
	ch <- pjc.gpuMedianStartTime
	ch <- pjc.gpuAvgStartTime
}

func (pjc *PartitionJobsCollector) Collect(ch chan<- prometheus.Metric) {
	var errValue float64
	pjm, err := pjc.metrics()
	if err != nil {
		errValue = 1
	}
	for p, v := range pjm.pending {
		ch <- prometheus.MustNewConstMetric(pjc.pending, prometheus.GaugeValue, v, p)
	}
	for p, v := range pjm.pendingDep {
		ch <- prometheus.MustNewConstMetric(pjc.pendingDep, prometheus.GaugeValue, v, p)
	}
	for p, v := range pjm.pending {
		ch <- prometheus.MustNewConstMetric(pjc.running, prometheus.GaugeValue, v, p)
	}
	for p, v := range pjm.suspended {
		ch <- prometheus.MustNewConstMetric(pjc.suspended, prometheus.GaugeValue, v, p)
	}
	for p, v := range pjm.cancelled {
		ch <- prometheus.MustNewConstMetric(pjc.cancelled, prometheus.GaugeValue, v, p)
	}
	for p, v := range pjm.completing {
		ch <- prometheus.MustNewConstMetric(pjc.completing, prometheus.GaugeValue, v, p)
	}
	for p, v := range pjm.completed {
		ch <- prometheus.MustNewConstMetric(pjc.completed, prometheus.GaugeValue, v, p)
	}
	for p, v := range pjm.configuring {
		ch <- prometheus.MustNewConstMetric(pjc.configuring, prometheus.GaugeValue, v, p)
	}
	for p, v := range pjm.failed {
		ch <- prometheus.MustNewConstMetric(pjc.failed, prometheus.GaugeValue, v, p)
	}
	for p, v := range pjm.timeout {
		ch <- prometheus.MustNewConstMetric(pjc.timeout, prometheus.GaugeValue, v, p)
	}
	for p, v := range pjm.preempted {
		ch <- prometheus.MustNewConstMetric(pjc.preempted, prometheus.GaugeValue, v, p)
	}
	for p, v := range pjm.nodeFail {
		ch <- prometheus.MustNewConstMetric(pjc.nodeFail, prometheus.GaugeValue, v, p)
	}
	for p, v := range pjm.total {
		ch <- prometheus.MustNewConstMetric(pjc.total, prometheus.GaugeValue, v, p)
	}
	for p, v := range pjm.medianWaitTime {
		keys := strings.Split(p, "|")
		ch <- prometheus.MustNewConstMetric(pjc.medianWaitTime, prometheus.GaugeValue, v, keys[0], keys[1])
	}
	for p, v := range pjm.avgWaitTime {
		keys := strings.Split(p, "|")
		ch <- prometheus.MustNewConstMetric(pjc.avgWaitTime, prometheus.GaugeValue, v, keys[0], keys[1])
	}
	for p, v := range pjm.medianStartTime {
		keys := strings.Split(p, "|")
		ch <- prometheus.MustNewConstMetric(pjc.medianStartTime, prometheus.GaugeValue, v, keys[0], keys[1])
	}
	for p, v := range pjm.avgStartTime {
		keys := strings.Split(p, "|")
		ch <- prometheus.MustNewConstMetric(pjc.avgStartTime, prometheus.GaugeValue, v, keys[0], keys[1])
	}
	for p, v := range pjm.gpuPending {
		ch <- prometheus.MustNewConstMetric(pjc.gpuPending, prometheus.GaugeValue, v, p)
	}
	for p, v := range pjm.gpuPendingDep {
		ch <- prometheus.MustNewConstMetric(pjc.gpuPendingDep, prometheus.GaugeValue, v, p)
	}
	for p, v := range pjm.gpuRunning {
		ch <- prometheus.MustNewConstMetric(pjc.gpuRunning, prometheus.GaugeValue, v, p)
	}
	for p, v := range pjm.gpuSuspended {
		ch <- prometheus.MustNewConstMetric(pjc.gpuSuspended, prometheus.GaugeValue, v, p)
	}
	for p, v := range pjm.gpuCancelled {
		ch <- prometheus.MustNewConstMetric(pjc.gpuCancelled, prometheus.GaugeValue, v, p)
	}
	for p, v := range pjm.gpuCompleting {
		ch <- prometheus.MustNewConstMetric(pjc.gpuCompleting, prometheus.GaugeValue, v, p)
	}
	for p, v := range pjm.gpuCompleted {
		ch <- prometheus.MustNewConstMetric(pjc.gpuCompleted, prometheus.GaugeValue, v, p)
	}
	for p, v := range pjm.gpuConfiguring {
		ch <- prometheus.MustNewConstMetric(pjc.gpuConfiguring, prometheus.GaugeValue, v, p)
	}
	for p, v := range pjm.gpuFailed {
		ch <- prometheus.MustNewConstMetric(pjc.gpuFailed, prometheus.GaugeValue, v, p)
	}
	for p, v := range pjm.gpuTimeout {
		ch <- prometheus.MustNewConstMetric(pjc.gpuTimeout, prometheus.GaugeValue, v, p)
	}
	for p, v := range pjm.gpuPreempted {
		ch <- prometheus.MustNewConstMetric(pjc.gpuPreempted, prometheus.GaugeValue, v, p)
	}
	for p, v := range pjm.gpuNodeFail {
		ch <- prometheus.MustNewConstMetric(pjc.gpuNodeFail, prometheus.GaugeValue, v, p)
	}
	for p, v := range pjm.gpuTotal {
		ch <- prometheus.MustNewConstMetric(pjc.gpuTotal, prometheus.GaugeValue, v, p)
	}
	for p, v := range pjm.gpuMedianWaitTime {
		keys := strings.Split(p, "|")
		ch <- prometheus.MustNewConstMetric(pjc.gpuMedianWaitTime, prometheus.GaugeValue, v, keys[0], keys[1])
	}
	for p, v := range pjm.gpuAvgWaitTime {
		keys := strings.Split(p, "|")
		ch <- prometheus.MustNewConstMetric(pjc.gpuAvgWaitTime, prometheus.GaugeValue, v, keys[0], keys[1])
	}
	for p, v := range pjm.gpuMedianStartTime {
		keys := strings.Split(p, "|")
		ch <- prometheus.MustNewConstMetric(pjc.gpuMedianStartTime, prometheus.GaugeValue, v, keys[0], keys[1])
	}
	for p, v := range pjm.gpuAvgStartTime {
		keys := strings.Split(p, "|")
		ch <- prometheus.MustNewConstMetric(pjc.gpuAvgStartTime, prometheus.GaugeValue, v, keys[0], keys[1])
	}
	ch <- prometheus.MustNewConstMetric(collectError, prometheus.GaugeValue, errValue, "partition_jobs")
}
