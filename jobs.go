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
	"strings"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/montanaflynn/stats"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/ubccr/slurmrest"
)

const (
	queueNamespace   = "queue"
	gresGPUNamespace = "gres_gpu"
)

type JobsCollector struct {
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

type jobMetrics struct {
	pending            float64
	pendingDep         float64
	running            float64
	suspended          float64
	cancelled          float64
	completing         float64
	completed          float64
	configuring        float64
	failed             float64
	timeout            float64
	preempted          float64
	nodeFail           float64
	total              float64
	medianWaitTime     float64
	avgWaitTime        float64
	medianStartTime    float64
	avgStartTime       float64
	gpuPending         float64
	gpuPendingDep      float64
	gpuRunning         float64
	gpuSuspended       float64
	gpuCancelled       float64
	gpuCompleting      float64
	gpuCompleted       float64
	gpuConfiguring     float64
	gpuFailed          float64
	gpuTimeout         float64
	gpuPreempted       float64
	gpuNodeFail        float64
	gpuTotal           float64
	gpuMedianWaitTime  float64
	gpuAvgWaitTime     float64
	gpuMedianStartTime float64
	gpuAvgStartTime    float64
}

func NewJobsCollector(client *slurmrest.APIClient, logger log.Logger) *JobsCollector {
	return &JobsCollector{
		client: client,
		logger: log.With(logger, "collector", "jobs"),
		pending: prometheus.NewDesc(prometheus.BuildFQName(namespace, queueNamespace, "pending"),
			"Pending jobs in queue", nil, nil),
		pendingDep: prometheus.NewDesc(prometheus.BuildFQName(namespace, queueNamespace, "pending_dependency"),
			"Pending jobs because of dependency in queue", nil, nil),
		running: prometheus.NewDesc(prometheus.BuildFQName(namespace, queueNamespace, "running"),
			"Running jobs in the cluster", nil, nil),
		suspended: prometheus.NewDesc(prometheus.BuildFQName(namespace, queueNamespace, "suspended"),
			"Suspended jobs in the cluster", nil, nil),
		cancelled: prometheus.NewDesc(prometheus.BuildFQName(namespace, queueNamespace, "cancelled"),
			"Cancelled jobs in the cluster", nil, nil),
		completing: prometheus.NewDesc(prometheus.BuildFQName(namespace, queueNamespace, "completing"),
			"Completing jobs in the cluster", nil, nil),
		completed: prometheus.NewDesc(prometheus.BuildFQName(namespace, queueNamespace, "completed"),
			"Completed jobs in the cluster", nil, nil),
		configuring: prometheus.NewDesc(prometheus.BuildFQName(namespace, queueNamespace, "configuring"),
			"Configuring jobs in the cluster", nil, nil),
		failed: prometheus.NewDesc(prometheus.BuildFQName(namespace, queueNamespace, "failed"),
			"Number of failed jobs", nil, nil),
		timeout: prometheus.NewDesc(prometheus.BuildFQName(namespace, queueNamespace, "timeout"),
			"Jobs stopped by timeout", nil, nil),
		preempted: prometheus.NewDesc(prometheus.BuildFQName(namespace, queueNamespace, "preempted"),
			"Number of preempted jobs", nil, nil),
		nodeFail: prometheus.NewDesc(prometheus.BuildFQName(namespace, queueNamespace, "node_fail"),
			"Number of jobs stopped due to node fail", nil, nil),
		total: prometheus.NewDesc(prometheus.BuildFQName(namespace, queueNamespace, "total"),
			"Total jobs in the cluster", nil, nil),
		medianWaitTime: prometheus.NewDesc(prometheus.BuildFQName(namespace, queueNamespace, "median_wait_time_seconds"),
			"Median wait time of pending jobs", nil, nil),
		avgWaitTime: prometheus.NewDesc(prometheus.BuildFQName(namespace, queueNamespace, "average_wait_time_seconds"),
			"Average wait time of pending jobs", nil, nil),
		medianStartTime: prometheus.NewDesc(prometheus.BuildFQName(namespace, queueNamespace, "median_start_time_seconds"),
			"Mean estimated start time of pending jobs", nil, nil),
		avgStartTime: prometheus.NewDesc(prometheus.BuildFQName(namespace, queueNamespace, "average_start_time_seconds"),
			"Average estimated start time of pending jobs", nil, nil),
		gpuPending: prometheus.NewDesc(prometheus.BuildFQName(namespace, gresGPUNamespace, "pending"),
			"Pending gres/gpu jobs in queue", nil, nil),
		gpuPendingDep: prometheus.NewDesc(prometheus.BuildFQName(namespace, gresGPUNamespace, "pending_dependency"),
			"Pending gres/gpu jobs because of dependency in queue", nil, nil),
		gpuRunning: prometheus.NewDesc(prometheus.BuildFQName(namespace, gresGPUNamespace, "running"),
			"Running gres/gpu jobs in the cluster", nil, nil),
		gpuSuspended: prometheus.NewDesc(prometheus.BuildFQName(namespace, gresGPUNamespace, "suspended"),
			"Suspended gres/gpu jobs in the cluster", nil, nil),
		gpuCancelled: prometheus.NewDesc(prometheus.BuildFQName(namespace, gresGPUNamespace, "cancelled"),
			"Cancelled gres/gpu jobs in the cluster", nil, nil),
		gpuCompleting: prometheus.NewDesc(prometheus.BuildFQName(namespace, gresGPUNamespace, "completing"),
			"Completing gres/gpu jobs in the cluster", nil, nil),
		gpuCompleted: prometheus.NewDesc(prometheus.BuildFQName(namespace, gresGPUNamespace, "completed"),
			"Completed gres/gpu jobs in the cluster", nil, nil),
		gpuConfiguring: prometheus.NewDesc(prometheus.BuildFQName(namespace, gresGPUNamespace, "configuring"),
			"Configuring gres/gpu jobs in the cluster", nil, nil),
		gpuFailed: prometheus.NewDesc(prometheus.BuildFQName(namespace, gresGPUNamespace, "failed"),
			"Number of failed gres/gpu jobs", nil, nil),
		gpuTimeout: prometheus.NewDesc(prometheus.BuildFQName(namespace, gresGPUNamespace, "timeout"),
			"gres/gpu Jobs stopped by timeout", nil, nil),
		gpuPreempted: prometheus.NewDesc(prometheus.BuildFQName(namespace, gresGPUNamespace, "preempted"),
			"Number of preempted gres/gpu jobs", nil, nil),
		gpuNodeFail: prometheus.NewDesc(prometheus.BuildFQName(namespace, gresGPUNamespace, "node_fail"),
			"Number of gres/gpu jobs stopped due to node fail", nil, nil),
		gpuTotal: prometheus.NewDesc(prometheus.BuildFQName(namespace, gresGPUNamespace, "total"),
			"Total gres/gpu jobs in the cluster", nil, nil),
		gpuMedianWaitTime: prometheus.NewDesc(prometheus.BuildFQName(namespace, gresGPUNamespace, "median_wait_time_seconds"),
			"Median wait time of pending jobs that requested GPU", nil, nil),
		gpuAvgWaitTime: prometheus.NewDesc(prometheus.BuildFQName(namespace, gresGPUNamespace, "average_wait_time_seconds"),
			"Average wait time of pending jobs that requested GPU", nil, nil),
		gpuMedianStartTime: prometheus.NewDesc(prometheus.BuildFQName(namespace, gresGPUNamespace, "median_start_time_seconds"),
			"Median estimated start time of pending jobs that requested GPU", nil, nil),
		gpuAvgStartTime: prometheus.NewDesc(prometheus.BuildFQName(namespace, gresGPUNamespace, "average_start_time_seconds"),
			"Average estimated start time of pending jobs that requested GPU", nil, nil),
	}
}

func (jc *JobsCollector) metrics() (*jobMetrics, error) {
	var jm jobMetrics

	req := jc.client.SlurmAPI.SlurmV0040GetJobs(context.Background())
	jobs, resp, err := jc.client.SlurmAPI.SlurmV0040GetJobsExecute(req)
	if err != nil {
		level.Error(jc.logger).Log("msg", "Failed to fetch jobs from slurm rest api", "err", err)
		return &jm, err
	} else if resp.StatusCode != 200 {
		err = fmt.Errorf("HTTP response not OK while fetching jobs from slurm rest api")
		level.Error(jc.logger).Log("err", err, "status_code", resp.StatusCode)
		return &jm, err
	} else if len(jobs.GetErrors()) > 0 {
		for _, err := range jobs.GetErrors() {
			level.Error(jc.logger).Log("err", err.GetError())
		}
		return &jm, fmt.Errorf("HTTP response contained %d errors", len(jobs.GetErrors()))
	}

	var waitTimes, gpuWaitTimes, startTimes, gpuStartTimes []float64
	now := time.Now().Unix()

	for _, j := range jobs.GetJobs() {
		tres := parseTres(j.GetTresReqStr())
		tresAlloc := parseTres(j.GetTresAllocStr())

		jm.total++
		if tres.GresGpu > 0 {
			jm.gpuTotal++
		}

		jobstate := strings.Join(j.GetJobState(), ",")
		switch {
		case strings.Contains(jobstate, "PENDING"):
			jm.pending++
			if j.GetStateReason() == "Dependency" {
				jm.pendingDep++
			}
			if tres.GresGpu > 0 {
				jm.gpuPending++
				if j.GetStateReason() == "Dependency" {
					jm.gpuPendingDep++
				}
			}
			startTime := int64(0)
			if jst, ok := j.GetStartTimeOk(); ok {
				startTime = jst.GetNumber() - now
			}
			if startTime >= 0 {
				startTimes = append(startTimes, float64(startTime))
				if tres.GresGpu > 0 {
					gpuStartTimes = append(gpuStartTimes, float64(startTime))
				}
			}
			waitTime := int64(0)
			if jst, ok := j.GetSubmitTimeOk(); ok {
				waitTime = now - jst.GetNumber()
			}
			waitTimes = append(waitTimes, float64(waitTime))
			if tres.GresGpu > 0 {
				gpuWaitTimes = append(gpuWaitTimes, float64(waitTime))
			}
		case strings.Contains(jobstate, "RUNNING"):
			jm.running++
			if tresAlloc.GresGpu > 0 {
				jm.gpuRunning++
			}
		case strings.Contains(jobstate, "SUSPENDED"):
			jm.suspended++
			if tres.GresGpu > 0 {
				jm.gpuSuspended++
			}
		case strings.Contains(jobstate, "CANCELLED"):
			jm.cancelled++
			if tres.GresGpu > 0 {
				jm.gpuCancelled++
			}
		case strings.Contains(jobstate, "COMPLETING"):
			jm.completing++
			if tres.GresGpu > 0 {
				jm.gpuCompleting++
			}
		case strings.Contains(jobstate, "COMPLETED"):
			jm.completed++
			if tres.GresGpu > 0 {
				jm.gpuCompleted++
			}
		case strings.Contains(jobstate, "CONFIGURING"):
			jm.configuring++
			if tres.GresGpu > 0 {
				jm.gpuConfiguring++
			}
		case strings.Contains(jobstate, "FAILED"):
			jm.failed++
			if tres.GresGpu > 0 {
				jm.gpuFailed++
			}
		case strings.Contains(jobstate, "TIMEOUT"):
			jm.timeout++
			if tres.GresGpu > 0 {
				jm.gpuTimeout++
			}
		case strings.Contains(jobstate, "PREEMPTED"):
			jm.preempted++
			if tres.GresGpu > 0 {
				jm.gpuPreempted++
			}
		case strings.Contains(jobstate, "NODE_FAIL"):
			jm.nodeFail++
			if tres.GresGpu > 0 {
				jm.gpuFailed++
			}
		}
	}

	if len(waitTimes) > 0 {
		if waitTime, err := stats.Median(waitTimes); err != nil {
			level.Error(jc.logger).Log("msg", "Unable to calculate median wait time", "err", err)
		} else {
			jm.medianWaitTime = waitTime
		}
		if waitTime, err := stats.Mean(waitTimes); err != nil {
			level.Error(jc.logger).Log("msg", "Unable to calculate average wait time", "err", err)
		} else {
			jm.avgWaitTime = waitTime
		}
	}
	if len(gpuWaitTimes) > 0 {
		if gpuWaitTime, err := stats.Median(gpuWaitTimes); err != nil {
			level.Error(jc.logger).Log("msg", "Unable to calculate median GPU wait time", "err", err)
		} else {
			jm.gpuMedianWaitTime = gpuWaitTime
		}
		if gpuWaitTime, err := stats.Mean(gpuWaitTimes); err != nil {
			level.Error(jc.logger).Log("msg", "Unable to calculate average GPU wait time", "err", err)
		} else {
			jm.gpuAvgWaitTime = gpuWaitTime
		}
	}
	if len(startTimes) > 0 {
		if startTime, err := stats.Median(startTimes); err != nil {
			level.Error(jc.logger).Log("msg", "Unable to calculate median start time", "err", err)
		} else {
			jm.medianStartTime = startTime
		}
		if startTime, err := stats.Mean(startTimes); err != nil {
			level.Error(jc.logger).Log("msg", "Unable to calculate average start time", "err", err)
		} else {
			jm.avgStartTime = startTime
		}
	}
	if len(gpuStartTimes) > 0 {
		if gpuStartTime, err := stats.Median(gpuStartTimes); err != nil {
			level.Error(jc.logger).Log("msg", "Unable to calculate median GPU start time", "err", err)
		} else {
			jm.gpuMedianStartTime = gpuStartTime
		}
		if gpuStartTime, err := stats.Mean(gpuStartTimes); err != nil {
			level.Error(jc.logger).Log("msg", "Unable to calculate average GPU start time", "err", err)
		} else {
			jm.gpuAvgStartTime = gpuStartTime
		}
	}

	return &jm, err
}

func (jc *JobsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- jc.pending
	ch <- jc.pendingDep
	ch <- jc.running
	ch <- jc.suspended
	ch <- jc.cancelled
	ch <- jc.completing
	ch <- jc.completed
	ch <- jc.configuring
	ch <- jc.failed
	ch <- jc.timeout
	ch <- jc.preempted
	ch <- jc.nodeFail
	ch <- jc.total
	ch <- jc.medianWaitTime
	ch <- jc.avgWaitTime
	ch <- jc.medianStartTime
	ch <- jc.avgStartTime
	ch <- jc.gpuPending
	ch <- jc.gpuPendingDep
	ch <- jc.gpuRunning
	ch <- jc.gpuSuspended
	ch <- jc.gpuCancelled
	ch <- jc.gpuCompleting
	ch <- jc.gpuCompleted
	ch <- jc.gpuConfiguring
	ch <- jc.gpuFailed
	ch <- jc.gpuTimeout
	ch <- jc.gpuPreempted
	ch <- jc.gpuNodeFail
	ch <- jc.gpuTotal
	ch <- jc.gpuMedianWaitTime
	ch <- jc.gpuAvgWaitTime
	ch <- jc.gpuMedianStartTime
	ch <- jc.gpuAvgStartTime
}

func (jc *JobsCollector) Collect(ch chan<- prometheus.Metric) {
	var errValue float64
	jm, err := jc.metrics()
	if err != nil {
		errValue = 1
	}
	ch <- prometheus.MustNewConstMetric(jc.pending, prometheus.GaugeValue, jm.pending)
	ch <- prometheus.MustNewConstMetric(jc.pendingDep, prometheus.GaugeValue, jm.pendingDep)
	ch <- prometheus.MustNewConstMetric(jc.running, prometheus.GaugeValue, jm.running)
	ch <- prometheus.MustNewConstMetric(jc.suspended, prometheus.GaugeValue, jm.suspended)
	ch <- prometheus.MustNewConstMetric(jc.cancelled, prometheus.GaugeValue, jm.cancelled)
	ch <- prometheus.MustNewConstMetric(jc.completing, prometheus.GaugeValue, jm.completing)
	ch <- prometheus.MustNewConstMetric(jc.completed, prometheus.GaugeValue, jm.completed)
	ch <- prometheus.MustNewConstMetric(jc.configuring, prometheus.GaugeValue, jm.configuring)
	ch <- prometheus.MustNewConstMetric(jc.failed, prometheus.GaugeValue, jm.failed)
	ch <- prometheus.MustNewConstMetric(jc.timeout, prometheus.GaugeValue, jm.timeout)
	ch <- prometheus.MustNewConstMetric(jc.preempted, prometheus.GaugeValue, jm.preempted)
	ch <- prometheus.MustNewConstMetric(jc.nodeFail, prometheus.GaugeValue, jm.nodeFail)
	ch <- prometheus.MustNewConstMetric(jc.total, prometheus.GaugeValue, jm.total)
	ch <- prometheus.MustNewConstMetric(jc.medianWaitTime, prometheus.GaugeValue, jm.medianWaitTime)
	ch <- prometheus.MustNewConstMetric(jc.avgWaitTime, prometheus.GaugeValue, jm.avgWaitTime)
	ch <- prometheus.MustNewConstMetric(jc.medianStartTime, prometheus.GaugeValue, jm.medianStartTime)
	ch <- prometheus.MustNewConstMetric(jc.avgStartTime, prometheus.GaugeValue, jm.avgStartTime)
	ch <- prometheus.MustNewConstMetric(jc.gpuPending, prometheus.GaugeValue, jm.gpuPending)
	ch <- prometheus.MustNewConstMetric(jc.gpuPendingDep, prometheus.GaugeValue, jm.gpuPendingDep)
	ch <- prometheus.MustNewConstMetric(jc.gpuRunning, prometheus.GaugeValue, jm.gpuRunning)
	ch <- prometheus.MustNewConstMetric(jc.gpuSuspended, prometheus.GaugeValue, jm.gpuSuspended)
	ch <- prometheus.MustNewConstMetric(jc.gpuCancelled, prometheus.GaugeValue, jm.gpuCancelled)
	ch <- prometheus.MustNewConstMetric(jc.gpuCompleting, prometheus.GaugeValue, jm.gpuCompleting)
	ch <- prometheus.MustNewConstMetric(jc.gpuCompleted, prometheus.GaugeValue, jm.gpuCompleted)
	ch <- prometheus.MustNewConstMetric(jc.gpuConfiguring, prometheus.GaugeValue, jm.gpuConfiguring)
	ch <- prometheus.MustNewConstMetric(jc.gpuFailed, prometheus.GaugeValue, jm.gpuFailed)
	ch <- prometheus.MustNewConstMetric(jc.gpuTimeout, prometheus.GaugeValue, jm.gpuTimeout)
	ch <- prometheus.MustNewConstMetric(jc.gpuPreempted, prometheus.GaugeValue, jm.gpuPreempted)
	ch <- prometheus.MustNewConstMetric(jc.gpuNodeFail, prometheus.GaugeValue, jm.gpuNodeFail)
	ch <- prometheus.MustNewConstMetric(jc.gpuTotal, prometheus.GaugeValue, jm.gpuTotal)
	ch <- prometheus.MustNewConstMetric(jc.gpuMedianWaitTime, prometheus.GaugeValue, jm.gpuMedianWaitTime)
	ch <- prometheus.MustNewConstMetric(jc.gpuAvgWaitTime, prometheus.GaugeValue, jm.gpuAvgWaitTime)
	ch <- prometheus.MustNewConstMetric(jc.gpuMedianStartTime, prometheus.GaugeValue, jm.gpuMedianStartTime)
	ch <- prometheus.MustNewConstMetric(jc.gpuAvgStartTime, prometheus.GaugeValue, jm.gpuAvgStartTime)
	ch <- prometheus.MustNewConstMetric(collectError, prometheus.GaugeValue, errValue, "jobs")
}
