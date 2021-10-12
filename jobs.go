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
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/ubccr/slurmrest"
)

type JobsCollector struct {
	client         *slurmrest.APIClient
	logger         log.Logger
	pending        *prometheus.Desc
	pendingDep     *prometheus.Desc
	running        *prometheus.Desc
	suspended      *prometheus.Desc
	cancelled      *prometheus.Desc
	completing     *prometheus.Desc
	completed      *prometheus.Desc
	configuring    *prometheus.Desc
	failed         *prometheus.Desc
	timeout        *prometheus.Desc
	preempted      *prometheus.Desc
	nodeFail       *prometheus.Desc
	waitTime       *prometheus.Desc
	waitTimeGpu    *prometheus.Desc
	waitTime128    *prometheus.Desc
	waitTime256    *prometheus.Desc
	startTime      *prometheus.Desc
	gpuPending     *prometheus.Desc
	gpuPendingDep  *prometheus.Desc
	gpuRunning     *prometheus.Desc
	gpuSuspended   *prometheus.Desc
	gpuCancelled   *prometheus.Desc
	gpuCompleting  *prometheus.Desc
	gpuCompleted   *prometheus.Desc
	gpuConfiguring *prometheus.Desc
	gpuFailed      *prometheus.Desc
	gpuTimeout     *prometheus.Desc
	gpuPreempted   *prometheus.Desc
	gpuNodeFail    *prometheus.Desc
}

type jobMetrics struct {
	pending        float64
	pendingDep     float64
	running        float64
	suspended      float64
	cancelled      float64
	completing     float64
	completed      float64
	configuring    float64
	failed         float64
	timeout        float64
	preempted      float64
	nodeFail       float64
	waitTime       float64
	waitTimeGpu    float64
	waitTime128    float64
	waitTime256    float64
	startTime      float64
	gpuPending     float64
	gpuPendingDep  float64
	gpuRunning     float64
	gpuSuspended   float64
	gpuCancelled   float64
	gpuCompleting  float64
	gpuCompleted   float64
	gpuConfiguring float64
	gpuFailed      float64
	gpuTimeout     float64
	gpuPreempted   float64
	gpuNodeFail    float64
}

type timeMetric struct {
	total int64
	count int
}

func (t *timeMetric) average() float64 {
	if t.count == 0 {
		return 0.0
	}
	return float64(t.total) / float64(t.count)
}

func NewJobsCollector(client *slurmrest.APIClient, logger log.Logger) *JobsCollector {
	return &JobsCollector{
		client:         client,
		logger:         log.With(logger, "collector", "jobs"),
		pending:        prometheus.NewDesc("slurm_queue_pending", "Pending jobs in queue", nil, nil),
		pendingDep:     prometheus.NewDesc("slurm_queue_pending_dependency", "Pending jobs because of dependency in queue", nil, nil),
		running:        prometheus.NewDesc("slurm_queue_running", "Running jobs in the cluster", nil, nil),
		suspended:      prometheus.NewDesc("slurm_queue_suspended", "Suspended jobs in the cluster", nil, nil),
		cancelled:      prometheus.NewDesc("slurm_queue_cancelled", "Cancelled jobs in the cluster", nil, nil),
		completing:     prometheus.NewDesc("slurm_queue_completing", "Completing jobs in the cluster", nil, nil),
		completed:      prometheus.NewDesc("slurm_queue_completed", "Completed jobs in the cluster", nil, nil),
		configuring:    prometheus.NewDesc("slurm_queue_configuring", "Configuring jobs in the cluster", nil, nil),
		failed:         prometheus.NewDesc("slurm_queue_failed", "Number of failed jobs", nil, nil),
		timeout:        prometheus.NewDesc("slurm_queue_timeout", "Jobs stopped by timeout", nil, nil),
		preempted:      prometheus.NewDesc("slurm_queue_preempted", "Number of preempted jobs", nil, nil),
		nodeFail:       prometheus.NewDesc("slurm_queue_node_fail", "Number of jobs stopped due to node fail", nil, nil),
		waitTime:       prometheus.NewDesc("slurm_queue_wait_time", "Average wait time of running jobs", nil, nil),
		waitTimeGpu:    prometheus.NewDesc("slurm_queue_wait_time_gpu", "Average wait time of running jobs that requested GPU", nil, nil),
		waitTime128:    prometheus.NewDesc("slurm_queue_wait_time_128", "Average wait time of running jobs that requested 128G RAM or greater", nil, nil),
		waitTime256:    prometheus.NewDesc("slurm_queue_wait_time_256", "Average wait time of running jobs that requested 256G RAM or greater", nil, nil),
		startTime:      prometheus.NewDesc("slurm_queue_start_time", "Average estimated start time of pending jobs", nil, nil),
		gpuPending:     prometheus.NewDesc("slurm_gres_gpu_pending", "Pending gres/gpu jobs in queue", nil, nil),
		gpuPendingDep:  prometheus.NewDesc("slurm_gres_gpu_pending_dependency", "Pending gres/gpu jobs because of dependency in queue", nil, nil),
		gpuRunning:     prometheus.NewDesc("slurm_gres_gpu_running", "Running gres/gpu jobs in the cluster", nil, nil),
		gpuSuspended:   prometheus.NewDesc("slurm_gres_gpu_suspended", "Suspended gres/gpu jobs in the cluster", nil, nil),
		gpuCancelled:   prometheus.NewDesc("slurm_gres_gpu_cancelled", "Cancelled gres/gpu jobs in the cluster", nil, nil),
		gpuCompleting:  prometheus.NewDesc("slurm_gres_gpu_completing", "Completing gres/gpu jobs in the cluster", nil, nil),
		gpuCompleted:   prometheus.NewDesc("slurm_gres_gpu_completed", "Completed gres/gpu jobs in the cluster", nil, nil),
		gpuConfiguring: prometheus.NewDesc("slurm_gres_gpu_configuring", "Configuring gres/gpu jobs in the cluster", nil, nil),
		gpuFailed:      prometheus.NewDesc("slurm_gres_gpu_failed", "Number of failed gres/gpu jobs", nil, nil),
		gpuTimeout:     prometheus.NewDesc("slurm_gres_gpu_timeout", "gres/gpu Jobs stopped by timeout", nil, nil),
		gpuPreempted:   prometheus.NewDesc("slurm_gres_gpu_preempted", "Number of preempted gres/gpu jobs", nil, nil),
		gpuNodeFail:    prometheus.NewDesc("slurm_gres_gpu_node_fail", "Number of gres/gpu jobs stopped due to node fail", nil, nil),
	}
}

func (jc *JobsCollector) metrics() (*jobMetrics, error) {
	var jm jobMetrics

	req := jc.client.SlurmApi.SlurmctldGetJobs(context.Background())
	jobs, resp, err := jc.client.SlurmApi.SlurmctldGetJobsExecute(req)
	if err != nil {
		level.Error(jc.logger).Log("msg", "Failed to fetch jobs from slurm rest api", "err", err)
		return &jm, err
	} else if resp.StatusCode != 200 {
		err = fmt.Errorf("HTTP response not OK while fetching jobs from slurm rest api")
		level.Error(jc.logger).Log("err", err, "status_code", resp.StatusCode)
		return &jm, err
	} else if len(jobs.GetErrors()) > 0 {
		for _, err := range jobs.GetErrors() {
			level.Error(jc.logger).Log("err", err)
		}
		return &jm, fmt.Errorf("HTTP response contained %d errors", len(jobs.GetErrors()))
	}

	waitTime := &timeMetric{}
	waitTimeGpu := &timeMetric{}
	waitTime128 := &timeMetric{}
	waitTime256 := &timeMetric{}
	startTime := &timeMetric{}
	now := time.Now().Local().Unix()

	for _, j := range jobs.GetJobs() {
		tres := parseTres(j.GetTresAllocStr())

		switch j.GetJobState() {
		case "PENDING":
			jm.pending++
			if j.GetStateReason() == "Dependency" {
				jm.pendingDep++
			}
			if j.GetStartTime() >= now {
				startTime.count++
				startTime.total += j.GetStartTime() - now
			}
			if tres.GresGpu > 0 {
				jm.gpuPending++
				if j.GetStateReason() == "Dependency" {
					jm.gpuPendingDep++
				}
			}
		case "RUNNING":
			jm.running++
			waitTime.count++
			waitTime.total += j.GetStartTime() - j.GetSubmitTime()
			if tres.GresGpu > 0 {
				jm.gpuRunning++
				level.Info(jc.logger).Log("msg", "GPU Job",
					"job_id", j.GetJobId(), "partition", j.GetPartition(), "wait_time", (j.GetStartTime() - j.GetSubmitTime()))
				waitTimeGpu.count++
				waitTimeGpu.total += j.GetStartTime() - j.GetSubmitTime()
			}
			if (tres.Memory/uint64(tres.Node)) >= 128000000000 &&
				(tres.Memory/uint64(tres.Node)) < 256000000000 {
				level.Info(jc.logger).Log("msg", "Large Mem 128G Job",
					"job_id", j.GetJobId(), "partition", j.GetPartition(), "wait_time", (j.GetStartTime() - j.GetSubmitTime()))
				waitTime128.count++
				waitTime128.total += j.GetStartTime() - j.GetSubmitTime()
			}
			if (tres.Memory / uint64(tres.Node)) >= 256000000000 {
				level.Info(jc.logger).Log("msg", "Large Mem 256G Job",
					"job_id", j.GetJobId(), "partition", j.GetPartition(), "wait_time", (j.GetStartTime() - j.GetSubmitTime()))
				waitTime256.count++
				waitTime256.total += j.GetStartTime() - j.GetSubmitTime()
			}
		case "SUSPENDED":
			jm.suspended++
			if tres.GresGpu > 0 {
				jm.gpuSuspended++
			}
		case "CANCELLED":
			jm.cancelled++
			if tres.GresGpu > 0 {
				jm.gpuCancelled++
			}
		case "COMPLETING":
			jm.completing++
			if tres.GresGpu > 0 {
				jm.gpuCompleting++
			}
		case "COMPLETED":
			jm.completed++
			if tres.GresGpu > 0 {
				jm.gpuCompleted++
			}
		case "CONFIGURING":
			jm.configuring++
			if tres.GresGpu > 0 {
				jm.gpuConfiguring++
			}
		case "FAILED":
			jm.failed++
			if tres.GresGpu > 0 {
				jm.gpuFailed++
			}
		case "TIMEOUT":
			jm.timeout++
			if tres.GresGpu > 0 {
				jm.gpuTimeout++
			}
		case "PREEMPTED":
			jm.preempted++
			if tres.GresGpu > 0 {
				jm.gpuPreempted++
			}
		case "NODE_FAIL":
			jm.nodeFail++
			if tres.GresGpu > 0 {
				jm.gpuFailed++
			}
		}
	}

	jm.waitTime = waitTime.average()
	jm.waitTimeGpu = waitTimeGpu.average()
	jm.waitTime128 = waitTime128.average()
	jm.waitTime256 = waitTime256.average()
	jm.startTime = startTime.average()

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
	ch <- jc.waitTime
	ch <- jc.waitTimeGpu
	ch <- jc.waitTime128
	ch <- jc.waitTime256
	ch <- jc.startTime
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
	ch <- prometheus.MustNewConstMetric(jc.waitTime, prometheus.GaugeValue, jm.waitTime)
	ch <- prometheus.MustNewConstMetric(jc.waitTimeGpu, prometheus.GaugeValue, jm.waitTimeGpu)
	ch <- prometheus.MustNewConstMetric(jc.waitTime128, prometheus.GaugeValue, jm.waitTime128)
	ch <- prometheus.MustNewConstMetric(jc.waitTime256, prometheus.GaugeValue, jm.waitTime256)
	ch <- prometheus.MustNewConstMetric(jc.startTime, prometheus.GaugeValue, jm.startTime)
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
	ch <- prometheus.MustNewConstMetric(collectError, prometheus.GaugeValue, errValue, "jobs")
}
