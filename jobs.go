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

type JobsCollector struct {
	client      *slurmrest.APIClient
	pending     *prometheus.Desc
	pendingDep  *prometheus.Desc
	running     *prometheus.Desc
	suspended   *prometheus.Desc
	cancelled   *prometheus.Desc
	completing  *prometheus.Desc
	completed   *prometheus.Desc
	configuring *prometheus.Desc
	failed      *prometheus.Desc
	timeout     *prometheus.Desc
	preempted   *prometheus.Desc
	nodeFail    *prometheus.Desc
}

type jobMetrics struct {
	pending     float64
	pendingDep  float64
	running     float64
	suspended   float64
	cancelled   float64
	completing  float64
	completed   float64
	configuring float64
	failed      float64
	timeout     float64
	preempted   float64
	nodeFail    float64
}

func NewJobsCollector(client *slurmrest.APIClient) *JobsCollector {
	return &JobsCollector{
		client:      client,
		pending:     prometheus.NewDesc("slurm_queue_pending", "Pending jobs in queue", nil, nil),
		pendingDep:  prometheus.NewDesc("slurm_queue_pending_dependency", "Pending jobs because of dependency in queue", nil, nil),
		running:     prometheus.NewDesc("slurm_queue_running", "Running jobs in the cluster", nil, nil),
		suspended:   prometheus.NewDesc("slurm_queue_suspended", "Suspended jobs in the cluster", nil, nil),
		cancelled:   prometheus.NewDesc("slurm_queue_cancelled", "Cancelled jobs in the cluster", nil, nil),
		completing:  prometheus.NewDesc("slurm_queue_completing", "Completing jobs in the cluster", nil, nil),
		completed:   prometheus.NewDesc("slurm_queue_completed", "Completed jobs in the cluster", nil, nil),
		configuring: prometheus.NewDesc("slurm_queue_configuring", "Configuring jobs in the cluster", nil, nil),
		failed:      prometheus.NewDesc("slurm_queue_failed", "Number of failed jobs", nil, nil),
		timeout:     prometheus.NewDesc("slurm_queue_timeout", "Jobs stopped by timeout", nil, nil),
		preempted:   prometheus.NewDesc("slurm_queue_preempted", "Number of preempted jobs", nil, nil),
		nodeFail:    prometheus.NewDesc("slurm_queue_node_fail", "Number of jobs stopped due to node fail", nil, nil),
	}
}

func (jc *JobsCollector) metrics() *jobMetrics {
	var jm jobMetrics

	jobs, resp, err := jc.client.DefaultApi.Jobs(context.Background())
	if err != nil {
		log.Errorf("Failed to fetch jobs from slurm rest api: %s", err)
		return &jm
	} else if resp.StatusCode != 200 {
		log.WithFields(log.Fields{
			"status_code": resp.StatusCode,
		}).Error("HTTP response not OK while fetching jobs from slurm rest api")
		return &jm
	}

	for _, j := range jobs {
		switch j.JobState {
		case "PENDING":
			jm.pending++
			if j.StateReason == "Dependency" {
				jm.pendingDep++
			}
		case "RUNNING":
			jm.running++
		case "SUSPENDED":
			jm.suspended++
		case "CANCELLED":
			jm.cancelled++
		case "COMPLETING":
			jm.completing++
		case "COMPLETED":
			jm.completed++
		case "CONFIGURING":
			jm.configuring++
		case "FAILED":
			jm.failed++
		case "TIMEOUT":
			jm.timeout++
		case "PREEMPTED":
			jm.preempted++
		case "NODE_FAIL":
			jm.nodeFail++
		}
	}

	return &jm
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
}

func (jc *JobsCollector) Collect(ch chan<- prometheus.Metric) {
	jm := jc.metrics()
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
}
