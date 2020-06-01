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
	"regexp"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"github.com/ubccr/go-slurmrest"
)

var (
	partFilter   = []string{"general-compute", "cascade", "gpu"}
	allocPattern = regexp.MustCompile(`^ALLOC`)
	compPattern  = regexp.MustCompile(`^COMP`)
	downPattern  = regexp.MustCompile(`^DOWN`)
	drainPattern = regexp.MustCompile(`^DRAIN`)
	failPattern  = regexp.MustCompile(`^FAIL`)
	errPattern   = regexp.MustCompile(`^ERR`)
	idlePattern  = regexp.MustCompile(`^IDLE`)
	maintPattern = regexp.MustCompile(`^MAINT`)
	mixPattern   = regexp.MustCompile(`^MIX`)
	resvPattern  = regexp.MustCompile(`^RES`)
)

type NodesCollector struct {
	client   *slurmrest.APIClient
	alloc    *prometheus.Desc
	comp     *prometheus.Desc
	down     *prometheus.Desc
	drain    *prometheus.Desc
	err      *prometheus.Desc
	fail     *prometheus.Desc
	idle     *prometheus.Desc
	maint    *prometheus.Desc
	mix      *prometheus.Desc
	resv     *prometheus.Desc
	cpuAlloc *prometheus.Desc
	cpuIdle  *prometheus.Desc
	cpuOther *prometheus.Desc
	cpuTotal *prometheus.Desc
	gpuAlloc *prometheus.Desc
	gpuIdle  *prometheus.Desc
	gpuTotal *prometheus.Desc
}

type nodeMetrics struct {
	alloc    float64
	comp     float64
	down     float64
	drain    float64
	err      float64
	fail     float64
	idle     float64
	maint    float64
	mix      float64
	resv     float64
	cpuAlloc float64
	cpuIdle  float64
	cpuOther float64
	cpuTotal float64
	gpuAlloc float64
	gpuIdle  float64
	gpuTotal float64
}

func NewNodesCollector(client *slurmrest.APIClient) *NodesCollector {
	return &NodesCollector{
		client:   client,
		alloc:    prometheus.NewDesc("slurm_nodes_alloc", "Allocated nodes", nil, nil),
		comp:     prometheus.NewDesc("slurm_nodes_comp", "Completing nodes", nil, nil),
		down:     prometheus.NewDesc("slurm_nodes_down", "Down nodes", nil, nil),
		drain:    prometheus.NewDesc("slurm_nodes_drain", "Drain nodes", nil, nil),
		err:      prometheus.NewDesc("slurm_nodes_err", "Error nodes", nil, nil),
		fail:     prometheus.NewDesc("slurm_nodes_fail", "Fail nodes", nil, nil),
		idle:     prometheus.NewDesc("slurm_nodes_idle", "Idle nodes", nil, nil),
		maint:    prometheus.NewDesc("slurm_nodes_maint", "Maint nodes", nil, nil),
		mix:      prometheus.NewDesc("slurm_nodes_mix", "Mix nodes", nil, nil),
		resv:     prometheus.NewDesc("slurm_nodes_resv", "Reserved nodes", nil, nil),
		cpuAlloc: prometheus.NewDesc("slurm_cpus_alloc", "Allocated CPUs", nil, nil),
		cpuIdle:  prometheus.NewDesc("slurm_cpus_idle", "Idle CPUs", nil, nil),
		cpuOther: prometheus.NewDesc("slurm_cpus_other", "Mix CPUs", nil, nil),
		cpuTotal: prometheus.NewDesc("slurm_cpus_total", "Total CPUs", nil, nil),
		gpuAlloc: prometheus.NewDesc("slurm_gpus_alloc", "Allocated GPUs", nil, nil),
		gpuIdle:  prometheus.NewDesc("slurm_gpus_idle", "Idle GPUs", nil, nil),
		gpuTotal: prometheus.NewDesc("slurm_gpus_total", "Total GPUs", nil, nil),
	}
}

func (nc *NodesCollector) metrics() *nodeMetrics {
	var nm nodeMetrics

	nodeInfo, resp, err := nc.client.DefaultApi.Nodes(context.Background())
	if err != nil {
		log.Errorf("Failed to fetch nodes from slurm rest api: %s", err)
		return &nm
	} else if resp.StatusCode != 200 {
		log.WithFields(log.Fields{
			"status_code": resp.StatusCode,
		}).Error("HTTP response not OK while fetching nodes from slurm rest api")
		return &nm
	}

	for _, n := range nodeInfo.Nodes {
		// Node states
		switch {
		case allocPattern.MatchString(n.State):
			nm.alloc++
		case compPattern.MatchString(n.State):
			nm.comp++
		case downPattern.MatchString(n.State):
			nm.down++
		case drainPattern.MatchString(n.State):
			nm.drain++
		case failPattern.MatchString(n.State):
			nm.fail++
		case errPattern.MatchString(n.State):
			nm.err++
		case idlePattern.MatchString(n.State):
			nm.idle++
		case maintPattern.MatchString(n.State):
			nm.maint++
		case mixPattern.MatchString(n.State):
			nm.mix++
		case resvPattern.MatchString(n.State):
			nm.resv++
		}

		// CPUs
		nm.cpuTotal += float64(n.Cpus)
		nm.cpuAlloc += float64(n.AllocCpus)

		if drainPattern.MatchString(n.State) || downPattern.MatchString(n.State) ||
			failPattern.MatchString(n.State) || errPattern.MatchString(n.State) {
			nm.cpuOther += float64(n.IdleCpus)
		} else {
			nm.cpuIdle += float64(n.IdleCpus)
		}

		// GPUs
		// XXX remove this soon
		found := false
		for _, p := range partFilter {
			if strings.Contains(n.Partitions, p) {
				found = true
				break
			}
		}

		if !found {
			continue
		}

		avail := gpuCountFromTres(n.Tres)
		alloc := gpuCountFromTres(n.TresUsed)
		idle := avail - alloc
		if n.IdleCpus == 0 {
			// No cores available so can't possibly get a GPU
			idle = 0
		} else if idle > int(n.IdleCpus) {
			// Less cores than idle GPUs so adjust accordingly
			idle = idle - int(n.IdleCpus)
		}

		nm.gpuAlloc += float64(alloc)
		nm.gpuTotal += float64(avail)
		nm.gpuIdle += float64(idle)
	}

	return &nm
}

func (nc *NodesCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- nc.alloc
	ch <- nc.comp
	ch <- nc.down
	ch <- nc.drain
	ch <- nc.err
	ch <- nc.fail
	ch <- nc.idle
	ch <- nc.maint
	ch <- nc.mix
	ch <- nc.resv
	ch <- nc.cpuAlloc
	ch <- nc.cpuIdle
	ch <- nc.cpuOther
	ch <- nc.cpuTotal
	ch <- nc.gpuAlloc
	ch <- nc.gpuIdle
	ch <- nc.gpuTotal
}
func (nc *NodesCollector) Collect(ch chan<- prometheus.Metric) {
	nm := nc.metrics()
	ch <- prometheus.MustNewConstMetric(nc.alloc, prometheus.GaugeValue, nm.alloc)
	ch <- prometheus.MustNewConstMetric(nc.comp, prometheus.GaugeValue, nm.comp)
	ch <- prometheus.MustNewConstMetric(nc.down, prometheus.GaugeValue, nm.down)
	ch <- prometheus.MustNewConstMetric(nc.drain, prometheus.GaugeValue, nm.drain)
	ch <- prometheus.MustNewConstMetric(nc.err, prometheus.GaugeValue, nm.err)
	ch <- prometheus.MustNewConstMetric(nc.fail, prometheus.GaugeValue, nm.fail)
	ch <- prometheus.MustNewConstMetric(nc.idle, prometheus.GaugeValue, nm.idle)
	ch <- prometheus.MustNewConstMetric(nc.maint, prometheus.GaugeValue, nm.maint)
	ch <- prometheus.MustNewConstMetric(nc.mix, prometheus.GaugeValue, nm.mix)
	ch <- prometheus.MustNewConstMetric(nc.resv, prometheus.GaugeValue, nm.resv)
	ch <- prometheus.MustNewConstMetric(nc.cpuAlloc, prometheus.GaugeValue, nm.cpuAlloc)
	ch <- prometheus.MustNewConstMetric(nc.cpuIdle, prometheus.GaugeValue, nm.cpuIdle)
	ch <- prometheus.MustNewConstMetric(nc.cpuOther, prometheus.GaugeValue, nm.cpuOther)
	ch <- prometheus.MustNewConstMetric(nc.cpuTotal, prometheus.GaugeValue, nm.cpuTotal)
	ch <- prometheus.MustNewConstMetric(nc.gpuAlloc, prometheus.GaugeValue, nm.gpuAlloc)
	ch <- prometheus.MustNewConstMetric(nc.gpuIdle, prometheus.GaugeValue, nm.gpuIdle)
	ch <- prometheus.MustNewConstMetric(nc.gpuTotal, prometheus.GaugeValue, nm.gpuTotal)
}
