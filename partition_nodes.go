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

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/ubccr/slurmrest"
)

const (
	partitionNodesNamespace = "partition_nodes"
	partitionCpusNamespace  = "partition_cpus"
	partitionGpusNamespace  = "partition_gpus"
)

type PartitionNodesCollector struct {
	client   *slurmrest.APIClient
	logger   log.Logger
	alloc    *prometheus.Desc
	comp     *prometheus.Desc
	down     *prometheus.Desc
	drain    *prometheus.Desc
	err      *prometheus.Desc
	fail     *prometheus.Desc
	idle     *prometheus.Desc
	inval    *prometheus.Desc
	maint    *prometheus.Desc
	mix      *prometheus.Desc
	planned  *prometheus.Desc
	reboot   *prometheus.Desc
	resv     *prometheus.Desc
	total    *prometheus.Desc
	unknown  *prometheus.Desc
	cpuAlloc *prometheus.Desc
	cpuIdle  *prometheus.Desc
	cpuOther *prometheus.Desc
	cpuTotal *prometheus.Desc
	gpuAlloc *prometheus.Desc
	gpuIdle  *prometheus.Desc
	gpuTotal *prometheus.Desc
}

type partitionNodeMetrics struct {
	alloc    map[string]float64
	comp     map[string]float64
	down     map[string]float64
	drain    map[string]float64
	err      map[string]float64
	fail     map[string]float64
	idle     map[string]float64
	inval    map[string]float64
	maint    map[string]float64
	mix      map[string]float64
	planned  map[string]float64
	reboot   map[string]float64
	resv     map[string]float64
	total    map[string]float64
	unknown  map[string]float64
	cpuAlloc map[string]float64
	cpuIdle  map[string]float64
	cpuOther map[string]float64
	cpuTotal map[string]float64
	gpuAlloc map[string]float64
	gpuIdle  map[string]float64
	gpuTotal map[string]float64
}

func NewPartitionNodesCollector(client *slurmrest.APIClient, logger log.Logger) *PartitionNodesCollector {
	labels := []string{"partition"}
	return &PartitionNodesCollector{
		client: client,
		logger: log.With(logger, "collector", "partition_nodes"),
		alloc: prometheus.NewDesc(prometheus.BuildFQName(namespace, partitionNodesNamespace, "alloc"),
			"Allocated nodes in the partition", labels, nil),
		comp: prometheus.NewDesc(prometheus.BuildFQName(namespace, partitionNodesNamespace, "comp"),
			"Completing nodes in the partition", labels, nil),
		down: prometheus.NewDesc(prometheus.BuildFQName(namespace, partitionNodesNamespace, "down"),
			"Down nodes in the partition", labels, nil),
		drain: prometheus.NewDesc(prometheus.BuildFQName(namespace, partitionNodesNamespace, "drain"),
			"Drain nodes in the partition", labels, nil),
		err: prometheus.NewDesc(prometheus.BuildFQName(namespace, partitionNodesNamespace, "err"),
			"Error nodes in the partition", labels, nil),
		fail: prometheus.NewDesc(prometheus.BuildFQName(namespace, partitionNodesNamespace, "fail"),
			"Fail nodes in the partition", labels, nil),
		idle: prometheus.NewDesc(prometheus.BuildFQName(namespace, partitionNodesNamespace, "idle"),
			"Idle nodes in the partition", labels, nil),
		inval: prometheus.NewDesc(prometheus.BuildFQName(namespace, partitionNodesNamespace, "invalid"),
			"Invalid nodes in the partition", labels, nil),
		maint: prometheus.NewDesc(prometheus.BuildFQName(namespace, partitionNodesNamespace, "maint"),
			"Maint nodes in the partition", labels, nil),
		mix: prometheus.NewDesc(prometheus.BuildFQName(namespace, partitionNodesNamespace, "mix"),
			"Mix nodes in the partition", labels, nil),
		planned: prometheus.NewDesc(prometheus.BuildFQName(namespace, partitionNodesNamespace, "planned"),
			"Planned nodes in the partition", labels, nil),
		reboot: prometheus.NewDesc(prometheus.BuildFQName(namespace, partitionNodesNamespace, "reboot"),
			"Reboot nodes in the partition", labels, nil),
		resv: prometheus.NewDesc(prometheus.BuildFQName(namespace, partitionNodesNamespace, "resv"),
			"Reserved nodes in the partition", labels, nil),
		total: prometheus.NewDesc(prometheus.BuildFQName(namespace, partitionNodesNamespace, "total"),
			"Total nodes in the partition", labels, nil),
		unknown: prometheus.NewDesc(prometheus.BuildFQName(namespace, partitionNodesNamespace, "unknown"),
			"Unknown nodes in the partition", labels, nil),
		cpuAlloc: prometheus.NewDesc(prometheus.BuildFQName(namespace, partitionCpusNamespace, "alloc"),
			"Allocated CPUs in the partition", labels, nil),
		cpuIdle: prometheus.NewDesc(prometheus.BuildFQName(namespace, partitionCpusNamespace, "idle"),
			"Idle CPUs in the partition", labels, nil),
		cpuOther: prometheus.NewDesc(prometheus.BuildFQName(namespace, partitionCpusNamespace, "other"),
			"Mix CPUs in the partition", labels, nil),
		cpuTotal: prometheus.NewDesc(prometheus.BuildFQName(namespace, partitionCpusNamespace, "total"),
			"Total CPUs in the partition", labels, nil),
		gpuAlloc: prometheus.NewDesc(prometheus.BuildFQName(namespace, partitionGpusNamespace, "alloc"),
			"Allocated GPUs in the partition", labels, nil),
		gpuIdle: prometheus.NewDesc(prometheus.BuildFQName(namespace, partitionGpusNamespace, "idle"),
			"Idle GPUs in the partition", labels, nil),
		gpuTotal: prometheus.NewDesc(prometheus.BuildFQName(namespace, partitionGpusNamespace, "total"),
			"Total GPUs in the partition", labels, nil),
	}
}

func (pnc *PartitionNodesCollector) metrics() (*partitionNodeMetrics, error) {
	var pnm partitionNodeMetrics
	ignoredPattern := regexp.MustCompile(*ignorePartitions)

	partitions, resp, err := pnc.client.SlurmApi.SlurmctldGetPartitions(context.Background()).Execute()
	if err != nil {
		level.Error(pnc.logger).Log("msg", "Failed to fetch partitions from slurm rest api", "err", err)
		return &pnm, err
	} else if resp.StatusCode != 200 {
		err = fmt.Errorf("HTTP response not OK while fetching partitions from slurm rest api")
		level.Error(pnc.logger).Log("err", err, "status_code", resp.StatusCode)
		return &pnm, err
	} else if len(partitions.GetErrors()) > 0 {
		for _, err := range partitions.GetErrors() {
			level.Error(pnc.logger).Log("err", err.GetError())
		}
		return &pnm, fmt.Errorf("HTTP response contained %d errors", len(partitions.GetErrors()))
	}
	nodeInfo, resp, err := pnc.client.SlurmApi.SlurmctldGetNodes(context.Background()).Execute()
	if err != nil {
		level.Error(pnc.logger).Log("msg", "Failed to fetch nodes from slurm rest api", "err", err)
		return &pnm, err
	} else if resp.StatusCode != 200 {
		err = fmt.Errorf("HTTP response not OK while fetching nodes from slurm rest api")
		level.Error(pnc.logger).Log("err", err, "status_code", resp.StatusCode)
		return &pnm, err
	} else if len(nodeInfo.GetErrors()) > 0 {
		for _, err := range nodeInfo.GetErrors() {
			level.Error(pnc.logger).Log("err", err.GetError())
		}
		return &pnm, fmt.Errorf("HTTP response contained %d errors", len(nodeInfo.GetErrors()))
	}

	alloc := make(map[string]float64)
	comp := make(map[string]float64)
	down := make(map[string]float64)
	drain := make(map[string]float64)
	errNodes := make(map[string]float64)
	fail := make(map[string]float64)
	idle := make(map[string]float64)
	inval := make(map[string]float64)
	maint := make(map[string]float64)
	mix := make(map[string]float64)
	planned := make(map[string]float64)
	reboot := make(map[string]float64)
	resv := make(map[string]float64)
	total := make(map[string]float64)
	unknown := make(map[string]float64)
	cpuAlloc := make(map[string]float64)
	cpuIdle := make(map[string]float64)
	cpuOther := make(map[string]float64)
	cpuTotal := make(map[string]float64)
	gpuAlloc := make(map[string]float64)
	gpuIdle := make(map[string]float64)
	gpuTotal := make(map[string]float64)

	for _, p := range partitions.GetPartitions() {
		if ignoredPattern.MatchString(p.GetName()) {
			continue
		}
		alloc[p.GetName()] = 0
		comp[p.GetName()] = 0
		down[p.GetName()] = 0
		drain[p.GetName()] = 0
		errNodes[p.GetName()] = 0
		fail[p.GetName()] = 0
		idle[p.GetName()] = 0
		inval[p.GetName()] = 0
		maint[p.GetName()] = 0
		mix[p.GetName()] = 0
		planned[p.GetName()] = 0
		reboot[p.GetName()] = 0
		resv[p.GetName()] = 0
		total[p.GetName()] = 0
		unknown[p.GetName()] = 0
		cpuAlloc[p.GetName()] = 0
		cpuIdle[p.GetName()] = 0
		cpuOther[p.GetName()] = 0
		cpuTotal[p.GetName()] = 0
		gpuAlloc[p.GetName()] = 0
		gpuIdle[p.GetName()] = 0
		gpuTotal[p.GetName()] = 0
	}

	for _, n := range nodeInfo.GetNodes() {
		states := []string{n.GetState()}
		if len(n.GetStateFlags()) > 0 {
			states = n.GetStateFlags()
		}
		for _, p := range n.GetPartitions() {
			total[p]++
			for _, state := range states {
				// Node states
				switch {
				case allocPattern.MatchString(state):
					alloc[p]++
				case compPattern.MatchString(state):
					comp[p]++
				case downPattern.MatchString(state):
					down[p]++
				case drainPattern.MatchString(state):
					drain[p]++
				case failPattern.MatchString(state):
					fail[p]++
				case errPattern.MatchString(state):
					errNodes[p]++
				case idlePattern.MatchString(state):
					idle[p]++
				case invalPattern.MatchString(state):
					inval[p]++
				case maintPattern.MatchString(state):
					maint[p]++
				case mixPattern.MatchString(state):
					mix[p]++
				case plannedPattern.MatchString(state):
					planned[p]++
				case rebootPattern.MatchString(state):
					reboot[p]++
				case resvPattern.MatchString(state):
					resv[p]++
				case unknownPattern.MatchString(state):
					unknown[p]++
				default:
					unknown[p]++
				}
			}

			var down float64
			for _, state := range states {
				if downPattern.MatchString(state) || drainPattern.MatchString(state) ||
					invalPattern.MatchString(state) || failPattern.MatchString(state) {
					down = 1
				}
			}

			// CPUs
			cpuTotal[p] += float64(n.GetCpus())
			cpuAlloc[p] += float64(n.GetAllocCpus())

			if down == 1 {
				cpuOther[p] += float64(n.GetIdleCpus())
			} else {
				cpuIdle[p] += float64(n.GetIdleCpus())
			}

			// GPUs
			tres := parseTres(n.GetTres())
			if tres.GresGpu == 0 {
				continue
			}

			tresUsed := parseTres(n.GetTresUsed())

			gpuAlloc[p] += float64(tresUsed.GresGpu)
			gpuTotal[p] += float64(tres.GresGpu)
			gpuIdle[p] += float64(tres.GresGpu - tresUsed.GresGpu)
		}
	}

	pnm.alloc = alloc
	pnm.comp = comp
	pnm.down = down
	pnm.drain = drain
	pnm.err = errNodes
	pnm.fail = fail
	pnm.idle = idle
	pnm.inval = inval
	pnm.maint = maint
	pnm.mix = mix
	pnm.planned = planned
	pnm.reboot = reboot
	pnm.resv = resv
	pnm.total = total
	pnm.unknown = unknown
	pnm.cpuAlloc = cpuAlloc
	pnm.cpuIdle = cpuIdle
	pnm.cpuOther = cpuOther
	pnm.cpuTotal = cpuTotal
	pnm.gpuAlloc = gpuAlloc
	pnm.gpuIdle = gpuIdle
	pnm.gpuTotal = gpuTotal

	return &pnm, nil
}

func (pnc *PartitionNodesCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- pnc.alloc
	ch <- pnc.comp
	ch <- pnc.down
	ch <- pnc.drain
	ch <- pnc.err
	ch <- pnc.fail
	ch <- pnc.idle
	ch <- pnc.inval
	ch <- pnc.maint
	ch <- pnc.mix
	ch <- pnc.planned
	ch <- pnc.reboot
	ch <- pnc.resv
	ch <- pnc.total
	ch <- pnc.unknown
	ch <- pnc.cpuAlloc
	ch <- pnc.cpuIdle
	ch <- pnc.cpuOther
	ch <- pnc.cpuTotal
	ch <- pnc.gpuAlloc
	ch <- pnc.gpuIdle
	ch <- pnc.gpuTotal
}
func (pnc *PartitionNodesCollector) Collect(ch chan<- prometheus.Metric) {
	var errValue float64
	pnm, err := pnc.metrics()
	if err != nil {
		errValue = 1
	}
	for p, v := range pnm.alloc {
		ch <- prometheus.MustNewConstMetric(pnc.alloc, prometheus.GaugeValue, v, p)
	}
	for p, v := range pnm.comp {
		ch <- prometheus.MustNewConstMetric(pnc.comp, prometheus.GaugeValue, v, p)
	}
	for p, v := range pnm.down {
		ch <- prometheus.MustNewConstMetric(pnc.down, prometheus.GaugeValue, v, p)
	}
	for p, v := range pnm.drain {
		ch <- prometheus.MustNewConstMetric(pnc.drain, prometheus.GaugeValue, v, p)
	}
	for p, v := range pnm.err {
		ch <- prometheus.MustNewConstMetric(pnc.err, prometheus.GaugeValue, v, p)
	}
	for p, v := range pnm.fail {
		ch <- prometheus.MustNewConstMetric(pnc.fail, prometheus.GaugeValue, v, p)
	}
	for p, v := range pnm.idle {
		ch <- prometheus.MustNewConstMetric(pnc.idle, prometheus.GaugeValue, v, p)
	}
	for p, v := range pnm.inval {
		ch <- prometheus.MustNewConstMetric(pnc.inval, prometheus.GaugeValue, v, p)
	}
	for p, v := range pnm.maint {
		ch <- prometheus.MustNewConstMetric(pnc.maint, prometheus.GaugeValue, v, p)
	}
	for p, v := range pnm.mix {
		ch <- prometheus.MustNewConstMetric(pnc.mix, prometheus.GaugeValue, v, p)
	}
	for p, v := range pnm.planned {
		ch <- prometheus.MustNewConstMetric(pnc.planned, prometheus.GaugeValue, v, p)
	}
	for p, v := range pnm.reboot {
		ch <- prometheus.MustNewConstMetric(pnc.reboot, prometheus.GaugeValue, v, p)
	}
	for p, v := range pnm.resv {
		ch <- prometheus.MustNewConstMetric(pnc.resv, prometheus.GaugeValue, v, p)
	}
	for p, v := range pnm.total {
		ch <- prometheus.MustNewConstMetric(pnc.total, prometheus.GaugeValue, v, p)
	}
	for p, v := range pnm.unknown {
		ch <- prometheus.MustNewConstMetric(pnc.unknown, prometheus.GaugeValue, v, p)
	}
	for p, v := range pnm.cpuAlloc {
		ch <- prometheus.MustNewConstMetric(pnc.cpuAlloc, prometheus.GaugeValue, v, p)
	}
	for p, v := range pnm.cpuIdle {
		ch <- prometheus.MustNewConstMetric(pnc.cpuIdle, prometheus.GaugeValue, v, p)
	}
	for p, v := range pnm.cpuOther {
		ch <- prometheus.MustNewConstMetric(pnc.cpuOther, prometheus.GaugeValue, v, p)
	}
	for p, v := range pnm.cpuTotal {
		ch <- prometheus.MustNewConstMetric(pnc.cpuTotal, prometheus.GaugeValue, v, p)
	}
	for p, v := range pnm.gpuAlloc {
		ch <- prometheus.MustNewConstMetric(pnc.gpuAlloc, prometheus.GaugeValue, v, p)
	}
	for p, v := range pnm.gpuIdle {
		ch <- prometheus.MustNewConstMetric(pnc.gpuIdle, prometheus.GaugeValue, v, p)
	}
	for p, v := range pnm.gpuTotal {
		ch <- prometheus.MustNewConstMetric(pnc.gpuTotal, prometheus.GaugeValue, v, p)
	}
	ch <- prometheus.MustNewConstMetric(collectError, prometheus.GaugeValue, errValue, "partition_nodes")
}
