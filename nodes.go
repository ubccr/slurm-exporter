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

	kingpin "github.com/alecthomas/kingpin/v2"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/ubccr/slurmrest"
)

const (
	nodesNamespace = "nodes"
	nodeNamespace  = "node"
	cpusNamespace  = "cpus"
	gpusNamespace  = "gpus"
)

var (
	ignoreNodeFeatures = kingpin.Flag("collector.nodes.ignore-features",
		"Regular expression of node features to ignore").Default("^$").String()
)

type NodesCollector struct {
	client       *slurmrest.APIClient
	logger       log.Logger
	alloc        *prometheus.Desc
	comp         *prometheus.Desc
	down         *prometheus.Desc
	drain        *prometheus.Desc
	err          *prometheus.Desc
	fail         *prometheus.Desc
	idle         *prometheus.Desc
	inval        *prometheus.Desc
	maint        *prometheus.Desc
	mix          *prometheus.Desc
	planned      *prometheus.Desc
	reboot       *prometheus.Desc
	resv         *prometheus.Desc
	total        *prometheus.Desc
	unknown      *prometheus.Desc
	nodeStates   *prometheus.Desc
	nodeDown     *prometheus.Desc
	nodeFeatures *prometheus.Desc
	cpuAlloc     *prometheus.Desc
	cpuIdle      *prometheus.Desc
	cpuOther     *prometheus.Desc
	cpuTotal     *prometheus.Desc
	gpuAlloc     *prometheus.Desc
	gpuIdle      *prometheus.Desc
	gpuTotal     *prometheus.Desc
}

type nodeMetrics struct {
	alloc          float64
	comp           float64
	down           float64
	drain          float64
	err            float64
	fail           float64
	idle           float64
	inval          float64
	maint          float64
	mix            float64
	planned        float64
	reboot         float64
	resv           float64
	total          float64
	unknown        float64
	nodeStates     map[string][]string
	nodeDown       map[string]float64
	nodeDownReason map[string]string
	nodeFeatures   map[string]string
	cpuAlloc       float64
	cpuIdle        float64
	cpuOther       float64
	cpuTotal       float64
	gpuAlloc       float64
	gpuIdle        float64
	gpuTotal       float64
}

func NewNodesCollector(client *slurmrest.APIClient, logger log.Logger) *NodesCollector {
	return &NodesCollector{
		client: client,
		logger: log.With(logger, "collector", "nodes"),
		alloc: prometheus.NewDesc(prometheus.BuildFQName(namespace, nodesNamespace, "alloc"),
			"Allocated nodes", nil, nil),
		comp: prometheus.NewDesc(prometheus.BuildFQName(namespace, nodesNamespace, "comp"),
			"Completing nodes", nil, nil),
		down: prometheus.NewDesc(prometheus.BuildFQName(namespace, nodesNamespace, "down"),
			"Down nodes", nil, nil),
		drain: prometheus.NewDesc(prometheus.BuildFQName(namespace, nodesNamespace, "drain"),
			"Drain nodes", nil, nil),
		err: prometheus.NewDesc(prometheus.BuildFQName(namespace, nodesNamespace, "err"),
			"Error nodes", nil, nil),
		fail: prometheus.NewDesc(prometheus.BuildFQName(namespace, nodesNamespace, "fail"),
			"Fail nodes", nil, nil),
		idle: prometheus.NewDesc(prometheus.BuildFQName(namespace, nodesNamespace, "idle"),
			"Idle nodes", nil, nil),
		inval: prometheus.NewDesc(prometheus.BuildFQName(namespace, nodesNamespace, "invalid"),
			"Invalid nodes", nil, nil),
		maint: prometheus.NewDesc(prometheus.BuildFQName(namespace, nodesNamespace, "maint"),
			"Maint nodes", nil, nil),
		mix: prometheus.NewDesc(prometheus.BuildFQName(namespace, nodesNamespace, "mix"),
			"Mix nodes", nil, nil),
		planned: prometheus.NewDesc(prometheus.BuildFQName(namespace, nodesNamespace, "planned"),
			"Planned nodes", nil, nil),
		reboot: prometheus.NewDesc(prometheus.BuildFQName(namespace, nodesNamespace, "reboot"),
			"Reboot nodes", nil, nil),
		resv: prometheus.NewDesc(prometheus.BuildFQName(namespace, nodesNamespace, "resv"),
			"Reserved nodes", nil, nil),
		total: prometheus.NewDesc(prometheus.BuildFQName(namespace, nodesNamespace, "total"),
			"Total nodes", nil, nil),
		unknown: prometheus.NewDesc(prometheus.BuildFQName(namespace, nodesNamespace, "unknown"),
			"Unknown nodes", nil, nil),
		nodeStates: prometheus.NewDesc(prometheus.BuildFQName(namespace, nodeNamespace, "state_info"),
			"Node state", []string{"node", "state"}, nil),
		nodeDown: prometheus.NewDesc(prometheus.BuildFQName(namespace, nodeNamespace, "down"),
			"Indicates if a node is down, 1=down 0=not down", []string{"node", "reason"}, nil),
		nodeFeatures: prometheus.NewDesc(prometheus.BuildFQName(namespace, nodeNamespace, "features_info"),
			"Node features", []string{"node", "features"}, nil),
		cpuAlloc: prometheus.NewDesc(prometheus.BuildFQName(namespace, cpusNamespace, "alloc"),
			"Allocated CPUs", nil, nil),
		cpuIdle: prometheus.NewDesc(prometheus.BuildFQName(namespace, cpusNamespace, "idle"),
			"Idle CPUs", nil, nil),
		cpuOther: prometheus.NewDesc(prometheus.BuildFQName(namespace, cpusNamespace, "other"),
			"Mix CPUs", nil, nil),
		cpuTotal: prometheus.NewDesc(prometheus.BuildFQName(namespace, cpusNamespace, "total"),
			"Total CPUs", nil, nil),
		gpuAlloc: prometheus.NewDesc(prometheus.BuildFQName(namespace, gpusNamespace, "alloc"),
			"Allocated GPUs", nil, nil),
		gpuIdle: prometheus.NewDesc(prometheus.BuildFQName(namespace, gpusNamespace, "idle"),
			"Idle GPUs", nil, nil),
		gpuTotal: prometheus.NewDesc(prometheus.BuildFQName(namespace, gpusNamespace, "total"),
			"Total GPUs", nil, nil),
	}
}

func (nc *NodesCollector) metrics() (*nodeMetrics, error) {
	var nm nodeMetrics
	ignoreFeatures := regexp.MustCompile(*ignoreNodeFeatures)
	nodeStates := make(map[string][]string)
	nodeDown := make(map[string]float64)
	nodeDownReason := make(map[string]string)
	nodeFeatures := make(map[string]string)

	req := nc.client.SlurmAPI.SlurmV0040GetNodes(context.Background())
	nodeInfo, resp, err := nc.client.SlurmAPI.SlurmV0040GetNodesExecute(req)
	if err != nil {
		level.Error(nc.logger).Log("msg", "Failed to fetch nodes from slurm rest api", "err", err)
		return &nm, err
	} else if resp.StatusCode != 200 {
		err = fmt.Errorf("HTTP response not OK while fetching nodes from slurm rest api")
		level.Error(nc.logger).Log("err", err, "status_code", resp.StatusCode)
		return &nm, err
	} else if len(nodeInfo.GetErrors()) > 0 {
		for _, err := range nodeInfo.GetErrors() {
			level.Error(nc.logger).Log("err", err.GetError())
		}
		return &nm, fmt.Errorf("HTTP response contained %d errors", len(nodeInfo.GetErrors()))
	}

	for _, n := range nodeInfo.GetNodes() {
		states := n.GetState()
		nm.total++
		for _, state := range states {
			// Node states
			switch {
			case allocPattern.MatchString(state):
				nm.alloc++
			case compPattern.MatchString(state):
				nm.comp++
			case downPattern.MatchString(state):
				nm.down++
			case drainPattern.MatchString(state):
				nm.drain++
			case failPattern.MatchString(state):
				nm.fail++
			case errPattern.MatchString(state):
				nm.err++
			case idlePattern.MatchString(state):
				nm.idle++
			case invalPattern.MatchString(state):
				nm.inval++
			case maintPattern.MatchString(state):
				nm.maint++
			case mixPattern.MatchString(state):
				nm.mix++
			case plannedPattern.MatchString(state):
				nm.planned++
			case rebootPattern.MatchString(state):
				nm.reboot++
			case resvPattern.MatchString(state):
				nm.resv++
			case unknownPattern.MatchString(state):
				nm.unknown++
			default:
				nm.unknown++
			}
			nodeStates[n.GetName()] = append(nodeStates[n.GetName()], strings.ToLower(state))
		}

		var down float64
		for _, state := range states {
			if downPattern.MatchString(state) || drainPattern.MatchString(state) ||
				invalPattern.MatchString(state) || failPattern.MatchString(state) {
				down = 1
			}
		}
		nodeDown[n.GetName()] = down
		nodeDownReason[n.GetName()] = n.GetReason()

		features := []string{}
		for _, feature := range n.GetFeatures() {
			if !ignoreFeatures.MatchString(feature) {
				features = append(features, feature)
			}
		}
		nodeFeatures[n.GetName()] = strings.Join(features, ",")

		// CPUs
		nm.cpuTotal += float64(n.GetCpus())
		nm.cpuAlloc += float64(n.GetAllocCpus())

		if down == 1 {
			nm.cpuOther += float64(n.GetAllocIdleCpus())
		} else {
			nm.cpuIdle += float64(n.GetAllocIdleCpus())
		}

		// GPUs
		tres := parseTres(n.GetTres())
		if tres.GresGpu == 0 {
			continue
		}

		tresUsed := parseTres(n.GetTresUsed())
		nm.gpuAlloc += float64(tresUsed.GresGpu)
		nm.gpuTotal += float64(tres.GresGpu)
		nm.gpuIdle += float64(tres.GresGpu - tresUsed.GresGpu)
	}

	nm.nodeStates = nodeStates
	nm.nodeDown = nodeDown
	nm.nodeDownReason = nodeDownReason
	nm.nodeFeatures = nodeFeatures
	return &nm, nil
}

func (nc *NodesCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- nc.alloc
	ch <- nc.comp
	ch <- nc.down
	ch <- nc.drain
	ch <- nc.err
	ch <- nc.fail
	ch <- nc.idle
	ch <- nc.inval
	ch <- nc.maint
	ch <- nc.mix
	ch <- nc.planned
	ch <- nc.reboot
	ch <- nc.resv
	ch <- nc.total
	ch <- nc.unknown
	ch <- nc.nodeStates
	ch <- nc.nodeDown
	ch <- nc.nodeFeatures
	ch <- nc.cpuAlloc
	ch <- nc.cpuIdle
	ch <- nc.cpuOther
	ch <- nc.cpuTotal
	ch <- nc.gpuAlloc
	ch <- nc.gpuIdle
	ch <- nc.gpuTotal
}
func (nc *NodesCollector) Collect(ch chan<- prometheus.Metric) {
	var errValue float64
	nm, err := nc.metrics()
	if err != nil {
		errValue = 1
	}
	ch <- prometheus.MustNewConstMetric(nc.alloc, prometheus.GaugeValue, nm.alloc)
	ch <- prometheus.MustNewConstMetric(nc.comp, prometheus.GaugeValue, nm.comp)
	ch <- prometheus.MustNewConstMetric(nc.down, prometheus.GaugeValue, nm.down)
	ch <- prometheus.MustNewConstMetric(nc.drain, prometheus.GaugeValue, nm.drain)
	ch <- prometheus.MustNewConstMetric(nc.err, prometheus.GaugeValue, nm.err)
	ch <- prometheus.MustNewConstMetric(nc.fail, prometheus.GaugeValue, nm.fail)
	ch <- prometheus.MustNewConstMetric(nc.idle, prometheus.GaugeValue, nm.idle)
	ch <- prometheus.MustNewConstMetric(nc.inval, prometheus.GaugeValue, nm.inval)
	ch <- prometheus.MustNewConstMetric(nc.maint, prometheus.GaugeValue, nm.maint)
	ch <- prometheus.MustNewConstMetric(nc.mix, prometheus.GaugeValue, nm.mix)
	ch <- prometheus.MustNewConstMetric(nc.planned, prometheus.GaugeValue, nm.planned)
	ch <- prometheus.MustNewConstMetric(nc.reboot, prometheus.GaugeValue, nm.reboot)
	ch <- prometheus.MustNewConstMetric(nc.resv, prometheus.GaugeValue, nm.resv)
	ch <- prometheus.MustNewConstMetric(nc.total, prometheus.GaugeValue, nm.total)
	ch <- prometheus.MustNewConstMetric(nc.unknown, prometheus.GaugeValue, nm.unknown)
	for node, states := range nm.nodeStates {
		for _, state := range states {
			ch <- prometheus.MustNewConstMetric(nc.nodeStates, prometheus.GaugeValue, 1, node, state)
		}
	}
	for node, down := range nm.nodeDown {
		var reason string
		if r, ok := nm.nodeDownReason[node]; ok {
			reason = r
		}
		ch <- prometheus.MustNewConstMetric(nc.nodeDown, prometheus.GaugeValue, down, node, reason)
	}
	for node, features := range nm.nodeFeatures {
		ch <- prometheus.MustNewConstMetric(nc.nodeFeatures, prometheus.GaugeValue, 1, node, features)
	}
	ch <- prometheus.MustNewConstMetric(nc.cpuAlloc, prometheus.GaugeValue, nm.cpuAlloc)
	ch <- prometheus.MustNewConstMetric(nc.cpuIdle, prometheus.GaugeValue, nm.cpuIdle)
	ch <- prometheus.MustNewConstMetric(nc.cpuOther, prometheus.GaugeValue, nm.cpuOther)
	ch <- prometheus.MustNewConstMetric(nc.cpuTotal, prometheus.GaugeValue, nm.cpuTotal)
	ch <- prometheus.MustNewConstMetric(nc.gpuAlloc, prometheus.GaugeValue, nm.gpuAlloc)
	ch <- prometheus.MustNewConstMetric(nc.gpuIdle, prometheus.GaugeValue, nm.gpuIdle)
	ch <- prometheus.MustNewConstMetric(nc.gpuTotal, prometheus.GaugeValue, nm.gpuTotal)
	ch <- prometheus.MustNewConstMetric(collectError, prometheus.GaugeValue, errValue, "nodes")
}
