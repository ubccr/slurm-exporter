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
	pingNamespace = "ping"
)

type PingCollector struct {
	client  *slurmrest.APIClient
	logger  log.Logger
	status  *prometheus.Desc
	latency *prometheus.Desc
}

type pingMetrics struct {
	hostname string
	mode     string
	status   float64
	latency  float64
}

func NewPingCollector(client *slurmrest.APIClient, logger log.Logger) *PingCollector {
	pingLabels := []string{"hostname", "mode"}
	return &PingCollector{
		client: client,
		logger: log.With(logger, "collector", "ping"),
		status: prometheus.NewDesc(prometheus.BuildFQName(namespace, pingNamespace, "status"),
			"Ping Status", pingLabels, nil),
		latency: prometheus.NewDesc(prometheus.BuildFQName(namespace, pingNamespace, "latency"),
			"Ping Latency", pingLabels, nil),
	}
}

func (pc *PingCollector) metrics() ([]pingMetrics, error) {
	var pms []pingMetrics

	pings, resp, err := pc.client.SlurmAPI.SlurmV0040GetPing(context.Background()).Execute()
	if err != nil {
		level.Error(pc.logger).Log("msg", "Failed to fetch pings from slurm rest api", "err", err)
		return pms, err
	} else if resp.StatusCode != 200 {
		err = fmt.Errorf("HTTP response not OK while fetching pings from slurm rest api")
		level.Error(pc.logger).Log("err", err, "status_code", resp.StatusCode)
		return pms, err
	} else if len(pings.GetErrors()) > 0 {
		for _, err := range pings.GetErrors() {
			level.Error(pc.logger).Log("err", err.GetError())
		}
		return pms, fmt.Errorf("HTTP response contained %d errors", len(pings.GetErrors()))
	}

	for _, p := range pings.GetPings() {
		pm := pingMetrics{
			hostname: p.GetHostname(),
			latency:  float64(p.GetLatency()),
			mode:     p.GetMode(),
		}
		if p.GetPinged() == "UP" {
			pm.status = float64(1)
		} else {
			pm.status = float64(0)
		}
		pms = append(pms, pm)
	}

	return pms, err
}

func (pc *PingCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- pc.latency
	ch <- pc.status
}

func (pc *PingCollector) Collect(ch chan<- prometheus.Metric) {
	var errValue float64
	pm, err := pc.metrics()
	if err != nil {
		errValue = 1
	}
	for _, p := range pm {
		ch <- prometheus.MustNewConstMetric(pc.status, prometheus.GaugeValue, p.status, p.hostname, p.mode)
		ch <- prometheus.MustNewConstMetric(pc.latency, prometheus.GaugeValue, p.latency, p.hostname, p.mode)
	}
	ch <- prometheus.MustNewConstMetric(collectError, prometheus.GaugeValue, errValue, "pings")
}
