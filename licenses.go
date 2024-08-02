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
	"strconv"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/ubccr/slurmrest"
)

const (
	licenseNamespace = "license"
)

type LicensesCollector struct {
	client   *slurmrest.APIClient
	logger   log.Logger
	total    *prometheus.Desc
	used     *prometheus.Desc
	free     *prometheus.Desc
	reserved *prometheus.Desc
}

type licenseMetrics struct {
	name     string
	total    float64
	used     float64
	free     float64
	reserved float64
	remote   bool
}

func NewLicensesCollector(client *slurmrest.APIClient, logger log.Logger) *LicensesCollector {
	licenseLabels := []string{"license", "remote"}
	return &LicensesCollector{
		client: client,
		logger: log.With(logger, "collector", "licenses"),
		total: prometheus.NewDesc(prometheus.BuildFQName(namespace, licenseNamespace, "total"),
			"Total licenses", licenseLabels, nil),
		used: prometheus.NewDesc(prometheus.BuildFQName(namespace, licenseNamespace, "used"),
			"Used licenses", licenseLabels, nil),
		free: prometheus.NewDesc(prometheus.BuildFQName(namespace, licenseNamespace, "free"),
			"Free licenses", licenseLabels, nil),
		reserved: prometheus.NewDesc(prometheus.BuildFQName(namespace, licenseNamespace, "reserved"),
			"Reserved licenses", licenseLabels, nil),
	}
}

func (lc *LicensesCollector) metrics() ([]licenseMetrics, error) {
	var lms []licenseMetrics

	licenses, resp, err := lc.client.SlurmAPI.SlurmV0040GetLicenses(context.Background()).Execute()
	if err != nil {
		level.Error(lc.logger).Log("msg", "Failed to fetch licenses from slurm rest api", "err", err)
		return lms, err
	} else if resp.StatusCode != 200 {
		err = fmt.Errorf("HTTP response not OK while fetching licenses from slurm rest api")
		level.Error(lc.logger).Log("err", err, "status_code", resp.StatusCode)
		return lms, err
	} else if len(licenses.GetErrors()) > 0 {
		for _, err := range licenses.GetErrors() {
			level.Error(lc.logger).Log("err", err.GetError())
		}
		return lms, fmt.Errorf("HTTP response contained %d errors", len(licenses.GetErrors()))
	}

	for _, l := range licenses.GetLicenses() {
		lm := licenseMetrics{
			name:     l.GetLicenseName(),
			total:    float64(l.GetTotal()),
			used:     float64(l.GetUsed()),
			reserved: float64(l.GetReserved()),
			remote:   l.GetRemote(),
		}
		lms = append(lms, lm)
	}

	return lms, err
}

func (lc *LicensesCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- lc.total
	ch <- lc.used
	ch <- lc.free
	ch <- lc.reserved
}

func (lc *LicensesCollector) Collect(ch chan<- prometheus.Metric) {
	var errValue float64
	lm, err := lc.metrics()
	if err != nil {
		errValue = 1
	}
	for _, l := range lm {
		ch <- prometheus.MustNewConstMetric(lc.total, prometheus.GaugeValue, l.total, l.name, strconv.FormatBool(l.remote))
		ch <- prometheus.MustNewConstMetric(lc.used, prometheus.GaugeValue, l.used, l.name, strconv.FormatBool(l.remote))
		ch <- prometheus.MustNewConstMetric(lc.free, prometheus.GaugeValue, l.free, l.name, strconv.FormatBool(l.remote))
		ch <- prometheus.MustNewConstMetric(lc.reserved, prometheus.GaugeValue, l.reserved, l.name, strconv.FormatBool(l.remote))
	}
	ch <- prometheus.MustNewConstMetric(collectError, prometheus.GaugeValue, errValue, "licenses")
}
