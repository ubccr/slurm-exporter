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
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
	"github.com/ubccr/slurmrest"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

func main() {
	var (
		listenAddress = kingpin.Flag(
			"listen-address",
			"Address on which to expose metrics and web interface.",
		).Default(":9122").String()

		unixSocket = kingpin.Flag(
			"unix-socket",
			"Path to slurmrestd unix socket.",
		).Default("/tmp/slurmrestd.sock").String()

		debug = kingpin.Flag("debug", "enable debug mode").Default("false").Bool()
	)

	kingpin.HelpFlag.Short('h')
	kingpin.Parse()

	if *debug {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.WarnLevel)
	}

	tr := &http.Transport{
		DialContext: func(ctx context.Context, _, addr string) (net.Conn, error) {
			dialer := net.Dialer{}
			return dialer.DialContext(ctx, "unix", *unixSocket)
		},
	}
	cfg := slurmrest.NewConfiguration()
	cfg.HTTPClient = &http.Client{Timeout: time.Second * 3600, Transport: tr}
	cfg.Scheme = "http"
	cfg.Host = "localhost"

	client := slurmrest.NewAPIClient(cfg)
	prometheus.MustRegister(NewNodesCollector(client))
	prometheus.MustRegister(NewSchedulerCollector(client))
	prometheus.MustRegister(NewJobsCollector(client))

	log.Infof("Starting Server: %s", *listenAddress)
	http.Handle("/metrics", promhttp.Handler())
	log.Fatal(http.ListenAndServe(*listenAddress, nil))
}
