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
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os/exec"
	"os/user"
	"regexp"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
	"github.com/ubccr/slurmrest"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	listenAddress = kingpin.Flag(
		"listen-address",
		"Address on which to expose metrics and web interface.",
	).Default(":9122").String()

	unixSocket = kingpin.Flag(
		"unix-socket",
		"Path to slurmrestd unix socket.",
	).Default("/tmp/slurmrestd.sock").String()

	restURL = kingpin.Flag(
		"rest-url",
		"SLURM REST API URL, eg http://localhost:6820",
	).Default("").String()

	timeout      = kingpin.Flag("rest-timeout", "Timeout of REST HTTP queries").Default("60").Int()
	tokenTimeout = kingpin.Flag("token-timeout", "Timeout of getting REST token").Default("5").Int()
	debug        = kingpin.Flag("debug", "enable debug mode").Default("false").Bool()
)

func main() {
	kingpin.HelpFlag.Short('h')
	kingpin.Parse()

	if *debug {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}

	cfg := slurmrest.NewConfiguration()
	cfg.HTTPClient = &http.Client{Timeout: time.Duration(*timeout) * time.Second}
	if *restURL != "" {
		url, err := url.Parse(*restURL)
		if err != nil {
			log.Fatal(err)
		}
		cfg.Scheme = url.Scheme
		cfg.Host = url.Host
		user, err := user.Current()
		if err != nil {
			log.Fatal(err)
		}
		headers := make(map[string]string)
		headers["X-SLURM-USER-NAME"] = user.Username
		headers["X-SLURM-USER-TOKEN"] = ""
		cfg.DefaultHeader = headers
	} else {
		tr := &http.Transport{
			DialContext: func(ctx context.Context, _, addr string) (net.Conn, error) {
				dialer := net.Dialer{}
				return dialer.DialContext(ctx, "unix", *unixSocket)
			},
		}
		cfg.Scheme = "http"
		cfg.Host = "localhost"
		cfg.HTTPClient.Transport = tr
	}

	log.Infof("Starting Server: %s", *listenAddress)
	http.Handle("/metrics", metricsHandler(cfg))
	log.Fatal(http.ListenAndServe(*listenAddress, nil))
}

func metricsHandler(cfg *slurmrest.Configuration) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if *restURL != "" {
			token, err := getToken()
			if err != nil {
				http.Error(w, fmt.Sprintf("Unable to get JWT: %s", err), http.StatusNotFound)
				return
			}
			cfg.DefaultHeader["X-SLURM-USER-TOKEN"] = token
		}
		client := slurmrest.NewAPIClient(cfg)

		registry := prometheus.NewRegistry()
		registry.MustRegister(NewNodesCollector(client))
		registry.MustRegister(NewSchedulerCollector(client))
		registry.MustRegister(NewJobsCollector(client))
		gatherers := prometheus.Gatherers{registry, prometheus.DefaultGatherer}
		h := promhttp.HandlerFor(gatherers, promhttp.HandlerOpts{})
		h.ServeHTTP(w, r)
	}
}

func getToken() (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*tokenTimeout)*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "scontrol", "token")
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		return "", err
	}
	re := regexp.MustCompile(`^SLURM_JWT=(.*)`)
	match := re.FindStringSubmatch(strings.TrimSpace(stdout.String()))
	var token string
	if len(match) == 2 {
		token = match[1]
	} else {
		return "", fmt.Errorf("Unable to match SLURM_JWT from output, output=%s", stdout.String())
	}
	return token, nil
}
