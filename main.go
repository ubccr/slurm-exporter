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
	"os"
	"os/exec"
	"os/user"
	"regexp"
	"strings"
	"time"

	kingpin "github.com/alecthomas/kingpin/v2"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/promlog"
	"github.com/prometheus/common/promlog/flag"
	"github.com/prometheus/common/version"
	"github.com/ubccr/slurmrest"
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
	token        Token
)

const (
	namespace     = "slurm"
	tokenLifespan = 86400
)

func main() {
	promlogConfig := &promlog.Config{}
	flag.AddFlags(kingpin.CommandLine, promlogConfig)
	kingpin.Version(version.Print("slurm-exporter"))
	kingpin.HelpFlag.Short('h')
	kingpin.Parse()

	logger := promlog.New(promlogConfig)

	cfg := slurmrest.NewConfiguration()
	cfg.HTTPClient = &http.Client{Timeout: time.Duration(*timeout) * time.Second}
	if *restURL != "" {
		url, err := url.Parse(*restURL)
		if err != nil {
			level.Error(logger).Log("err", err)
			os.Exit(1)
		}
		cfg.Scheme = url.Scheme
		cfg.Host = url.Host
		user, err := user.Current()
		if err != nil {
			level.Error(logger).Log("err", err)
			os.Exit(1)
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

	level.Info(logger).Log("msg", "Starting slurm-exporter", "version", version.Info())
	level.Info(logger).Log("msg", "Build context", "build_context", version.BuildContext())
	level.Info(logger).Log("msg", "Starting Server", "address", *listenAddress)
	http.Handle("/metrics", metricsHandler(cfg, logger))
	err := http.ListenAndServe(*listenAddress, nil)
	if err != nil {
		level.Error(logger).Log("err", err)
		os.Exit(1)
	}
}

func metricsHandler(cfg *slurmrest.Configuration, logger log.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if *restURL != "" {
			if (token.created + int64(tokenLifespan)) <= time.Now().Unix() {
				level.Info(logger).Log("msg", "Getting new JWT")
				err := getToken()
				if err != nil {
					level.Error(logger).Log("msg", "Unable to get JWT", "err", err)
					http.Error(w, fmt.Sprintf("Unable to get JWT: %s", err), http.StatusNotFound)
					return
				}
			}
			token.RLock()
			defer token.RUnlock()
			cfg.DefaultHeader["X-SLURM-USER-TOKEN"] = token.token
		}
		client := slurmrest.NewAPIClient(cfg)

		registry := prometheus.NewRegistry()
		registry.MustRegister(NewNodesCollector(client, logger))
		registry.MustRegister(NewPartitionNodesCollector(client, logger))
		registry.MustRegister(NewSchedulerCollector(client, logger))
		registry.MustRegister(NewJobsCollector(client, logger))
		registry.MustRegister(NewPartitionJobsCollector(client, logger))
		registry.MustRegister(NewLicensesCollector(client, logger))
		gatherers := prometheus.Gatherers{registry, prometheus.DefaultGatherer}
		h := promhttp.HandlerFor(gatherers, promhttp.HandlerOpts{})
		h.ServeHTTP(w, r)
	}
}

func getToken() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*tokenTimeout)*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "scontrol", "token", fmt.Sprintf("lifespan=%d", tokenLifespan))
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		return err
	}
	re := regexp.MustCompile(`^SLURM_JWT=(.*)`)
	match := re.FindStringSubmatch(strings.TrimSpace(stdout.String()))
	if len(match) == 2 {
		token.Lock()
		defer token.Unlock()
		token.token = match[1]
		token.created = time.Now().Unix()
	} else {
		return fmt.Errorf("Unable to match SLURM_JWT from output, output=%s", stdout.String())
	}
	return nil
}
