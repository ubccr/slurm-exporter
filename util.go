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
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/dustin/go-humanize"
	"github.com/prometheus/client_golang/prometheus"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	ignorePartitions = kingpin.Flag("collector.partition.ignore",
		"Regexp of partitions to ignore").Default("^$").String()
	// Regexp to parse GPU Gres and GresUsed strings. Example looks like this:
	//   gpu:tesla_v100-pcie-16gb:2(S:0-1)
	gpuGresPattern = regexp.MustCompile(`^gpu\:([^\:]+)\:?(\d+)?`)
	collectError   = prometheus.NewDesc("slurm_exporter_collect_error",
		"Indicates if an error has occurred during collection", []string{"collector"}, nil)
	allocPattern   = regexp.MustCompile(`(?i)^ALLOC`)
	compPattern    = regexp.MustCompile(`(?i)^COMP`)
	downPattern    = regexp.MustCompile(`(?i)^DOWN`)
	drainPattern   = regexp.MustCompile(`(?i)^DRAIN`)
	failPattern    = regexp.MustCompile(`(?i)^FAIL`)
	errPattern     = regexp.MustCompile(`(?i)^ERR`)
	idlePattern    = regexp.MustCompile(`(?i)^IDLE`)
	invalPattern   = regexp.MustCompile(`(?i)^INVAL`)
	maintPattern   = regexp.MustCompile(`(?i)^MAINT`)
	mixPattern     = regexp.MustCompile(`(?i)^MIX`)
	plannedPattern = regexp.MustCompile(`(?i)^PLANNED`)
	rebootPattern  = regexp.MustCompile(`(?i)^REBOOT`)
	resvPattern    = regexp.MustCompile(`(?i)^RES`)
	unknownPattern = regexp.MustCompile(`(?i)^UNKNOWN`)
)

type Tres struct {
	Memory  uint64
	CPU     int
	Node    int
	Billing int
	GresGpu int
}

type Token struct {
	sync.RWMutex
	token   string
	created int64
}

type rpcStat struct {
	count     float64
	aveTime   float64
	totalTime float64
}

// gpuCountFromGres parses Slurm GRES (generic resource) line and returns the
// count of GPUs
func gpuCountFromGres(line string) int {
	value := 0

	tres := strings.Split(line, ",")
	for _, g := range tres {
		if !strings.HasPrefix(g, "gpu:") {
			continue
		}

		matches := gpuGresPattern.FindStringSubmatch(g)
		if len(matches) == 3 {
			if matches[2] != "" {
				value, _ = strconv.Atoi(matches[2])
			} else {
				value, _ = strconv.Atoi(matches[1])
			}
		}
	}

	return value
}

// parseTres parses Slurm TRES (Trackable RESources) line. Example looks like:
// cpu=32,mem=187000M,billing=32,gres/gpu=2
func parseTres(line string) *Tres {
	var tres Tres

	items := strings.Split(line, ",")
	for _, item := range items {
		kv := strings.Split(item, "=")
		switch kv[0] {
		case "cpu":
			tres.CPU, _ = strconv.Atoi(kv[1])
		case "node":
			tres.Node, _ = strconv.Atoi(kv[1])
		case "billing":
			tres.Billing, _ = strconv.Atoi(kv[1])
		case "gres/gpu":
			tres.GresGpu, _ = strconv.Atoi(kv[1])
		case "mem":
			tres.Memory, _ = humanize.ParseBytes(kv[1])

		}
	}

	return &tres
}

func sliceContains(slice []string, str string) bool {
	for _, s := range slice {
		if str == s {
			return true
		}
	}
	return false
}
