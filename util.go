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
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/dustin/go-humanize"
	"github.com/prometheus/client_golang/prometheus"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	// Regexp to parse GPU Gres and GresUsed strings. Example looks like this:
	//   gpu:tesla_v100-pcie-16gb:2(S:0-1)
	gpuGresPattern = regexp.MustCompile(`^gpu\:([^\:]+)\:?(\d+)?`)
	collectError   = prometheus.NewDesc("slurm_exporter_collect_error",
		"Indicates if an error has occurred during collection", []string{"collector"}, nil)
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

type FloatBucketValues []float64

func (i *FloatBucketValues) Set(value string) error {
	buckets := []float64{}
	bucketFloats := strings.Split(value, ",")
	for _, bucketFloat := range bucketFloats {
		float, err := strconv.ParseFloat(bucketFloat, 64)
		if err != nil {
			return fmt.Errorf("'%s' is not a valid bucket float64", value)
		}
		buckets = append(buckets, float)
	}
	sort.Float64s(buckets)
	*i = buckets
	return nil
}

func (d *FloatBucketValues) String() string {
	return ""
}

func FloatBuckets(s kingpin.Settings) (target *[]float64) {
	target = &[]float64{}
	s.SetValue((*FloatBucketValues)(target))
	return
}
