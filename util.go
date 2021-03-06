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

	"github.com/dustin/go-humanize"
)

var (
	// Regexp to parse GPU Gres and GresUsed strings. Example looks like this:
	//   gpu:tesla_v100-pcie-16gb:2(S:0-1)
	gpuGresPattern = regexp.MustCompile(`^gpu\:([^\:]+)\:?(\d+)?`)
)

type Tres struct {
	Memory  uint64
	CPU     int
	Node    int
	Billing int
	GresGpu int
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
