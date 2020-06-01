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
)

var (
	// Regexp to parse GPU Gres and GresUsed strings. Example looks like this:
	//   gpu:tesla_v100-pcie-16gb:2(S:0-1)
	gpuGresPattern = regexp.MustCompile(`^gpu\:([^\:]+)\:?(\d+)?`)

	// Regexp to parse Tres and TresUsed strings. Example looks like this:
	//   cpu=32,mem=187000M,billing=32,gres/gpu=2
	gpuTresPattern = regexp.MustCompile(`^gres\/gpu\=(\d+)`)
)

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

// gpuCountFromTres parses Slurm TRES (Trackable RESources) line and returns
// the count of GPUs
func gpuCountFromTres(line string) int {
	value := 0

	tres := strings.Split(line, ",")
	for _, g := range tres {
		if !strings.HasPrefix(g, "gres") {
			continue
		}

		matches := gpuTresPattern.FindStringSubmatch(g)
		if len(matches) == 2 {
			value, _ = strconv.Atoi(matches[1])
		}
	}

	return value
}
