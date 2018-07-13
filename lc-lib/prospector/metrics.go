// Copyright 2018 RetailNext, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package prospector

import "github.com/prometheus/client_golang/prometheus"

var (
	filesOpenDesc = prometheus.NewDesc(
		"log_courier_files_open",
		"Number of log files open for reading.",
		nil,
		nil,
	)
)

func (p *Prospector) Describe(ch chan<- *prometheus.Desc) {
	ch <- filesOpenDesc
}

func (p *Prospector) Collect(ch chan<- prometheus.Metric) {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	var filesOpen int
	for _, info := range p.prospectorindex {
		if info.harvester != nil {
			filesOpen++
		}
	}
	ch <- prometheus.MustNewConstMetric(filesOpenDesc, prometheus.GaugeValue, float64(filesOpen))
}
