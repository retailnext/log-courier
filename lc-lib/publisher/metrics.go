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

package publisher

import "github.com/prometheus/client_golang/prometheus"

var (
	linesPublishedDesc = prometheus.NewDesc(
		"log_courier_lines_published_total",
		"Number of log lines published to Logstash.",
		nil,
		nil,
	)

	payloadsPendingDesc = prometheus.NewDesc(
		"log_courier_payloads_pending",
		"Number of payloads pending publishing to Logstash.",
		nil,
		nil,
	)
)

func (p *Publisher) Describe(ch chan<- *prometheus.Desc) {
	ch <- linesPublishedDesc
	ch <- payloadsPendingDesc
}

func (p *Publisher) Collect(ch chan<- prometheus.Metric) {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	ch <- prometheus.MustNewConstMetric(linesPublishedDesc, prometheus.CounterValue, float64(p.lineCount))
	ch <- prometheus.MustNewConstMetric(payloadsPendingDesc, prometheus.GaugeValue, float64(p.numPayloads))
}
