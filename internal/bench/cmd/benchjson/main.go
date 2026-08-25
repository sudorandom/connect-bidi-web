// Copyright 2021-2026 The Connect Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Command benchjson turns `go test -bench` output from internal/bench into
// the JSON the demo site renders. It reads the benchmark output on stdin
// and writes JSON on stdout:
//
//	go test -bench . -benchtime 200x ./internal/bench | go run ./internal/bench/cmd/benchjson > demo/web/src/bench-go.json
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// row is one benchmark result: a transport case and workload with its
// measurements. Metric fields are omitted when the benchmark didn't report
// them.
type row struct {
	Case string `json:"case"`
	// Bootstrap groups rows that are measured comparably. Byte counts are
	// plaintext TCP on HTTP/1.1, but include TLS on HTTP/2 and QUIC + TLS
	// on HTTP/3, so a reader (and the demo's best-value highlighting) must
	// not rank rows across groups.
	Bootstrap      string  `json:"bootstrap"`
	Workload       string  `json:"workload"`
	NsPerOp        float64 `json:"nsPerOp"`
	NsPerRoundtrip float64 `json:"nsPerRoundtrip,omitempty"`
	RxBytesPerOp   float64 `json:"rxBytesPerOp"`
	TxBytesPerOp   float64 `json:"txBytesPerOp"`
}

type output struct {
	GeneratedAt string `json:"generatedAt"`
	CPU         string `json:"cpu"`
	Rows        []row  `json:"rows"`
}

// benchLine matches e.g.:
//
//	BenchmarkTransports/ws-draft1/identity/unary_small-18  200  99773 ns/op  186.0 rxB/op  143.0 txB/op
var benchLine = regexp.MustCompile(`^BenchmarkTransports/(.+)-\d+\s+\d+\s+(.*)$`)

// bootstrapFor classifies a case by how its connection was established,
// which is what decides whether two rows can be compared.
func bootstrapFor(benchCase string) string {
	if strings.HasPrefix(benchCase, "webtransport") {
		return "HTTP/3"
	}
	// Cases are "<transport>/<variant>"; the HTTP/2 variants are the ones
	// dialed with NewH2Transport, named h2-*.
	if _, variant, ok := strings.Cut(benchCase, "/"); ok && strings.HasPrefix(variant, "h2") {
		return "HTTP/2"
	}
	return "HTTP/1.1"
}

func main() {
	out := output{GeneratedAt: time.Now().UTC().Format("2006-01-02")}
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		if cpu, ok := strings.CutPrefix(line, "cpu: "); ok {
			out.CPU = strings.TrimSpace(cpu)
			continue
		}
		match := benchLine.FindStringSubmatch(line)
		if match == nil {
			continue
		}
		// The benchmark name is <case>/<workload> where the case itself
		// contains one slash (e.g. ws-draft1/identity/unary_small).
		name := match[1]
		lastSlash := strings.LastIndex(name, "/")
		if lastSlash < 0 {
			continue
		}
		benchCase := name[:lastSlash]
		parsed := row{
			Case:      benchCase,
			Bootstrap: bootstrapFor(benchCase),
			Workload:  name[lastSlash+1:],
		}
		metrics := strings.Fields(match[2])
		for i := 0; i+1 < len(metrics); i += 2 {
			value, err := strconv.ParseFloat(metrics[i], 64)
			if err != nil {
				continue
			}
			switch metrics[i+1] {
			case "ns/op":
				parsed.NsPerOp = value
			case "ns/roundtrip":
				parsed.NsPerRoundtrip = value
			case "rxB/op":
				parsed.RxBytesPerOp = value
			case "txB/op":
				parsed.TxBytesPerOp = value
			}
		}
		out.Rows = append(out.Rows, parsed)
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "benchjson: read stdin: %v\n", err)
		os.Exit(1)
	}
	if len(out.Rows) == 0 {
		fmt.Fprintln(os.Stderr, "benchjson: no benchmark lines found on stdin")
		os.Exit(1)
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(out); err != nil {
		fmt.Fprintf(os.Stderr, "benchjson: encode: %v\n", err)
		os.Exit(1)
	}
}
