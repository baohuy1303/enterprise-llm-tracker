// Command loadgen emits real OTLP metrics over HTTP, exactly like the
// Claude Code → otelcol → sentinel-api production path. It builds an
// ExportMetricsServiceRequest protobuf (using the same otlp proto libs the
// ingest handler decodes with — zero extra dependencies) and POSTs it.
//
// Default target is the otelcol HTTP receiver (:4318/v1/metrics), so traffic
// flows through the real collector hop (filter + batch) before reaching the
// ingest handler. Point --target at http://localhost:8081/ingest/otel/v1/metrics
// to bypass the collector and hit sentinel-api directly (faster, deterministic).
//
// Examples:
//
//	# one $5 cost event for the user (crosses a $2 daily budget → threshold DM)
//	go run ./cmd/loadgen --email huynhbaohuy1303@gmail.com --kind cost --value 5
//
//	# a burst: 5 pricey cost events "now" for a seeded engineer (→ burst-critical DM)
//	go run ./cmd/loadgen --email test-alice@sentinel.local --kind cost --value 0.8 --count 5
//
//	# a cold-hour event (03:00 UTC today) → rhythm break
//	go run ./cmd/loadgen --email test-alice@sentinel.local --kind cost --value 0.2 --at-hour 3
//
//	# a PR-count event → wakes the github-trigger consumer / collector
//	go run ./cmd/loadgen --email huynhbaohuy1303@gmail.com --kind pr
package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"google.golang.org/protobuf/proto"

	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
)

// metric name + default data-point attributes per --kind.
var kinds = map[string]string{
	"cost":    "claude_code.cost.usage",
	"token":   "claude_code.token.usage",
	"lines":   "claude_code.lines_of_code.count",
	"pr":      "claude_code.pull_request.count",
	"commit":  "claude_code.commit.count",
	"session": "claude_code.session.count",
}

func main() {
	var (
		target   = flag.String("target", "http://localhost:4318/v1/metrics", "OTLP HTTP metrics endpoint (collector :4318/v1/metrics, or sentinel-api :8081/ingest/otel/v1/metrics)")
		email    = flag.String("email", "", "engineer email (user.email attribute) — required")
		kind     = flag.String("kind", "cost", "cost|token|lines|pr|commit|session")
		value    = flag.Float64("value", 0.10, "data-point value (USD for cost, token count for token, ignored for counts)")
		model    = flag.String("model", "claude-opus-4-7", "model attribute (cost/token only)")
		ttype    = flag.String("ttype", "input", "token type: input|output|cacheRead|cacheCreation (kind=token only)")
		count    = flag.Int("count", 1, "number of data points to emit in the request")
		atHour   = flag.Int("at-hour", -1, "if >=0, stamp events at this UTC hour today (for rhythm-break tests); else now")
	)
	flag.Parse()

	if *email == "" {
		log.Fatal("--email is required")
	}
	name, ok := kinds[*kind]
	if !ok {
		log.Fatalf("unknown --kind %q", *kind)
	}

	ts := time.Now().UTC()
	if *atHour >= 0 {
		y, m, d := ts.Date()
		ts = time.Date(y, m, d, *atHour, ts.Minute(), ts.Second(), 0, time.UTC)
	}

	dps := make([]*metricspb.NumberDataPoint, 0, *count)
	for i := 0; i < *count; i++ {
		attrs := []*commonpb.KeyValue{kv("user.email", *email)}
		if *kind == "cost" || *kind == "token" {
			attrs = append(attrs, kv("model", *model))
		}
		if *kind == "token" {
			attrs = append(attrs, kv("type", *ttype))
		}
		dps = append(dps, &metricspb.NumberDataPoint{
			Attributes:   attrs,
			TimeUnixNano: uint64(ts.Add(time.Duration(i) * time.Second).UnixNano()),
			Value:        &metricspb.NumberDataPoint_AsDouble{AsDouble: *value},
		})
	}

	req := &colmetricspb.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{{
			Resource: &resourcepb.Resource{
				Attributes: []*commonpb.KeyValue{kv("user.email", *email)},
			},
			ScopeMetrics: []*metricspb.ScopeMetrics{{
				Metrics: []*metricspb.Metric{{
					Name: name,
					Data: &metricspb.Metric_Sum{Sum: &metricspb.Sum{
						AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
						IsMonotonic:            true,
						DataPoints:             dps,
					}},
				}},
			}},
		}},
	}

	body, err := proto.Marshal(req)
	if err != nil {
		log.Fatalf("marshal: %v", err)
	}

	httpReq, err := http.NewRequest(http.MethodPost, *target, bytes.NewReader(body))
	if err != nil {
		log.Fatalf("new request: %v", err)
	}
	httpReq.Header.Set("Content-Type", "application/x-protobuf")

	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(httpReq)
	if err != nil {
		log.Fatalf("POST %s: %v", *target, err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)

	fmt.Printf("→ %s  %s x%d  value=%g email=%s  @%s\n",
		*target, name, *count, *value, *email, ts.Format(time.RFC3339))
	fmt.Printf("← %s (%d bytes)\n", resp.Status, len(respBody))
	if resp.StatusCode/100 != 2 {
		log.Fatalf("non-2xx response: %s", resp.Status)
	}
}

func kv(k, v string) *commonpb.KeyValue {
	return &commonpb.KeyValue{
		Key:   k,
		Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}},
	}
}
