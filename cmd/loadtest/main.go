// Command loadtest is a concurrent load generator for the Sentinel pipeline.
//
// It drives two paths:
//
//	--mode ingest  POSTs real OTLP protobuf metrics to the ingest hot path
//	               (sentinel-api :8081/ingest/otel/v1/metrics, or the otelcol
//	               :4318/v1/metrics receiver). This is the path every Claude
//	               Code session hits — Redis INCR + async Kafka produce.
//	--mode admin   GETs the admin/dashboard read endpoints (leaderboard,
//	               engineers, signals) with a bearer token. This is what the
//	               Next.js dashboard polls.
//
// Two load shapes:
//
//	closed-loop (default, --rate 0): --workers goroutines each fire requests
//	  back-to-back. Ramp --workers to find the saturation point / max RPS.
//	open-loop (--rate N>0): a pacer emits jobs at N req/sec regardless of
//	  latency; workers drain them. Latency is measured from each job's intended
//	  send time (so queueing delay is captured, avoiding coordinated omission).
//	  Use this for fixed-rate soak tests. --overruns in the summary counts jobs
//	  the workers couldn't start on time — a saturation signal.
//
// Output: a human summary plus, with --out, a JSON result file for charting.
//
// IMPORTANT: ingest traffic is only recorded if the engineer email is in the
// registry (loaded from the engineers table). Seed first with
// scripts/seed_load_engineers.sql (default 200 engineers) and keep
// --engineers <= the seeded count, or events are silently dropped as
// unattributed (the handler still returns 200, so the client can't see it —
// verify recorded volume via Redis/Postgres counts).
//
// Examples:
//
//	# 500 concurrent workers, 30s, direct to sentinel-api, 10 data points/req
//	go run ./cmd/loadtest --mode ingest --workers 500 --duration 30s --batch 10 \
//	    --target http://localhost:8081/ingest/otel/v1/metrics --out r.json
//
//	# fixed 200 req/sec soak for 1h (open-loop)
//	go run ./cmd/loadtest --mode ingest --rate 200 --duration 1h --workers 64
//
//	# admin read load: 200 workers hitting leaderboard/engineers/signals
//	go run ./cmd/loadtest --mode admin --workers 200 --duration 30s \
//	    --target http://localhost:8081 --token "$SENTINEL_ADMIN_TOKEN"
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"math/bits"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"

	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
)

var metricNames = map[string]string{
	"cost":    "claude_code.cost.usage",
	"token":   "claude_code.token.usage",
	"lines":   "claude_code.lines_of_code.count",
	"pr":      "claude_code.pull_request.count",
	"commit":  "claude_code.commit.count",
	"session": "claude_code.session.count",
}

func main() {
	var (
		mode     = flag.String("mode", "ingest", "ingest|admin")
		target   = flag.String("target", "", "ingest: OTLP metrics URL (default http://localhost:8081/ingest/otel/v1/metrics); admin: API base URL (default http://localhost:8081)")
		workers  = flag.Int("workers", 100, "concurrent workers")
		duration = flag.Duration("duration", 30*time.Second, "total run time")
		rate     = flag.Int("rate", 0, "open-loop target req/sec (0 = closed-loop, workers fire flat-out)")
		timeout  = flag.Duration("timeout", 10*time.Second, "per-request timeout")
		out      = flag.String("out", "", "write JSON result summary to this path")
		progress = flag.Bool("progress", true, "print a live progress line every second")

		// ingest
		engineers   = flag.Int("engineers", 200, "spread load across loadtest-0001..NNNN@<domain> (must be <= seeded count)")
		emailPrefix = flag.String("email-prefix", "loadtest-", "synthetic engineer email prefix")
		emailDomain = flag.String("email-domain", "@sentinel.local", "synthetic engineer email domain")
		kind        = flag.String("kind", "cost", "ingest metric kind: cost|token|lines|pr|commit|session")
		value       = flag.Float64("value", 0.01, "ingest data-point value (small so budgets aren't tripped)")
		model       = flag.String("model", "claude-opus-4-7", "ingest model attribute")
		batch       = flag.Int("batch", 1, "ingest data points per request (events/sec = rps * batch)")

		// admin
		token      = flag.String("token", os.Getenv("SENTINEL_ADMIN_TOKEN"), "admin bearer token (default $SENTINEL_ADMIN_TOKEN)")
		adminPaths = flag.String("admin-paths", "/admin/leaderboard,/admin/engineers,/admin/signals/efficiency", "comma-separated admin paths to rotate through")
	)
	flag.Parse()

	if *workers < 1 {
		log.Fatal("--workers must be >= 1")
	}
	if *batch < 1 {
		log.Fatal("--batch must be >= 1")
	}

	specs, tgt, err := buildSpecs(*mode, *target, *token, *adminPaths,
		*engineers, *emailPrefix, *emailDomain, *kind, *value, *model, *batch)
	if err != nil {
		log.Fatal(err)
	}

	// Tuned transport: reuse keep-alive connections so we don't exhaust ephemeral
	// ports at high concurrency, and so we measure server cost, not TCP handshakes.
	tr := &http.Transport{
		MaxIdleConns:        *workers * 2,
		MaxIdleConnsPerHost: *workers * 2,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true,
		ForceAttemptHTTP2:   false,
	}
	client := &http.Client{Transport: tr, Timeout: *timeout}

	// ctx ends at --duration or on Ctrl+C. Requests themselves run on a
	// background context (client.Timeout bounds them) so a clean shutdown lets
	// in-flight requests finish instead of counting them as canceled errors.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, *duration)
	defer cancel()

	fmt.Printf("loadtest mode=%s target=%s workers=%d rate=%d duration=%s batch=%d specs=%d\n",
		*mode, tgt, *workers, *rate, *duration, *batch, len(specs))

	stats := make([]*workerStat, *workers)
	for i := range stats {
		stats[i] = newWorkerStat()
	}

	var overruns uint64 // open-loop: jobs the pacer couldn't hand off in time

	if *progress {
		go progressLoop(ctx, stats)
	}

	start := time.Now()
	var wg sync.WaitGroup

	if *rate <= 0 {
		// Closed-loop: each worker loops until ctx is done.
		for i := 0; i < *workers; i++ {
			wg.Add(1)
			go func(st *workerStat, idx int) {
				defer wg.Done()
				rr := uint64(idx) // per-worker round-robin seed spreads specs
				for ctx.Err() == nil {
					spec := specs[rr%uint64(len(specs))]
					rr++
					sent := time.Now()
					code, err := doRequest(client, spec)
					st.record(time.Since(sent), code, err)
				}
			}(stats[i], i)
		}
	} else {
		// Open-loop: a pacer emits intended-send timestamps at --rate; workers
		// drain them and measure latency from the intended time.
		jobs := make(chan time.Time, *rate) // ~1s of buffer
		go pacer(ctx, *rate, jobs, &overruns)
		var specIdx uint64
		for i := 0; i < *workers; i++ {
			wg.Add(1)
			go func(st *workerStat) {
				defer wg.Done()
				for intended := range jobs {
					spec := specs[atomic.AddUint64(&specIdx, 1)%uint64(len(specs))]
					code, err := doRequest(client, spec)
					st.record(time.Since(intended), code, err)
				}
			}(stats[i])
		}
	}

	wg.Wait()
	elapsed := time.Since(start)

	report(*mode, tgt, *workers, *rate, *batch, elapsed, atomic.LoadUint64(&overruns), stats, *out)
}

// ── request specs ────────────────────────────────────────────────────────────

type reqSpec struct {
	method      string
	url         string
	body        []byte
	contentType string
	authBearer  string
}

func buildSpecs(mode, target, token, adminPaths string,
	engineers int, emailPrefix, emailDomain, kind string, value float64, model string, batch int,
) ([]reqSpec, string, error) {
	switch mode {
	case "ingest":
		if target == "" {
			target = "http://localhost:8081/ingest/otel/v1/metrics"
		}
		name, ok := metricNames[kind]
		if !ok {
			return nil, target, fmt.Errorf("unknown --kind %q", kind)
		}
		if engineers < 1 {
			return nil, target, fmt.Errorf("--engineers must be >= 1")
		}
		specs := make([]reqSpec, engineers)
		for i := 0; i < engineers; i++ {
			email := fmt.Sprintf("%s%04d%s", emailPrefix, i+1, emailDomain)
			body, err := buildOTLPBody(email, name, kind, value, model, batch)
			if err != nil {
				return nil, target, err
			}
			specs[i] = reqSpec{
				method:      http.MethodPost,
				url:         target,
				body:        body,
				contentType: "application/x-protobuf",
			}
		}
		return specs, target, nil

	case "admin":
		if target == "" {
			target = "http://localhost:8081"
		}
		if token == "" {
			return nil, target, fmt.Errorf("--mode admin requires --token (or $SENTINEL_ADMIN_TOKEN)")
		}
		base := strings.TrimRight(target, "/")
		var specs []reqSpec
		for _, p := range strings.Split(adminPaths, ",") {
			p = strings.TrimSpace(p)
			if p == "" {
				continue
			}
			if !strings.HasPrefix(p, "/") {
				p = "/" + p
			}
			specs = append(specs, reqSpec{
				method:     http.MethodGet,
				url:        base + p,
				authBearer: token,
			})
		}
		if len(specs) == 0 {
			return nil, target, fmt.Errorf("--admin-paths produced no paths")
		}
		return specs, target, nil

	default:
		return nil, target, fmt.Errorf("unknown --mode %q (want ingest|admin)", mode)
	}
}

func buildOTLPBody(email, metricName, kind string, value float64, model string, batch int) ([]byte, error) {
	ts := time.Now().UTC()
	dps := make([]*metricspb.NumberDataPoint, 0, batch)
	for i := 0; i < batch; i++ {
		attrs := []*commonpb.KeyValue{kv("user.email", email)}
		if kind == "cost" || kind == "token" {
			attrs = append(attrs, kv("model", model))
		}
		if kind == "token" {
			attrs = append(attrs, kv("type", "input"))
		}
		dps = append(dps, &metricspb.NumberDataPoint{
			Attributes:   attrs,
			TimeUnixNano: uint64(ts.UnixNano()),
			Value:        &metricspb.NumberDataPoint_AsDouble{AsDouble: value},
		})
	}
	req := &colmetricspb.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{{
			Resource: &resourcepb.Resource{
				Attributes: []*commonpb.KeyValue{kv("user.email", email)},
			},
			ScopeMetrics: []*metricspb.ScopeMetrics{{
				Metrics: []*metricspb.Metric{{
					Name: metricName,
					Data: &metricspb.Metric_Sum{Sum: &metricspb.Sum{
						AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
						IsMonotonic:            true,
						DataPoints:             dps,
					}},
				}},
			}},
		}},
	}
	return proto.Marshal(req)
}

func kv(k, v string) *commonpb.KeyValue {
	return &commonpb.KeyValue{
		Key:   k,
		Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}},
	}
}

// doRequest runs one request on a background context (so a run-level shutdown
// doesn't cancel it mid-flight) and returns the status code, draining the body
// so the keep-alive connection can be reused.
func doRequest(client *http.Client, spec reqSpec) (int, error) {
	var body io.Reader
	if spec.body != nil {
		body = bytes.NewReader(spec.body)
	}
	req, err := http.NewRequest(spec.method, spec.url, body)
	if err != nil {
		return 0, err
	}
	if spec.contentType != "" {
		req.Header.Set("Content-Type", spec.contentType)
	}
	if spec.authBearer != "" {
		req.Header.Set("Authorization", "Bearer "+spec.authBearer)
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	return resp.StatusCode, nil
}

// pacer emits intended-send timestamps onto jobs at ~rate per second using a
// 1ms tick with a fractional accumulator (so non-multiple-of-1000 rates are
// still accurate on average). If a worker isn't ready, the send is dropped and
// counted as an overrun rather than blocking (which would corrupt the open-loop
// model into closed-loop).
func pacer(ctx context.Context, rate int, jobs chan<- time.Time, overruns *uint64) {
	defer close(jobs)
	const tickHz = 1000
	tick := time.Second / tickHz
	perTick := float64(rate) / tickHz
	t := time.NewTicker(tick)
	defer t.Stop()
	var acc float64
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			acc += perTick
			emit := int(acc)
			acc -= float64(emit)
			for i := 0; i < emit; i++ {
				select {
				case jobs <- now:
				default:
					atomic.AddUint64(overruns, 1)
				}
			}
		}
	}
}

func progressLoop(ctx context.Context, stats []*workerStat) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	start := time.Now()
	var last uint64
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			var cur uint64
			for _, s := range stats {
				cur += s.live.Load()
			}
			fmt.Printf("  [%4.0fs] reqs≈%d  rps≈%d\n",
				time.Since(start).Seconds(), cur, cur-last)
			last = cur
		}
	}
}

// ── per-worker stats + HDR-style latency histogram ──────────────────────────

// Each worker owns a workerStat (no lock contention on the hot path); they are
// merged once at the end.
type workerStat struct {
	h           *hist
	count       uint64
	errors      uint64
	statusCount map[int]uint64
	// live is bumped every request (single writer, so uncontended) and read by
	// the progress goroutine; kept separate from count so the display read is
	// race-free without touching the hot-path fields.
	live atomic.Uint64
}

func newWorkerStat() *workerStat {
	return &workerStat{h: newHist(), statusCount: map[int]uint64{}}
}

func (s *workerStat) record(latency time.Duration, code int, err error) {
	s.live.Add(1)
	s.count++
	if err != nil {
		s.errors++
		return
	}
	s.statusCount[code]++
	if code/100 != 2 {
		s.errors++
	}
	s.h.record(uint64(latency.Microseconds()))
}

// hist is a log-linear latency histogram in microseconds (HdrHistogram-style).
// Values below subCount are 1:1; each higher power-of-two octave is split into
// subCount linear sub-buckets, giving ~1/subCount (~1.5%) relative error.
const (
	subBits  = 6
	subCount = 1 << subBits // 64
	// Track up to 600s; anything larger clamps into the top bucket.
	maxTrackMicros = uint64(600) * 1_000_000
)

type hist struct {
	counts []uint64
	sum    uint64 // sum of recorded micros (for mean)
	n      uint64
	min    uint64
	max    uint64
}

func newHist() *hist {
	return &hist{counts: make([]uint64, indexFor(maxTrackMicros)+1)}
}

func (h *hist) record(v uint64) {
	idx := indexFor(v)
	if idx >= len(h.counts) {
		idx = len(h.counts) - 1
	}
	h.counts[idx]++
	h.sum += v
	h.n++
	if h.min == 0 || v < h.min {
		h.min = v
	}
	if v > h.max {
		h.max = v
	}
}

func (h *hist) merge(o *hist) {
	for i, c := range o.counts {
		h.counts[i] += c
	}
	h.sum += o.sum
	h.n += o.n
	if o.n > 0 && (h.min == 0 || o.min < h.min) {
		h.min = o.min
	}
	if o.max > h.max {
		h.max = o.max
	}
}

func (h *hist) percentile(p float64) uint64 {
	if h.n == 0 {
		return 0
	}
	target := uint64(math.Ceil(p / 100 * float64(h.n)))
	if target == 0 {
		target = 1
	}
	var cum uint64
	for i, c := range h.counts {
		cum += c
		if cum >= target {
			return valueForIndex(i)
		}
	}
	return h.max
}

func (h *hist) mean() uint64 {
	if h.n == 0 {
		return 0
	}
	return h.sum / h.n
}

func indexFor(v uint64) int {
	if v < subCount {
		return int(v)
	}
	e := uint(bits.Len64(v)) - 1
	sub := (v >> (e - subBits)) & (subCount - 1)
	return int((e-subBits+1))<<subBits | int(sub)
}

func valueForIndex(i int) uint64 {
	if i < subCount {
		return uint64(i)
	}
	block := uint(i) >> subBits
	e := block + subBits - 1
	sub := uint64(i) & (subCount - 1)
	low := ((uint64(1) << subBits) | sub) << (e - subBits)
	width := uint64(1) << (e - subBits)
	return low + width/2
}

// ── reporting ────────────────────────────────────────────────────────────────

type result struct {
	Mode         string             `json:"mode"`
	Target       string             `json:"target"`
	Workers      int                `json:"workers"`
	Rate         int                `json:"rate"`
	Batch        int                `json:"batch"`
	ElapsedSec   float64            `json:"elapsed_sec"`
	Requests     uint64             `json:"requests"`
	Events       uint64             `json:"events"`
	RPS          float64            `json:"rps"`
	EventsPerSec float64            `json:"events_per_sec"`
	Errors       uint64             `json:"errors"`
	ErrorRatePct float64            `json:"error_rate_pct"`
	Overruns     uint64             `json:"overruns,omitempty"`
	StatusCounts map[string]uint64  `json:"status_counts"`
	LatencyMS    map[string]float64 `json:"latency_ms"`
}

func report(mode, target string, workers, rate, batch int, elapsed time.Duration, overruns uint64, stats []*workerStat, out string) {
	merged := newHist()
	var total, errors uint64
	statusCount := map[int]uint64{}
	for _, s := range stats {
		merged.merge(s.h)
		total += s.count
		errors += s.errors
		for code, n := range s.statusCount {
			statusCount[code] += n
		}
	}

	secs := elapsed.Seconds()
	rps := float64(total) / secs
	events := total * uint64(batch)
	ms := func(micros uint64) float64 { return float64(micros) / 1000 }

	errRate := 0.0
	if total > 0 {
		errRate = float64(errors) / float64(total) * 100
	}

	statusStr := map[string]uint64{}
	for code, n := range statusCount {
		statusStr[fmt.Sprintf("%d", code)] = n
	}

	res := result{
		Mode: mode, Target: target, Workers: workers, Rate: rate, Batch: batch,
		ElapsedSec: round2(secs), Requests: total, Events: events,
		RPS: round2(rps), EventsPerSec: round2(rps * float64(batch)),
		Errors: errors, ErrorRatePct: round2(errRate), Overruns: overruns,
		StatusCounts: statusStr,
		LatencyMS: map[string]float64{
			"min":  round2(ms(merged.min)),
			"mean": round2(ms(merged.mean())),
			"p50":  round2(ms(merged.percentile(50))),
			"p90":  round2(ms(merged.percentile(90))),
			"p95":  round2(ms(merged.percentile(95))),
			"p99":  round2(ms(merged.percentile(99))),
			"p999": round2(ms(merged.percentile(99.9))),
			"max":  round2(ms(merged.max)),
		},
	}

	fmt.Println("\n──────────── results ────────────")
	fmt.Printf("elapsed       %.2fs\n", res.ElapsedSec)
	fmt.Printf("requests      %d  (%.0f req/sec)\n", res.Requests, res.RPS)
	if batch > 1 {
		fmt.Printf("events        %d  (%.0f events/sec)\n", res.Events, res.EventsPerSec)
	}
	fmt.Printf("errors        %d  (%.2f%%)\n", res.Errors, res.ErrorRatePct)
	if rate > 0 {
		fmt.Printf("overruns      %d  (pacer couldn't hand off in time)\n", res.Overruns)
	}
	fmt.Printf("status        %v\n", statusStr)
	fmt.Printf("latency ms    min=%.2f mean=%.2f p50=%.2f p90=%.2f p95=%.2f p99=%.2f p99.9=%.2f max=%.2f\n",
		res.LatencyMS["min"], res.LatencyMS["mean"], res.LatencyMS["p50"], res.LatencyMS["p90"],
		res.LatencyMS["p95"], res.LatencyMS["p99"], res.LatencyMS["p999"], res.LatencyMS["max"])

	if out != "" {
		b, _ := json.MarshalIndent(res, "", "  ")
		if err := os.WriteFile(out, b, 0o644); err != nil {
			log.Printf("write %s: %v", out, err)
		} else {
			fmt.Printf("\nwrote %s\n", out)
		}
	}
}

func round2(f float64) float64 { return math.Round(f*100) / 100 }
