package ingest

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/protobuf/proto"

	"enterprise-llm-tracker/internal/registry"
	"enterprise-llm-tracker/internal/store"
)

type Handler struct {
	registry *registry.EngineerRegistry
	store    *store.Store
	logger   *slog.Logger
}

func New(reg *registry.EngineerRegistry, st *store.Store, logger *slog.Logger) *Handler {
	if logger == nil {
		logger = slog.Default()
	}
	return &Handler{registry: reg, store: st, logger: logger}
}

func (h *Handler) Metrics(w http.ResponseWriter, r *http.Request) {
	body, err := readBody(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var req colmetricspb.ExportMetricsServiceRequest
	if err := proto.Unmarshal(body, &req); err != nil {
		h.logger.Error("metrics unmarshal failed", slog.String("err", err.Error()))
		http.Error(w, "unmarshal: "+err.Error(), http.StatusBadRequest)
		return
	}
	for _, rm := range req.ResourceMetrics {
		resAttrs := rm.GetResource().GetAttributes()
		for _, sm := range rm.GetScopeMetrics() {
			for _, m := range sm.GetMetrics() {
				h.processMetric(r.Context(), resAttrs, m)
			}
		}
	}
	writeProtoResponse(w, &colmetricspb.ExportMetricsServiceResponse{})
}

func (h *Handler) Logs(w http.ResponseWriter, r *http.Request) {
	body, err := readBody(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var req collogspb.ExportLogsServiceRequest
	if err := proto.Unmarshal(body, &req); err != nil {
		h.logger.Error("logs unmarshal failed", slog.String("err", err.Error()))
		http.Error(w, "unmarshal: "+err.Error(), http.StatusBadRequest)
		return
	}
	for _, rl := range req.ResourceLogs {
		resAttrs := rl.GetResource().GetAttributes()
		for _, sl := range rl.GetScopeLogs() {
			for _, lr := range sl.GetLogRecords() {
				h.processLogRecord(r.Context(), resAttrs, lr)
			}
		}
	}
	writeProtoResponse(w, &collogspb.ExportLogsServiceResponse{})
}

func (h *Handler) lookupEngineer(attrSets ...[]*commonpb.KeyValue) (string, bool) {
	var email, enduserID string
	for _, attrs := range attrSets {
		for _, kv := range attrs {
			switch kv.GetKey() {
			case "user.email":
				if email == "" {
					email = kv.GetValue().GetStringValue()
				}
			case "enduser.id":
				if enduserID == "" {
					enduserID = kv.GetValue().GetStringValue()
				}
			}
		}
	}
	if email != "" {
		if eng, ok := h.registry.LookupByEmail(email); ok {
			return eng.Email, true
		}
		return email, false
	}
	if enduserID != "" {
		if eng, ok := h.registry.LookupByEmail(enduserID); ok {
			return eng.Email, true
		}
		return enduserID, false
	}
	return "", false
}

var metricTypeMap = map[string]string{
	"claude_code.cost.usage":          "cost",
	"claude_code.token.usage":         "token",
	"claude_code.lines_of_code.count": "lines_of_code",
	"claude_code.pull_request.count":  "pull_request",
	"claude_code.commit.count":        "commit",
	"claude_code.session.count":       "session",
}

// processMetric writes one usage_events row per data point and (for cost/token
// metrics) increments the corresponding Redis counter. Log records use
// processLogRecord — they go to PG only so Redis isn't double-counted.
func (h *Handler) processMetric(ctx context.Context, resAttrs []*commonpb.KeyValue, m *metricspb.Metric) {
	name := m.GetName()
	if !strings.HasPrefix(name, "claude_code.") {
		return
	}
	mtype, ok := metricTypeMap[name]
	if !ok {
		mtype = strings.TrimPrefix(name, "claude_code.")
	}
	for _, dp := range extractNumberDataPoints(m) {
		engineer, known := h.lookupEngineer(dp.GetAttributes(), resAttrs)
		if !known {
			h.logger.Warn("unattributed_metric",
				slog.String("identity", engineer),
				slog.String("metric", name),
			)
			continue
		}
		attrs := keyValuesToMap(dp.GetAttributes())
		value := numberValue(dp)
		ts := time.Unix(0, int64(dp.GetTimeUnixNano()))

		ev := store.Event{
			EngineerID: engineer,
			OccurredAt: ts,
			Source:     "otel_metric",
			MetricName: name,
			Model:      attrs["model"],
			Raw:        attrs,
		}
		switch name {
		case "claude_code.cost.usage":
			v := value
			ev.CostUSD = &v
		case "claude_code.token.usage":
			v := int(value)
			switch attrs["type"] {
			case "input":
				ev.TokensInput = &v
			case "output":
				ev.TokensOutput = &v
			case "cacheRead":
				ev.TokensCacheRead = &v
			case "cacheCreation":
				ev.TokensCacheCreation = &v
			}
		}
		if err := h.store.WriteEvent(ctx, ev); err != nil {
			h.logger.Error("store metric failed",
				slog.String("engineer", engineer),
				slog.String("metric", name),
				slog.String("err", err.Error()))
		}

		args := []any{
			slog.String("engineer", engineer),
			slog.String("type", mtype),
			slog.Float64("value", value),
			slog.Time("ts", ts),
		}
		if v := attrs["model"]; v != "" {
			args = append(args, slog.String("model", v))
		}
		if v := attrs["type"]; v != "" {
			args = append(args, slog.String("token_type", v))
		}
		h.logger.Info("usage_event", args...)
	}
}

// processLogRecord writes per-event forensic rows to PG. Does not touch Redis
// counters — those are owned by processMetric to avoid double-counting.
func (h *Handler) processLogRecord(ctx context.Context, resAttrs []*commonpb.KeyValue, lr *logspb.LogRecord) {
	attrs := keyValuesToMap(lr.GetAttributes())
	eventName := lr.GetBody().GetStringValue()
	if eventName == "" {
		eventName = attrs["event.name"]
	}
	engineer, known := h.lookupEngineer(lr.GetAttributes(), resAttrs)
	if !known {
		h.logger.Warn("unattributed_event",
			slog.String("identity", engineer),
			slog.String("event", eventName),
		)
		return
	}

	ev := store.Event{
		EngineerID: engineer,
		OccurredAt: time.Unix(0, int64(lr.GetTimeUnixNano())),
		Source:     "otel_event",
		MetricName: eventName,
		Model:      attrs["model"],
		Raw:        attrs,
	}
	if f, ok := parseFloat(attrs["cost_usd"]); ok {
		ev.CostUSD = &f
	}
	if i, ok := parseInt(attrs["input_tokens"]); ok {
		ev.TokensInput = &i
	}
	if i, ok := parseInt(attrs["output_tokens"]); ok {
		ev.TokensOutput = &i
	}
	if i, ok := parseInt(attrs["cache_read_tokens"]); ok {
		ev.TokensCacheRead = &i
	}
	if i, ok := parseInt(attrs["cache_creation_tokens"]); ok {
		ev.TokensCacheCreation = &i
	}
	// PG insert only — skip Redis increments to avoid double-counting with metric stream.
	if _, err := h.store.PG.Exec(ctx, `
		INSERT INTO usage_events
		  (engineer_id, occurred_at, source, metric_name,
		   cost_usd, tokens_input, tokens_output, tokens_cache_read, tokens_cache_creation,
		   model, raw)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
	`, ev.EngineerID, ev.OccurredAt, ev.Source, ev.MetricName,
		ev.CostUSD, ev.TokensInput, ev.TokensOutput, ev.TokensCacheRead, ev.TokensCacheCreation,
		ev.Model, mustJSONRaw(ev.Raw)); err != nil {
		h.logger.Error("store event failed",
			slog.String("engineer", engineer),
			slog.String("event", eventName),
			slog.String("err", err.Error()))
	}

	args := []any{
		slog.String("engineer", engineer),
		slog.String("event", eventName),
		slog.Time("ts", ev.OccurredAt),
	}
	if v := attrs["model"]; v != "" {
		args = append(args, slog.String("model", v))
	}
	if v := attrs["cost_usd"]; v != "" {
		args = append(args, slog.String("cost_usd", v))
	}
	h.logger.Info("usage_log", args...)
}

func parseFloat(s string) (float64, bool) {
	if s == "" {
		return 0, false
	}
	f, err := strconv.ParseFloat(s, 64)
	return f, err == nil
}

func parseInt(s string) (int, bool) {
	if s == "" {
		return 0, false
	}
	i, err := strconv.Atoi(s)
	return i, err == nil
}

func mustJSONRaw(m map[string]string) []byte {
	b, _ := json.Marshal(m)
	return b
}

func readBody(r *http.Request) ([]byte, error) {
	defer r.Body.Close()
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}
	if r.Header.Get("Content-Encoding") == "gzip" {
		gz, err := gzip.NewReader(bytes.NewReader(body))
		if err != nil {
			return nil, fmt.Errorf("gzip reader: %w", err)
		}
		defer gz.Close()
		body, err = io.ReadAll(gz)
		if err != nil {
			return nil, fmt.Errorf("gzip read: %w", err)
		}
	}
	return body, nil
}

func writeProtoResponse(w http.ResponseWriter, msg proto.Message) {
	data, err := proto.Marshal(msg)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/x-protobuf")
	w.WriteHeader(http.StatusOK)
	w.Write(data)
}

func extractNumberDataPoints(m *metricspb.Metric) []*metricspb.NumberDataPoint {
	switch d := m.GetData().(type) {
	case *metricspb.Metric_Sum:
		return d.Sum.GetDataPoints()
	case *metricspb.Metric_Gauge:
		return d.Gauge.GetDataPoints()
	default:
		return nil
	}
}

func numberValue(dp *metricspb.NumberDataPoint) float64 {
	switch v := dp.GetValue().(type) {
	case *metricspb.NumberDataPoint_AsDouble:
		return v.AsDouble
	case *metricspb.NumberDataPoint_AsInt:
		return float64(v.AsInt)
	default:
		return 0
	}
}

func keyValuesToMap(kvs []*commonpb.KeyValue) map[string]string {
	out := make(map[string]string, len(kvs))
	for _, kv := range kvs {
		out[kv.GetKey()] = anyValueString(kv.GetValue())
	}
	return out
}

func anyValueString(v *commonpb.AnyValue) string {
	if v == nil {
		return ""
	}
	switch x := v.GetValue().(type) {
	case *commonpb.AnyValue_StringValue:
		return x.StringValue
	case *commonpb.AnyValue_BoolValue:
		return fmt.Sprintf("%t", x.BoolValue)
	case *commonpb.AnyValue_IntValue:
		return fmt.Sprintf("%d", x.IntValue)
	case *commonpb.AnyValue_DoubleValue:
		return fmt.Sprintf("%g", x.DoubleValue)
	default:
		return ""
	}
}
