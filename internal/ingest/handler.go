package ingest

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/protobuf/proto"

	"enterprise-llm-tracker/internal/registry"
)

type Handler struct {
	registry *registry.EngineerRegistry
	logger   *slog.Logger
}

func New(reg *registry.EngineerRegistry, logger *slog.Logger) *Handler {
	if logger == nil {
		logger = slog.Default()
	}
	return &Handler{registry: reg, logger: logger}
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
				h.processMetric(resAttrs, m)
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
				h.processLogRecord(resAttrs, lr)
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

func (h *Handler) processMetric(resAttrs []*commonpb.KeyValue, m *metricspb.Metric) {
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
		args := []any{
			slog.String("engineer", engineer),
			slog.String("type", mtype),
			slog.Float64("value", numberValue(dp)),
			slog.Time("ts", time.Unix(0, int64(dp.GetTimeUnixNano()))),
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

func (h *Handler) processLogRecord(resAttrs []*commonpb.KeyValue, lr *logspb.LogRecord) {
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
	args := []any{
		slog.String("engineer", engineer),
		slog.String("event", eventName),
		slog.Time("ts", time.Unix(0, int64(lr.GetTimeUnixNano()))),
	}
	if v := attrs["model"]; v != "" {
		args = append(args, slog.String("model", v))
	}
	if v := attrs["cost_usd"]; v != "" {
		args = append(args, slog.String("cost_usd", v))
	}
	if v := attrs["input_tokens"]; v != "" {
		args = append(args, slog.String("input_tokens", v))
	}
	if v := attrs["output_tokens"]; v != "" {
		args = append(args, slog.String("output_tokens", v))
	}
	h.logger.Info("usage_log", args...)
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
