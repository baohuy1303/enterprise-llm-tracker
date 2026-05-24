package ingest

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/protobuf/proto"

	"enterprise-llm-tracker/internal/service"
)

type Handler struct {
	svc    *service.IngestService
	logger *slog.Logger
}

func New(svc *service.IngestService, logger *slog.Logger) *Handler {
	if logger == nil {
		logger = slog.Default()
	}
	return &Handler{svc: svc, logger: logger}
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
		resAttrs := keyValuesToMap(rm.GetResource().GetAttributes())
		for _, sm := range rm.GetScopeMetrics() {
			for _, m := range sm.GetMetrics() {
				for _, dp := range extractNumberDataPoints(m) {
					dpAttrs := keyValuesToMap(dp.GetAttributes())
					value := numberValue(dp)
					ts := time.Unix(0, int64(dp.GetTimeUnixNano()))
					h.svc.RecordMetric(r.Context(), resAttrs, dpAttrs, m.GetName(), value, ts)
				}
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
		resAttrs := keyValuesToMap(rl.GetResource().GetAttributes())
		for _, sl := range rl.GetScopeLogs() {
			for _, lr := range sl.GetLogRecords() {
				eventAttrs := keyValuesToMap(lr.GetAttributes())
				eventName := lr.GetBody().GetStringValue()
				if eventName == "" {
					eventName = eventAttrs["event.name"]
				}
				ts := time.Unix(0, int64(lr.GetTimeUnixNano()))
				h.svc.RecordLogEvent(r.Context(), resAttrs, eventAttrs, eventName, ts)
			}
		}
	}
	writeProtoResponse(w, &collogspb.ExportLogsServiceResponse{})
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
