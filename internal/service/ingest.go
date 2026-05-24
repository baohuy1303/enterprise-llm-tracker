package service

import (
	"context"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"enterprise-llm-tracker/internal/registry"
	"enterprise-llm-tracker/internal/store"
)

var metricTypeMap = map[string]string{
	"claude_code.cost.usage":          "cost",
	"claude_code.token.usage":         "token",
	"claude_code.lines_of_code.count": "lines_of_code",
	"claude_code.pull_request.count":  "pull_request",
	"claude_code.commit.count":        "commit",
	"claude_code.session.count":       "session",
}

type IngestService struct {
	registry *registry.EngineerRegistry
	store    *store.Store
	logger   *slog.Logger
}

func NewIngestService(reg *registry.EngineerRegistry, st *store.Store, logger *slog.Logger) *IngestService {
	if logger == nil {
		logger = slog.Default()
	}
	return &IngestService{registry: reg, store: st, logger: logger}
}

// RecordMetric processes a single OTel metric data point: attributes the event
// to an engineer, maps the metric name to Event fields, and dual-writes to
// Postgres + Redis via store.WriteEvent.
func (s *IngestService) RecordMetric(ctx context.Context, resAttrs, dpAttrs map[string]string, name string, value float64, ts time.Time) {
	if !strings.HasPrefix(name, "claude_code.") {
		return
	}
	mtype, ok := metricTypeMap[name]
	if !ok {
		mtype = strings.TrimPrefix(name, "claude_code.")
	}

	engineer, known := s.lookupEngineer(dpAttrs, resAttrs)
	if !known {
		s.logger.Warn("unattributed_metric",
			slog.String("identity", engineer),
			slog.String("metric", name),
		)
		return
	}

	ev := store.Event{
		EngineerID: engineer,
		OccurredAt: ts,
		Source:     "otel_metric",
		MetricName: name,
		Model:      dpAttrs["model"],
		Raw:        dpAttrs,
	}
	switch name {
	case "claude_code.cost.usage":
		v := value
		ev.CostUSD = &v
	case "claude_code.token.usage":
		v := int(value)
		switch dpAttrs["type"] {
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

	if err := s.store.WriteEvent(ctx, ev); err != nil {
		s.logger.Error("store metric failed",
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
	if v := dpAttrs["model"]; v != "" {
		args = append(args, slog.String("model", v))
	}
	if v := dpAttrs["type"]; v != "" {
		args = append(args, slog.String("token_type", v))
	}
	s.logger.Info("usage_event", args...)
}

// RecordLogEvent processes a single OTel log record: attributes the event to
// an engineer and writes to Postgres only (no Redis) to avoid double-counting
// with the metric stream.
func (s *IngestService) RecordLogEvent(ctx context.Context, resAttrs, eventAttrs map[string]string, eventName string, ts time.Time) {
	engineer, known := s.lookupEngineer(eventAttrs, resAttrs)
	if !known {
		s.logger.Warn("unattributed_event",
			slog.String("identity", engineer),
			slog.String("event", eventName),
		)
		return
	}

	ev := store.Event{
		EngineerID: engineer,
		OccurredAt: ts,
		Source:     "otel_event",
		MetricName: eventName,
		Model:      eventAttrs["model"],
		Raw:        eventAttrs,
	}
	if f, ok := parseFloat(eventAttrs["cost_usd"]); ok {
		ev.CostUSD = &f
	}
	if i, ok := parseInt(eventAttrs["input_tokens"]); ok {
		ev.TokensInput = &i
	}
	if i, ok := parseInt(eventAttrs["output_tokens"]); ok {
		ev.TokensOutput = &i
	}
	if i, ok := parseInt(eventAttrs["cache_read_tokens"]); ok {
		ev.TokensCacheRead = &i
	}
	if i, ok := parseInt(eventAttrs["cache_creation_tokens"]); ok {
		ev.TokensCacheCreation = &i
	}

	if err := s.store.WriteLogEvent(ctx, ev); err != nil {
		s.logger.Error("store event failed",
			slog.String("engineer", engineer),
			slog.String("event", eventName),
			slog.String("err", err.Error()))
	}

	args := []any{
		slog.String("engineer", engineer),
		slog.String("event", eventName),
		slog.Time("ts", ts),
	}
	if v := eventAttrs["model"]; v != "" {
		args = append(args, slog.String("model", v))
	}
	if v := eventAttrs["cost_usd"]; v != "" {
		args = append(args, slog.String("cost_usd", v))
	}
	s.logger.Info("usage_log", args...)
}

// lookupEngineer checks dpAttrs first (more specific), then resAttrs for
// user.email or enduser.id. Returns the canonical email and whether it matched
// a known active engineer.
func (s *IngestService) lookupEngineer(dpAttrs, resAttrs map[string]string) (string, bool) {
	for _, attrs := range []map[string]string{dpAttrs, resAttrs} {
		if email := attrs["user.email"]; email != "" {
			if eng, ok := s.registry.LookupByEmail(email); ok {
				return eng.Email, true
			}
			return email, false
		}
		if id := attrs["enduser.id"]; id != "" {
			if eng, ok := s.registry.LookupByEmail(id); ok {
				return eng.Email, true
			}
			return id, false
		}
	}
	return "", false
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
