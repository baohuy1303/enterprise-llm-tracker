package http

import (
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"enterprise-llm-tracker/internal/service"
	"enterprise-llm-tracker/internal/store"
)

// SignalHandlers exposes the signal-analytics read endpoints. Mounted behind
// the same BearerAuth as the rest of /admin. Writes happen in the
// sentinel-workers binary; this surface is read-only.
type SignalHandlers struct {
	store  *store.Store
	logger *slog.Logger
}

func NewSignalHandlers(st *store.Store, logger *slog.Logger) *SignalHandlers {
	if logger == nil {
		logger = slog.Default()
	}
	return &SignalHandlers{store: st, logger: logger}
}

// EfficiencyList handles GET /admin/signals/efficiency?window=30d.
// Returns the latest rollup per engineer for the requested window. Sorted by
// dollars_per_merged_pr ascending — engineers with no merges land at the bottom.
func (h *SignalHandlers) EfficiencyList(w http.ResponseWriter, r *http.Request) {
	window := r.URL.Query().Get("window")
	if window == "" {
		window = "30d"
	}
	_, end, ok := service.WindowForName(window, time.Now().UTC())
	if !ok {
		writeError(w, http.StatusBadRequest, "window must be one of: 1d, 7d, 30d, 180d")
		return
	}
	rows, err := h.store.ListEngineerSignals(r.Context(), window, end)
	if err != nil {
		h.logger.Error("list engineer_signals failed", slog.String("err", err.Error()))
		writeError(w, http.StatusInternalServerError, "list engineer_signals failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"window":  window,
		"window_end": end.Format("2006-01-02"),
		"rows":    rows,
		"count":   len(rows),
	})
}

// EfficiencyOne handles GET /admin/signals/efficiency/{email}?window=30d.
func (h *SignalHandlers) EfficiencyOne(w http.ResponseWriter, r *http.Request) {
	email := r.PathValue("email")
	if email == "" {
		writeError(w, http.StatusBadRequest, "email path parameter required")
		return
	}
	window := r.URL.Query().Get("window")
	if window == "" {
		window = "30d"
	}
	_, end, ok := service.WindowForName(window, time.Now().UTC())
	if !ok {
		writeError(w, http.StatusBadRequest, "window must be one of: 1d, 7d, 30d, 180d")
		return
	}
	sig, err := h.store.GetEngineerSignal(r.Context(), email, window, end)
	if err != nil {
		h.logger.Error("get engineer_signal failed",
			slog.String("email", email), slog.String("err", err.Error()))
		writeError(w, http.StatusInternalServerError, "get engineer_signal failed")
		return
	}
	if sig == nil {
		writeError(w, http.StatusNotFound, "no rollup for this engineer/window — has the rollup job run yet?")
		return
	}
	writeJSON(w, http.StatusOK, sig)
}

// EventsList handles GET /admin/signals/events with optional filters:
//   ?engineer=alice@example.com   — filter to one engineer
//   ?type=burst                   — burst | spend_zscore_high | rhythm_break
//   ?severity=critical            — info | warn | critical
//   ?since=2026-05-20T00:00:00Z   — RFC3339 timestamp; defaults to 7 days ago
//   ?limit=100                    — capped at 1000
func (h *SignalHandlers) EventsList(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	filter := store.SignalEventFilter{
		EngineerID: q.Get("engineer"),
		SignalType: q.Get("type"),
		Severity:   q.Get("severity"),
	}
	if v := q.Get("since"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			writeError(w, http.StatusBadRequest, "since must be RFC3339")
			return
		}
		filter.Since = t
	}
	if v := q.Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			filter.Limit = n
		}
	}
	events, err := h.store.ListSignalEvents(r.Context(), filter)
	if err != nil {
		h.logger.Error("list signal_events failed", slog.String("err", err.Error()))
		writeError(w, http.StatusInternalServerError, "list signal_events failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"events": events,
		"count":  len(events),
	})
}

// EventsForEngineer handles GET /admin/signals/events/{email}.
// Same as EventsList but scoped to one engineer (path-value, not query-string).
func (h *SignalHandlers) EventsForEngineer(w http.ResponseWriter, r *http.Request) {
	email := r.PathValue("email")
	if email == "" {
		writeError(w, http.StatusBadRequest, "email path parameter required")
		return
	}
	q := r.URL.Query()
	filter := store.SignalEventFilter{
		EngineerID: email,
		SignalType: q.Get("type"),
		Severity:   q.Get("severity"),
	}
	if v := q.Get("since"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			writeError(w, http.StatusBadRequest, "since must be RFC3339")
			return
		}
		filter.Since = t
	}
	if v := q.Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			filter.Limit = n
		}
	}
	events, err := h.store.ListSignalEvents(r.Context(), filter)
	if err != nil {
		h.logger.Error("list signal_events failed",
			slog.String("email", email), slog.String("err", err.Error()))
		writeError(w, http.StatusInternalServerError, "list signal_events failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"engineer": email,
		"events":   events,
		"count":    len(events),
	})
}
