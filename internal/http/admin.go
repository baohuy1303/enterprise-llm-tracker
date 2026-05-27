package http

import (
	"context"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"enterprise-llm-tracker/internal/service"
	"enterprise-llm-tracker/internal/store"
)

// AdminHandlers groups admin-only endpoints that don't belong to a single
// REST resource (leaderboard, usage debugging, ops triggers). Engineer CRUD
// is in EngineerHandlers; both are mounted behind the same BearerAuth.
type AdminHandlers struct {
	store     *store.Store
	engineers *service.EngineerService
	logger    *slog.Logger
}

func NewAdminHandlers(st *store.Store, engineers *service.EngineerService, logger *slog.Logger) *AdminHandlers {
	if logger == nil {
		logger = slog.Default()
	}
	return &AdminHandlers{store: st, engineers: engineers, logger: logger}
}

// Leaderboard returns the efficiency leaderboard for a window. Sorted by
// $/merged-PR ascending; engineers with zero merges land at the bottom.
func (h *AdminHandlers) Leaderboard(w http.ResponseWriter, r *http.Request) {
	window := r.URL.Query().Get("window")
	if window == "" {
		window = "7d"
	}
	start, end, ok := service.WindowForName(window, time.Now().UTC())
	if !ok {
		writeError(w, http.StatusBadRequest, "window must be one of: 1d, 7d, 30d, 180d")
		return
	}
	entries, err := h.store.Leaderboard(r.Context(), start, end, 100)
	if err != nil {
		h.logger.Error("leaderboard query failed", slog.String("err", err.Error()))
		writeError(w, http.StatusInternalServerError, "leaderboard query failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"window":  window,
		"start":   start.Format("2006-01-02"),
		"end":     end.Format("2006-01-02"),
		"entries": entries,
	})
}

// RecentUsage returns the N most recent usage_events rows. Debugging surface
// — clients should generally drive dashboards off engineer detail or rollups.
// Accepts ?limit=N (default 100, capped at 1000).
func (h *AdminHandlers) RecentUsage(w http.ResponseWriter, r *http.Request) {
	limit := 100
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	events, err := h.store.ListRecentEvents(r.Context(), limit)
	if err != nil {
		h.logger.Error("recent usage query failed", slog.String("err", err.Error()))
		writeError(w, http.StatusInternalServerError, "recent usage query failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"events": events,
		"count":  len(events),
	})
}

// RefreshRegistry forces an immediate reload of the engineer registry. Useful
// right after bulk onboarding so the ingest hot path can attribute traffic
// without waiting on the 30-second background refresh.
func (h *AdminHandlers) RefreshRegistry(w http.ResponseWriter, r *http.Request) {
	if err := h.engineers.RefreshRegistry(r.Context()); err != nil {
		h.logger.Error("registry refresh failed", slog.String("err", err.Error()))
		writeError(w, http.StatusInternalServerError, "registry refresh failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "refreshed"})
}

// RefreshEfficiency publishes a trigger to the collector (running in
// sentinel-workers) and returns 202 immediately. The collector debounces, so
// hammering this endpoint won't trigger more than one run per debounce window.
func (h *AdminHandlers) RefreshEfficiency(w http.ResponseWriter, r *http.Request) {
	// Detach from the request context so the publish survives the response
	// being written. Pub/sub publish should be sub-millisecond anyway.
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := h.store.PublishCollectorTrigger(ctx); err != nil {
			h.logger.Warn("collector trigger publish failed",
				slog.String("err", err.Error()))
		}
	}()
	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write([]byte(`{"status":"accepted"}` + "\n"))
}
