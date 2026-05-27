package http

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"enterprise-llm-tracker/internal/service"
	"enterprise-llm-tracker/internal/store"
)

// AdminHandlers groups admin-only endpoints. Wired behind a BearerAuth
// middleware; see router.go.
type AdminHandlers struct {
	store  *store.Store
	logger *slog.Logger
}

func NewAdminHandlers(st *store.Store, logger *slog.Logger) *AdminHandlers {
	if logger == nil {
		logger = slog.Default()
	}
	return &AdminHandlers{store: st, logger: logger}
}

// Leaderboard returns the efficiency leaderboard for a window ("1d", "7d",
// "30d"). Sorted by $/merged-PR ascending; engineers with zero merges land at
// the bottom.
func (h *AdminHandlers) Leaderboard(w http.ResponseWriter, r *http.Request) {
	window := r.URL.Query().Get("window")
	if window == "" {
		window = "7d"
	}
	start, end, ok := service.WindowForName(window, time.Now().UTC())
	if !ok {
		http.Error(w, `window must be one of: 1d, 7d, 30d`, http.StatusBadRequest)
		return
	}
	entries, err := h.store.Leaderboard(r.Context(), start, end, 100)
	if err != nil {
		h.logger.Error("leaderboard query failed", slog.String("err", err.Error()))
		http.Error(w, "leaderboard query failed", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"window":  window,
		"start":   start.Format("2006-01-02"),
		"end":     end.Format("2006-01-02"),
		"entries": entries,
	})
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
