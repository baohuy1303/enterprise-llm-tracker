package http

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"enterprise-llm-tracker/internal/ingest"
	"enterprise-llm-tracker/internal/middleware"
	"enterprise-llm-tracker/internal/registry"
	"enterprise-llm-tracker/internal/store"
)

func NewRouter(ih *ingest.Handler, reg *registry.EngineerRegistry, st *store.Store) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", healthz)
	mux.HandleFunc("GET /readyz", readyz(reg, st))
	mux.HandleFunc("POST /ingest/otel/v1/metrics", ih.Metrics)
	mux.HandleFunc("POST /ingest/otel/v1/logs", ih.Logs)

	// /metrics sits outside the logging middleware so scrape requests don't
	// pollute the very metrics being scraped.
	top := http.NewServeMux()
	top.Handle("GET /metrics", promhttp.Handler())
	top.Handle("/", middleware.Logging(mux))

	return top
}

func healthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

func readyz(reg *registry.EngineerRegistry, st *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		s := reg.Stats()
		body := map[string]any{
			"status":             "ok",
			"engineer_count":     s.Count,
			"last_refresh_at":    s.LastRefreshAt.Format(time.RFC3339),
			"last_refresh_error": s.LastRefreshError,
		}
		pingCtx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := st.PingPG(pingCtx); err != nil {
			body["postgres"] = err.Error()
			body["status"] = "degraded"
		} else {
			body["postgres"] = "ok"
		}
		if err := st.PingRedis(pingCtx); err != nil {
			body["redis"] = err.Error()
			body["status"] = "degraded"
		} else {
			body["redis"] = "ok"
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(body)
	}
}
