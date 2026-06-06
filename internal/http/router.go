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
	"enterprise-llm-tracker/internal/service"
	"enterprise-llm-tracker/internal/store"
)

func NewRouter(
	ih *ingest.Handler,
	reg *registry.EngineerRegistry,
	st *store.Store,
	engineerSvc *service.EngineerService,
	adminToken string,
) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", healthz)
	mux.HandleFunc("GET /readyz", readyz(reg, st))
	mux.HandleFunc("POST /ingest/otel/v1/metrics", ih.Metrics)
	mux.HandleFunc("POST /ingest/otel/v1/logs", ih.Logs)

	// Admin endpoints: gated by bearer-token middleware. Mounted on the same
	// mux as ingest because they share the request-logging middleware below.
	admin := NewAdminHandlers(st, engineerSvc, nil)
	engineers := NewEngineerHandlers(engineerSvc, nil)
	signals := NewSignalHandlers(st, nil)

	adminMux := http.NewServeMux()
	// engineer CRUD
	adminMux.HandleFunc("POST /admin/engineers", engineers.Create)
	adminMux.HandleFunc("GET /admin/engineers", engineers.List)
	adminMux.HandleFunc("GET /admin/engineers/{email}", engineers.Get)
	adminMux.HandleFunc("PUT /admin/engineers/{email}", engineers.Update)
	adminMux.HandleFunc("DELETE /admin/engineers/{email}", engineers.Delete)
	// analytics + ops
	adminMux.HandleFunc("GET /admin/leaderboard", admin.Leaderboard)
	adminMux.HandleFunc("GET /admin/usage/recent", admin.RecentUsage)
	adminMux.HandleFunc("POST /admin/refresh-efficiency", admin.RefreshEfficiency)
	adminMux.HandleFunc("POST /admin/registry/refresh", admin.RefreshRegistry)
	// signal analytics (read-only; writes happen in sentinel-workers)
	adminMux.HandleFunc("GET /admin/signals/efficiency", signals.EfficiencyList)
	adminMux.HandleFunc("GET /admin/signals/efficiency/{email}", signals.EfficiencyOne)
	adminMux.HandleFunc("GET /admin/signals/events", signals.EventsList)
	adminMux.HandleFunc("GET /admin/signals/events/{email}", signals.EventsForEngineer)

	mux.Handle("/admin/", middleware.BearerAuth(adminToken, adminMux))

	// /metrics sits outside the logging middleware so scrape requests don't
	// pollute the very metrics being scraped.
	top := http.NewServeMux()
	top.Handle("GET /metrics", promhttp.Handler())
	top.Handle("/", middleware.Logging(mux))

	return top
}

func healthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
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
