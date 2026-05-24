package main

import (
	"context"
	"encoding/json"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"

	"enterprise-llm-tracker/internal/config"
	"enterprise-llm-tracker/internal/ingest"
	"enterprise-llm-tracker/internal/middleware"
	"enterprise-llm-tracker/internal/migrate"
	"enterprise-llm-tracker/internal/registry"
	"enterprise-llm-tracker/internal/store"
)

func main() {
	configPath := "sentinel.yaml"
	if v := os.Getenv("SENTINEL_CONFIG"); v != "" {
		configPath = v
	}
	migrationsDir := "migrations"
	if v := os.Getenv("SENTINEL_MIGRATIONS_DIR"); v != "" {
		migrationsDir = v
	}

	cfg, err := config.Load(configPath)
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	rootCtx, rootCancel := context.WithCancel(context.Background())
	defer rootCancel()

	pgPool, err := pgxpool.New(rootCtx, cfg.Postgres.URL)
	if err != nil {
		log.Fatalf("postgres connect: %v", err)
	}
	defer pgPool.Close()
	if err := pingWithTimeout(rootCtx, 5*time.Second, pgPool.Ping); err != nil {
		log.Fatalf("postgres ping: %v", err)
	}

	applied, err := migrate.Apply(rootCtx, pgPool, migrationsDir)
	if err != nil {
		log.Fatalf("migrate: %v", err)
	}
	if len(applied) > 0 {
		log.Printf("migrations applied: %v", applied)
	}

	rdb := redis.NewClient(&redis.Options{Addr: cfg.Redis.Addr})
	defer rdb.Close()
	if err := pingWithTimeout(rootCtx, 5*time.Second, func(ctx context.Context) error {
		return rdb.Ping(ctx).Err()
	}); err != nil {
		log.Fatalf("redis ping: %v", err)
	}

	st := store.New(pgPool, rdb)

	reg := registry.New(pgPool, time.Duration(cfg.Registry.RefreshIntervalSeconds)*time.Second)
	if err := reg.Load(rootCtx); err != nil {
		log.Fatalf("registry initial load: %v", err)
	}
	rs := reg.Stats()
	log.Printf("registry loaded %d active engineers, last refresh %dms ago",
		rs.Count, time.Since(rs.LastRefreshAt).Milliseconds())
	reg.StartRefresh(rootCtx)

	if err := st.RebuildCounters(rootCtx, reg.AllEmails(), slog.Default()); err != nil {
		log.Printf("redis counter rebuild: %v", err)
	} else {
		log.Printf("redis counters rebuilt from postgres")
	}

	ingestHandler := ingest.New(reg, st, nil)

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", readyzHandler(reg, st))
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("POST /ingest/otel/v1/metrics", ingestHandler.Metrics)
	mux.HandleFunc("POST /ingest/otel/v1/logs", ingestHandler.Logs)

	srv := &http.Server{
		Addr:    cfg.Listen,
		Handler: middleware.Logging(mux),
	}

	go func() {
		log.Printf("sentinel-api listening on %s", cfg.Listen)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)
	<-quit

	log.Println("shutting down...")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Fatal("forced shutdown:", err)
	}
	rootCancel()
	log.Println("exited")
}

func pingWithTimeout(parent context.Context, d time.Duration, ping func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(parent, d)
	defer cancel()
	return ping(ctx)
}

func readyzHandler(reg *registry.EngineerRegistry, st *store.Store) http.HandlerFunc {
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
		if err := st.PG.Ping(pingCtx); err != nil {
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
