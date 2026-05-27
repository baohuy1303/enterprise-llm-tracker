package main

import (
	"context"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"

	"enterprise-llm-tracker/internal/config"
	apphttp "enterprise-llm-tracker/internal/http"
	"enterprise-llm-tracker/internal/ingest"
	appkafka "enterprise-llm-tracker/internal/kafka"
	"enterprise-llm-tracker/internal/migrate"
	"enterprise-llm-tracker/internal/registry"
	"enterprise-llm-tracker/internal/service"
	"enterprise-llm-tracker/internal/store"
)

const usageEventsPartitions = 12

func main() {
	// .env is optional — in production, env vars come from the orchestrator.
	_ = godotenv.Load()

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

	if err := appkafka.EnsureTopic(cfg.Kafka.Brokers, cfg.Kafka.Topic, usageEventsPartitions); err != nil {
		log.Printf("kafka EnsureTopic warning: %v (continuing — topic may already exist)", err)
	} else {
		log.Printf("kafka topic %q ready (%d partitions)", cfg.Kafka.Topic, usageEventsPartitions)
	}
	producer := appkafka.NewProducer(cfg.Kafka.Brokers, cfg.Kafka.Topic, slog.Default())
	defer producer.Close()

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

	ingestSvc := service.NewIngestService(reg, st, producer, slog.Default())
	ingestHandler := ingest.New(ingestSvc, nil)

	adminToken := ""
	if cfg.Admin.TokenEnv != "" {
		adminToken = os.Getenv(cfg.Admin.TokenEnv)
	}
	if adminToken == "" {
		log.Printf("warning: %s not set — /admin/* endpoints will return 503 until configured",
			cfg.Admin.TokenEnv)
	}
	router := apphttp.NewRouter(ingestHandler, reg, st, adminToken)

	srv := &http.Server{
		Addr:    cfg.Listen,
		Handler: router,
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
