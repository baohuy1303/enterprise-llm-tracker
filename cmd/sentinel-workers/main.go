package main

import (
	"context"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"

	"enterprise-llm-tracker/internal/config"
	appgithub "enterprise-llm-tracker/internal/github"
	appkafka "enterprise-llm-tracker/internal/kafka"
	"enterprise-llm-tracker/internal/registry"
	"enterprise-llm-tracker/internal/service"
	"enterprise-llm-tracker/internal/slack"
	"enterprise-llm-tracker/internal/store"
)

func main() {
	// .env is optional — in production, env vars come from the orchestrator.
	_ = godotenv.Load()

	configPath := "sentinel.yaml"
	if v := os.Getenv("SENTINEL_CONFIG"); v != "" {
		configPath = v
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

	rdb := redis.NewClient(&redis.Options{Addr: cfg.Redis.Addr})
	defer rdb.Close()
	if err := pingWithTimeout(rootCtx, 5*time.Second, func(ctx context.Context) error {
		return rdb.Ping(ctx).Err()
	}); err != nil {
		log.Fatalf("redis ping: %v", err)
	}

	st := store.New(pgPool, rdb)

	// Minimal health server so Kubernetes can liveness/readiness probe the
	// workers process — it otherwise has no HTTP surface. Liveness = process is
	// up; readiness = Postgres + Redis reachable.
	healthAddr := ":8082"
	if v := os.Getenv("WORKERS_HEALTH_ADDR"); v != "" {
		healthAddr = v
	}
	healthMux := http.NewServeMux()
	healthMux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	healthMux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := st.PingPG(ctx); err != nil {
			http.Error(w, "postgres: "+err.Error(), http.StatusServiceUnavailable)
			return
		}
		if err := st.PingRedis(ctx); err != nil {
			http.Error(w, "redis: "+err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready"))
	})
	healthSrv := &http.Server{Addr: healthAddr, Handler: healthMux}
	go func() {
		log.Printf("workers health server listening on %s", healthAddr)
		if err := healthSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("health server error: %v", err)
		}
	}()

	reg := registry.New(pgPool, time.Duration(cfg.Registry.RefreshIntervalSeconds)*time.Second)
	if err := reg.Load(rootCtx); err != nil {
		log.Fatalf("registry initial load: %v", err)
	}
	reg.StartRefresh(rootCtx)

	slackToken := os.Getenv(cfg.Slack.BotTokenEnv)
	slackClient := slack.New(slackToken, slog.Default())
	if !slackClient.Configured() {
		log.Printf("warning: %s not set — threshold worker will log instead of posting to Slack",
			cfg.Slack.BotTokenEnv)
	}

	thresholdSvc := service.NewThresholdService(st, reg, slackClient, cfg.Thresholds, slog.Default())
	persistSvc := service.NewPersistService(st, slog.Default())
	signalDetector := service.NewSignalDetector(st, reg, slackClient, cfg.Signals, slog.Default())

	baselineRebuilder := service.NewBaselineRebuilder(st, reg, cfg.Signals, slog.Default())
	efficiencyRollup := service.NewEfficiencyRollup(st, reg, cfg.Signals, slog.Default())
	go baselineRebuilder.Run(rootCtx)
	go efficiencyRollup.Run(rootCtx)
	log.Printf("signal-analytics jobs started (baseline rebuild + efficiency rollup, interval=%ds)",
		cfg.Signals.BaselineRebuildIntervalSeconds)

	// GitHub efficiency collector — only enabled when a token + org are
	// configured. Without those, the github-trigger consumer becomes a no-op.
	var collector *service.EfficiencyCollector
	githubToken := ""
	if cfg.GitHub.TokenEnv != "" {
		githubToken = os.Getenv(cfg.GitHub.TokenEnv)
	}
	if githubToken != "" && cfg.GitHub.Org != "" {
		ghClient := appgithub.New(githubToken, slog.Default())
		collector = service.NewEfficiencyCollector(ghClient, st, reg, cfg.GitHub, slog.Default())
		go collector.Run(rootCtx)
		log.Printf("efficiency collector started (org=%q, interval=%ds)",
			cfg.GitHub.Org, cfg.GitHub.Scheduler.IntervalSeconds)
	} else {
		log.Printf("efficiency collector disabled — set %s and github.org to enable",
			cfg.GitHub.TokenEnv)
	}

	prefix := cfg.Kafka.ConsumerGroupPrefix
	if prefix == "" {
		prefix = "sentinel"
	}

	consumers := []struct {
		groupID string
		handler appkafka.Handler
	}{
		{prefix + ".threshold-checker", thresholdSvc.HandleEvent},
		{prefix + ".postgres-writer", persistSvc.HandleEvent},
		{prefix + ".github-trigger", githubTriggerHandler(st, reg, collector, slog.Default())},
		{prefix + ".signal-detector", signalDetector.HandleEvent},
	}

	var wg sync.WaitGroup
	// Reader and consumer will be instantiated in the same thread
	closers := make([]*appkafka.Consumer, 0, len(consumers))
	for _, c := range consumers {
		consumer := appkafka.NewConsumer(cfg.Kafka.Brokers, cfg.Kafka.Topic, c.groupID, slog.Default())
		closers = append(closers, consumer)
		wg.Add(1)
		go func(name string, h appkafka.Handler) {
			defer wg.Done()
			if err := consumer.Run(rootCtx, h); err != nil {
				log.Printf("consumer %s exited with error: %v", name, err)
			}
		}(c.groupID, c.handler)
	}

	log.Printf("sentinel-workers started (%d consumers on topic %q)", len(consumers), cfg.Kafka.Topic)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)
	<-quit

	log.Println("shutting down...")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = healthSrv.Shutdown(shutdownCtx)
	rootCancel()
	for _, c := range closers {
		_ = c.Close()
	}
	wg.Wait()
	log.Println("exited")
}

// githubTriggerHandler reacts to commit/pull-request OTel events by marking
// the engineer's github_username dirty in Redis and signaling the collector
// to run. The collector debounces, so a flood of events coalesces into at
// most one run per MinTriggerIntervalSeconds.
//
// If the collector is nil (token/org not configured), this is a no-op — we
// still drain the Kafka offset so messages don't pile up.
func githubTriggerHandler(
	st *store.Store,
	reg *registry.EngineerRegistry,
	collector *service.EfficiencyCollector,
	logger *slog.Logger,
) appkafka.Handler {
	return func(ctx context.Context, e store.Event) error {
		if e.MetricName != "claude_code.pull_request.count" &&
			e.MetricName != "claude_code.commit.count" {
			return nil
		}
		if collector == nil {
			return nil
		}
		if eng, ok := reg.LookupByEmail(e.EngineerID); ok && eng.GitHubUsername != "" {
			if err := st.MarkEngineerDirty(ctx, eng.GitHubUsername); err != nil {
				logger.Warn("mark dirty failed",
					slog.String("github", eng.GitHubUsername),
					slog.String("err", err.Error()))
			}
		}
		collector.Trigger()
		return nil
	}
}

func pingWithTimeout(parent context.Context, d time.Duration, ping func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(parent, d)
	defer cancel()
	return ping(ctx)
}
