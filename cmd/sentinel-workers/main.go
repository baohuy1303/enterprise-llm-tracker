package main

import (
	"context"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"

	"enterprise-llm-tracker/internal/config"
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
		{prefix + ".github-trigger", stubGitHubTrigger},
	}

	var wg sync.WaitGroup
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
	rootCancel()
	for _, c := range closers {
		_ = c.Close()
	}
	wg.Wait()
	log.Println("exited")
}

// stubGitHubTrigger is a placeholder for Stage 7 — for now, just log when
// commit/PR events flow by so we can verify the consumer wiring works.
func stubGitHubTrigger(_ context.Context, e store.Event) error {
	if e.MetricName == "claude_code.pull_request.count" || e.MetricName == "claude_code.commit.count" {
		slog.Default().Info("github_trigger_stub",
			slog.String("engineer", e.EngineerID),
			slog.String("metric", e.MetricName))
	}
	return nil
}

func pingWithTimeout(parent context.Context, d time.Duration, ping func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(parent, d)
	defer cancel()
	return ping(ctx)
}
