package main

import (
	"context"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"enterprise-llm-tracker/internal/config"
)

func main() {
	configPath := "sentinel.yaml"
	if v := os.Getenv("SENTINEL_CONFIG"); v != "" {
		configPath = v
	}

	days := 7
	if v := os.Getenv("ROLLUP_DAYS"); v != "" {
		if d, err := strconv.Atoi(v); err == nil && d > 0 {
			days = d
		}
	}

	cfg, err := config.Load(configPath)
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pg, err := pgxpool.New(ctx, cfg.Postgres.URL)
	if err != nil {
		log.Fatalf("postgres: %v", err)
	}
	defer pg.Close()
	if err := pg.Ping(ctx); err != nil {
		log.Fatalf("postgres ping: %v", err)
	}

	tag, err := pg.Exec(ctx, `
		INSERT INTO daily_rollups
		  (engineer_id, date, total_cost_usd, total_tokens)
		SELECT
		  engineer_id,
		  DATE(occurred_at AT TIME ZONE 'UTC') AS date,
		  COALESCE(SUM(cost_usd), 0) AS total_cost_usd,
		  COALESCE(SUM(
		    COALESCE(tokens_input, 0) + COALESCE(tokens_output, 0) +
		    COALESCE(tokens_cache_read, 0) + COALESCE(tokens_cache_creation, 0)
		  ), 0) AS total_tokens
		FROM usage_events
		WHERE source = 'otel_metric'
		  AND occurred_at >= NOW() - make_interval(days => $1)
		GROUP BY engineer_id, DATE(occurred_at AT TIME ZONE 'UTC')
		ON CONFLICT (engineer_id, date) DO UPDATE SET
		  total_cost_usd = EXCLUDED.total_cost_usd,
		  total_tokens   = EXCLUDED.total_tokens
	`, days)
	if err != nil {
		log.Fatalf("rollup: %v", err)
	}
	log.Printf("rollup complete: days=%d rows=%d", days, tag.RowsAffected())
}
