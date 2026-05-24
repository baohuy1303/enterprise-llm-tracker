package store

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

//go:embed *.lua
var luaScripts embed.FS

type Store struct {
	pg    *pgxpool.Pool
	rdb   *redis.Client

	incrFloat *redis.Script
	incrInt   *redis.Script
}

func New(pg *pgxpool.Pool, rdb *redis.Client) *Store {
	return &Store{
		pg:        pg,
		rdb:       rdb,
		incrFloat: redis.NewScript(mustLoadScript("incr_float_expire.lua")),
		incrInt:   redis.NewScript(mustLoadScript("incr_int_expire.lua")),
	}
}

func mustLoadScript(name string) string {
	content, err := luaScripts.ReadFile(name)
	if err != nil {
		panic(fmt.Sprintf("load lua script %s: %v", name, err))
	}
	return string(content)
}

func (s *Store) PingPG(ctx context.Context) error {
	return s.pg.Ping(ctx)
}

func (s *Store) PingRedis(ctx context.Context) error {
	return s.rdb.Ping(ctx).Err()
}

// WriteEvent inserts to usage_events and increments Redis cost/token counters.
// Used for the OTel metric stream (source="otel_metric").
func (s *Store) WriteEvent(ctx context.Context, e Event) error {
	raw, _ := json.Marshal(e.Raw)
	if _, err := s.pg.Exec(ctx, `
		INSERT INTO usage_events
		  (engineer_id, occurred_at, source, metric_name,
		   cost_usd, tokens_input, tokens_output, tokens_cache_read, tokens_cache_creation,
		   model, raw)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
	`, e.EngineerID, e.OccurredAt, e.Source, e.MetricName,
		e.CostUSD, e.TokensInput, e.TokensOutput, e.TokensCacheRead, e.TokensCacheCreation,
		e.Model, raw); err != nil {
		return fmt.Errorf("pg insert: %w", err)
	}

	now := time.Now().UTC()
	eod := endOfDayUTC(now).Unix()
	eom := endOfMonthUTC(now).Unix()

	if e.CostUSD != nil && *e.CostUSD > 0 {
		cost := *e.CostUSD
		if err := s.incrFloat.Run(ctx, s.rdb,
			[]string{costKey(e.EngineerID, "today")}, cost, eod).Err(); err != nil {
			return fmt.Errorf("redis cost:today: %w", err)
		}
		if err := s.incrFloat.Run(ctx, s.rdb,
			[]string{costKey(e.EngineerID, "month")}, cost, eom).Err(); err != nil {
			return fmt.Errorf("redis cost:month: %w", err)
		}
	}

	tokens := 0
	if e.TokensInput != nil {
		tokens += *e.TokensInput
	}
	if e.TokensOutput != nil {
		tokens += *e.TokensOutput
	}
	if e.TokensCacheRead != nil {
		tokens += *e.TokensCacheRead
	}
	if e.TokensCacheCreation != nil {
		tokens += *e.TokensCacheCreation
	}
	if tokens > 0 {
		if err := s.incrInt.Run(ctx, s.rdb,
			[]string{tokensKey(e.EngineerID, "today")}, tokens, eod).Err(); err != nil {
			return fmt.Errorf("redis tokens:today: %w", err)
		}
	}

	_ = s.rdb.Set(ctx, lastOtelKey(e.EngineerID), now.Format(time.RFC3339), 0).Err()
	return nil
}

// WriteLogEvent inserts to usage_events without touching Redis counters.
// Used for the OTel log stream (source="otel_event") to avoid double-counting
// with WriteEvent, which already owns the Redis increments for the metric stream.
func (s *Store) WriteLogEvent(ctx context.Context, e Event) error {
	raw, _ := json.Marshal(e.Raw)
	if _, err := s.pg.Exec(ctx, `
		INSERT INTO usage_events
		  (engineer_id, occurred_at, source, metric_name,
		   cost_usd, tokens_input, tokens_output, tokens_cache_read, tokens_cache_creation,
		   model, raw)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
	`, e.EngineerID, e.OccurredAt, e.Source, e.MetricName,
		e.CostUSD, e.TokensInput, e.TokensOutput, e.TokensCacheRead, e.TokensCacheCreation,
		e.Model, raw); err != nil {
		return fmt.Errorf("pg insert: %w", err)
	}
	return nil
}

// RebuildCounters reseeds today/month Redis counters from Postgres.
// Called on startup; safe to call when Redis already holds data (overwrites).
func (s *Store) RebuildCounters(ctx context.Context, emails []string, logger *slog.Logger) error {
	now := time.Now().UTC()
	eod := endOfDayUTC(now)
	eom := endOfMonthUTC(now)

	for _, id := range emails {
		var costToday float64
		var tokensToday int64
		err := s.pg.QueryRow(ctx, `
			SELECT
			  COALESCE(SUM(cost_usd), 0)::float8,
			  COALESCE(SUM(
			    COALESCE(tokens_input,0) + COALESCE(tokens_output,0) +
			    COALESCE(tokens_cache_read,0) + COALESCE(tokens_cache_creation,0)
			  ), 0)::bigint
			FROM usage_events
			WHERE engineer_id = $1
			  AND DATE(occurred_at AT TIME ZONE 'UTC') = DATE($2 AT TIME ZONE 'UTC')
		`, id, now).Scan(&costToday, &tokensToday)
		if err != nil {
			logger.Warn("rebuild today failed", slog.String("engineer", id), slog.String("err", err.Error()))
			continue
		}

		if costToday > 0 {
			if err := s.rdb.Set(ctx, costKey(id, "today"), costToday, time.Until(eod)).Err(); err != nil {
				logger.Warn("redis set cost:today", slog.String("engineer", id), slog.String("err", err.Error()))
			}
		}
		if tokensToday > 0 {
			if err := s.rdb.Set(ctx, tokensKey(id, "today"), tokensToday, time.Until(eod)).Err(); err != nil {
				logger.Warn("redis set tokens:today", slog.String("engineer", id), slog.String("err", err.Error()))
			}
		}

		var costMonth float64
		err = s.pg.QueryRow(ctx, `
			SELECT COALESCE(SUM(cost_usd), 0)::float8
			FROM usage_events
			WHERE engineer_id = $1
			  AND DATE_TRUNC('month', occurred_at AT TIME ZONE 'UTC')
			    = DATE_TRUNC('month', $2::timestamptz AT TIME ZONE 'UTC')
		`, id, now).Scan(&costMonth)
		if err != nil {
			logger.Warn("rebuild month failed", slog.String("engineer", id), slog.String("err", err.Error()))
			continue
		}
		if costMonth > 0 {
			if err := s.rdb.Set(ctx, costKey(id, "month"), costMonth, time.Until(eom)).Err(); err != nil {
				logger.Warn("redis set cost:month", slog.String("engineer", id), slog.String("err", err.Error()))
			}
		}
	}
	return nil
}

func costKey(email, period string) string {
	return fmt.Sprintf("engineer:%s:cost:%s", email, period)
}

func tokensKey(email, period string) string {
	return fmt.Sprintf("engineer:%s:tokens:%s", email, period)
}

func lastOtelKey(email string) string {
	return fmt.Sprintf("engineer:%s:last_otel_at", email)
}

func endOfDayUTC(t time.Time) time.Time {
	y, m, d := t.UTC().Date()
	return time.Date(y, m, d, 23, 59, 59, 0, time.UTC)
}

func endOfMonthUTC(t time.Time) time.Time {
	y, m, _ := t.UTC().Date()
	return time.Date(y, m+1, 1, 0, 0, 0, 0, time.UTC).Add(-time.Second)
}
