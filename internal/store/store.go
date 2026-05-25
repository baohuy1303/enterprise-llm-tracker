package store

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

//go:embed *.lua
var luaScripts embed.FS

type Store struct {
	pg  *pgxpool.Pool
	rdb *redis.Client

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

// WriteEventRedis increments Redis cost/token counters for a metric event.
// No-ops for events without CostUSD or token deltas (e.g. log events).
// Used by the ingest hot path — must be fast and never block.
func (s *Store) WriteEventRedis(ctx context.Context, e Event) error {
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

// WriteEventPG inserts an event into usage_events. Idempotent via the event_id
// unique constraint: replaying the same event from Kafka is a no-op.
// Called by the Postgres writer consumer; not on the ingest hot path.
func (s *Store) WriteEventPG(ctx context.Context, e Event) error {
	raw, _ := json.Marshal(e.Raw)
	var eventID any
	if e.EventID != "" {
		eventID = e.EventID
	}
	if _, err := s.pg.Exec(ctx, `
		INSERT INTO usage_events
		  (event_id, engineer_id, occurred_at, source, metric_name,
		   cost_usd, tokens_input, tokens_output, tokens_cache_read, tokens_cache_creation,
		   model, raw)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
		ON CONFLICT (event_id) DO NOTHING
	`, eventID, e.EngineerID, e.OccurredAt, e.Source, e.MetricName,
		e.CostUSD, e.TokensInput, e.TokensOutput, e.TokensCacheRead, e.TokensCacheCreation,
		e.Model, raw); err != nil {
		return fmt.Errorf("pg insert: %w", err)
	}
	return nil
}

// GetCostCounter reads engineer:{email}:cost:{period} from Redis. Returns 0 if
// the key is missing (no spend yet today/this month).
func (s *Store) GetCostCounter(ctx context.Context, email, period string) (float64, error) {
	v, err := s.rdb.Get(ctx, costKey(email, period)).Float64()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return v, nil
}

// ClaimThresholdFired atomically claims the "fired" flag for an (engineer,
// period, pct) tuple. Returns true if this caller won the claim and should
// send the Slack DM; false if another caller already claimed it. The claim key
// expires at end of period so the next period starts fresh.
func (s *Store) ClaimThresholdFired(ctx context.Context, email, period string, pct int, expireAt time.Time) (bool, error) {
	key := thresholdFiredKey(email, period, pct)
	ttl := time.Until(expireAt)
	if ttl <= 0 {
		// Period already over — nothing to dedupe against. Skip the claim.
		return false, nil
	}
	ok, err := s.rdb.SetNX(ctx, key, "1", ttl).Result()
	if err != nil {
		return false, err
	}
	return ok, nil
}

// WriteThresholdTrigger appends a row to the threshold_triggers audit log.
func (s *Store) WriteThresholdTrigger(ctx context.Context, t ThresholdTrigger) error {
	var slackTS any
	if t.SlackMessageTS != "" {
		slackTS = t.SlackMessageTS
	}
	_, err := s.pg.Exec(ctx, `
		INSERT INTO threshold_triggers
		  (engineer_id, period, threshold_pct, triggered_at,
		   spend_at_trigger_usd, budget_usd, slack_message_ts, notified_manager)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
	`, t.EngineerID, t.Period, t.ThresholdPct, t.TriggeredAt,
		t.SpendAtTriggerUSD, t.BudgetUSD, slackTS, t.NotifiedManager)
	return err
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

func thresholdFiredKey(email, period string, pct int) string {
	return fmt.Sprintf("engineer:%s:threshold_fired:%s:%d", email, period, pct)
}

// EndOfDayUTC returns the last second of the current UTC day.
// Exported for use by the threshold service when computing claim TTLs.
func EndOfDayUTC(t time.Time) time.Time { return endOfDayUTC(t) }

// EndOfMonthUTC returns the last second of the current UTC month.
func EndOfMonthUTC(t time.Time) time.Time { return endOfMonthUTC(t) }

func endOfDayUTC(t time.Time) time.Time {
	y, m, d := t.UTC().Date()
	return time.Date(y, m, d, 23, 59, 59, 0, time.UTC)
}

func endOfMonthUTC(t time.Time) time.Time {
	y, m, _ := t.UTC().Date()
	return time.Date(y, m+1, 1, 0, 0, 0, 0, time.UTC).Add(-time.Second)
}
