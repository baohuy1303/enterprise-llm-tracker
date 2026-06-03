package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"
)

// -----------------------------------------------------------------------------
// engineer_signals (efficiency rollups)
// -----------------------------------------------------------------------------

// UpsertEngineerSignal writes a per-engineer × per-window rollup. Re-runs of
// the same (engineer, window_name, window_end) overwrite — keeps the table
// bounded and recomputes idempotent.
func (s *Store) UpsertEngineerSignal(ctx context.Context, sig EngineerSignal) error {
	mixJSON, _ := json.Marshal(sig.ModelMix)
	_, err := s.pg.Exec(ctx, `
		INSERT INTO engineer_signals
		  (engineer_id, window_name, window_start, window_end,
		   prs_opened, prs_merged, prs_closed_unmerged, prs_reverted,
		   cost_usd, lines_shipped, cache_hit_ratio, dollars_per_kloc, model_mix,
		   team_dollars_per_pr_median, peer_percentile, computed_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
		ON CONFLICT (engineer_id, window_name, window_end) DO UPDATE SET
		  window_start              = EXCLUDED.window_start,
		  prs_opened                = EXCLUDED.prs_opened,
		  prs_merged                = EXCLUDED.prs_merged,
		  prs_closed_unmerged       = EXCLUDED.prs_closed_unmerged,
		  prs_reverted              = EXCLUDED.prs_reverted,
		  cost_usd                  = EXCLUDED.cost_usd,
		  lines_shipped             = EXCLUDED.lines_shipped,
		  cache_hit_ratio           = EXCLUDED.cache_hit_ratio,
		  dollars_per_kloc          = EXCLUDED.dollars_per_kloc,
		  model_mix                 = EXCLUDED.model_mix,
		  team_dollars_per_pr_median= EXCLUDED.team_dollars_per_pr_median,
		  peer_percentile           = EXCLUDED.peer_percentile,
		  computed_at               = EXCLUDED.computed_at
	`, sig.EngineerID, sig.WindowName, sig.WindowStart, sig.WindowEnd,
		sig.PRsOpened, sig.PRsMerged, sig.PRsClosedUnmerged, sig.PRsReverted,
		sig.CostUSD, sig.LinesShipped, sig.CacheHitRatio, sig.DollarsPerKLOC, mixJSON,
		sig.TeamDollarsPerPRMedian, sig.PeerPercentile, sig.ComputedAt)
	return err
}

// SetPeerPercentile back-fills the peer percentile + team median after all
// engineers' base rollups have been written for a given window.
func (s *Store) SetPeerPercentile(ctx context.Context, engineerID, windowName string, windowEnd time.Time, median float64, percentile int) error {
	_, err := s.pg.Exec(ctx, `
		UPDATE engineer_signals
		SET team_dollars_per_pr_median = $4, peer_percentile = $5
		WHERE engineer_id = $1 AND window_name = $2 AND window_end = $3
	`, engineerID, windowName, windowEnd, median, percentile)
	return err
}

// ListEngineerSignals returns the most recent rollup per engineer for the
// given window. Sorted by dollars-per-merged-PR ascending; engineers with no
// merges land at the bottom. Used by the manager dashboard.
func (s *Store) ListEngineerSignals(ctx context.Context, windowName string, windowEnd time.Time) ([]EngineerSignal, error) {
	rows, err := s.pg.Query(ctx, `
		SELECT engineer_id, window_name, window_start, window_end,
		       prs_opened, prs_merged, prs_closed_unmerged, prs_reverted,
		       cost_usd::float8, lines_shipped, cache_hit_ratio, dollars_per_kloc,
		       COALESCE(model_mix, '{}'::jsonb),
		       team_dollars_per_pr_median, peer_percentile, computed_at
		FROM engineer_signals
		WHERE window_name = $1 AND window_end = $2
		ORDER BY engineer_id
	`, windowName, windowEnd)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanEngineerSignals(rows)
}

// GetEngineerSignal returns the rollup for one engineer × window. Returns nil
// if no row exists yet (rollup hasn't run, or engineer has no activity).
func (s *Store) GetEngineerSignal(ctx context.Context, engineerID, windowName string, windowEnd time.Time) (*EngineerSignal, error) {
	row := s.pg.QueryRow(ctx, `
		SELECT engineer_id, window_name, window_start, window_end,
		       prs_opened, prs_merged, prs_closed_unmerged, prs_reverted,
		       cost_usd::float8, lines_shipped, cache_hit_ratio, dollars_per_kloc,
		       COALESCE(model_mix, '{}'::jsonb),
		       team_dollars_per_pr_median, peer_percentile, computed_at
		FROM engineer_signals
		WHERE engineer_id = $1 AND window_name = $2 AND window_end = $3
	`, engineerID, windowName, windowEnd)
	sig, err := scanOneEngineerSignal(row)
	if err != nil {
		if errors.Is(err, errNoSignalRow) {
			return nil, nil
		}
		return nil, err
	}
	return sig, nil
}

var errNoSignalRow = errors.New("no engineer_signal row")

func scanOneEngineerSignal(row interface{ Scan(...any) error }) (*EngineerSignal, error) {
	var sig EngineerSignal
	var mixJSON []byte
	var cacheRatio, dpkloc, teamMedian *float64
	var percentile *int
	err := row.Scan(
		&sig.EngineerID, &sig.WindowName, &sig.WindowStart, &sig.WindowEnd,
		&sig.PRsOpened, &sig.PRsMerged, &sig.PRsClosedUnmerged, &sig.PRsReverted,
		&sig.CostUSD, &sig.LinesShipped, &cacheRatio, &dpkloc,
		&mixJSON, &teamMedian, &percentile, &sig.ComputedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, errNoSignalRow
	}
	if err != nil {
		return nil, err
	}
	sig.CacheHitRatio = cacheRatio
	sig.DollarsPerKLOC = dpkloc
	sig.TeamDollarsPerPRMedian = teamMedian
	sig.PeerPercentile = percentile
	if len(mixJSON) > 0 {
		_ = json.Unmarshal(mixJSON, &sig.ModelMix)
	}
	return &sig, nil
}

func scanEngineerSignals(rows interface {
	Next() bool
	Scan(...any) error
	Err() error
}) ([]EngineerSignal, error) {
	var out []EngineerSignal
	for rows.Next() {
		sig, err := scanOneEngineerSignal(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *sig)
	}
	return out, rows.Err()
}

// -----------------------------------------------------------------------------
// signal_events (detected anomalies)
// -----------------------------------------------------------------------------

// WriteSignalEvent appends a detected anomaly. Caller is responsible for
// dedup via ClaimSignalFired before writing — this method always inserts.
func (s *Store) WriteSignalEvent(ctx context.Context, ev SignalEvent) (int64, error) {
	ctxJSON, _ := json.Marshal(ev.Context)
	var id int64
	err := s.pg.QueryRow(ctx, `
		INSERT INTO signal_events
		  (engineer_id, signal_type, severity, occurred_at,
		   observed_value, baseline_value, z_score, context, notified, notified_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
		RETURNING id
	`, ev.EngineerID, ev.SignalType, ev.Severity, ev.OccurredAt,
		ev.ObservedValue, ev.BaselineValue, ev.ZScore, ctxJSON,
		ev.Notified, ev.NotifiedAt).Scan(&id)
	return id, err
}

// MarkSignalNotified flips the notified flag after a Slack DM is sent.
func (s *Store) MarkSignalNotified(ctx context.Context, id int64, at time.Time) error {
	_, err := s.pg.Exec(ctx, `
		UPDATE signal_events SET notified = TRUE, notified_at = $2 WHERE id = $1
	`, id, at)
	return err
}

// ListSignalEvents returns events filtered by engineer (optional), signal type
// (optional), severity (optional), and a `since` timestamp. Newest first.
type SignalEventFilter struct {
	EngineerID string
	SignalType string
	Severity   string
	Since      time.Time
	Limit      int
}

func (s *Store) ListSignalEvents(ctx context.Context, f SignalEventFilter) ([]SignalEvent, error) {
	if f.Limit <= 0 || f.Limit > 1000 {
		f.Limit = 100
	}
	if f.Since.IsZero() {
		f.Since = time.Now().UTC().AddDate(0, 0, -7)
	}
	rows, err := s.pg.Query(ctx, `
		SELECT id, engineer_id, signal_type, severity, occurred_at,
		       observed_value, baseline_value, z_score,
		       COALESCE(context, '{}'::jsonb),
		       notified, notified_at
		FROM signal_events
		WHERE occurred_at >= $1
		  AND ($2 = '' OR engineer_id = $2)
		  AND ($3 = '' OR signal_type = $3)
		  AND ($4 = '' OR severity   = $4)
		ORDER BY occurred_at DESC
		LIMIT $5
	`, f.Since, f.EngineerID, f.SignalType, f.Severity, f.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []SignalEvent
	for rows.Next() {
		var ev SignalEvent
		var ctxJSON []byte
		if err := rows.Scan(&ev.ID, &ev.EngineerID, &ev.SignalType, &ev.Severity, &ev.OccurredAt,
			&ev.ObservedValue, &ev.BaselineValue, &ev.ZScore,
			&ctxJSON, &ev.Notified, &ev.NotifiedAt); err != nil {
			return nil, err
		}
		if len(ctxJSON) > 0 {
			_ = json.Unmarshal(ctxJSON, &ev.Context)
		}
		out = append(out, ev)
	}
	return out, rows.Err()
}

// ClaimSignalFired dedupes detection. Same (engineer, signal_type, severity,
// utc-day) gets one alert per day. Returns true if this caller won the claim.
func (s *Store) ClaimSignalFired(ctx context.Context, engineerID, signalType, severity string, day time.Time) (bool, error) {
	key := signalFiredKey(engineerID, signalType, severity, day)
	// Expire shortly after end of day to make sure same-day dedup holds.
	ttl := time.Until(endOfDayUTC(day)) + 1*time.Hour
	if ttl <= 0 {
		return false, nil
	}
	return s.rdb.SetNX(ctx, key, "1", ttl).Result()
}

func signalFiredKey(email, signalType, severity string, day time.Time) string {
	return fmt.Sprintf("engineer:%s:signal_fired:%s:%s:%s",
		email, signalType, severity, day.UTC().Format("2006-01-02"))
}

// -----------------------------------------------------------------------------
// Per-engineer baselines (Redis-cached, rebuilt nightly)
// -----------------------------------------------------------------------------

// SaveBaseline writes an engineer's baseline to Redis. 48h TTL — if the
// rebuilder skips an engineer (no activity), the keys age out cleanly.
func (s *Store) SaveBaseline(ctx context.Context, email string, b EngineerBaseline) error {
	ttl := 48 * time.Hour
	key := baselineKey(email)
	pipe := s.rdb.Pipeline()
	pipe.HSet(ctx, key, map[string]any{
		"daily_mean":   b.DailyMean,
		"daily_stddev": b.DailyStddev,
		"hourly_p95":   b.HourlyP95,
		"active_days":  b.ActiveDays,
		"rebuilt_at":   b.RebuiltAt.UTC().Format(time.RFC3339),
	})
	hourFields := make(map[string]any, 24)
	for h, frac := range b.HourDistribution {
		hourFields[fmt.Sprintf("h%d", h)] = frac
	}
	pipe.HSet(ctx, key, hourFields)
	pipe.Expire(ctx, key, ttl)
	_, err := pipe.Exec(ctx)
	return err
}

// LoadBaseline reads an engineer's baseline from Redis. Returns (nil, nil) if
// the engineer has no baseline yet (rebuilder hasn't run or they're new).
func (s *Store) LoadBaseline(ctx context.Context, email string) (*EngineerBaseline, error) {
	h, err := s.rdb.HGetAll(ctx, baselineKey(email)).Result()
	if err != nil {
		return nil, err
	}
	if len(h) == 0 {
		return nil, nil
	}
	b := &EngineerBaseline{}
	b.DailyMean, _ = strconv.ParseFloat(h["daily_mean"], 64)
	b.DailyStddev, _ = strconv.ParseFloat(h["daily_stddev"], 64)
	b.HourlyP95, _ = strconv.ParseFloat(h["hourly_p95"], 64)
	if v, err := strconv.Atoi(h["active_days"]); err == nil {
		b.ActiveDays = v
	}
	if t, err := time.Parse(time.RFC3339, h["rebuilt_at"]); err == nil {
		b.RebuiltAt = t
	}
	for i := 0; i < 24; i++ {
		if v, ok := h[fmt.Sprintf("h%d", i)]; ok {
			f, _ := strconv.ParseFloat(v, 64)
			b.HourDistribution[i] = f
		}
	}
	return b, nil
}

func baselineKey(email string) string {
	return fmt.Sprintf("engineer:%s:baseline", email)
}

// -----------------------------------------------------------------------------
// Burst-detection rolling window (Redis sorted set per engineer)
// -----------------------------------------------------------------------------

// BumpBurstWindow adds `cost` at `at` to the engineer's rolling spend zset,
// trims entries older than `window`, and returns the sum of the remaining
// window. Score = unix-nano timestamp so trimming is a ZREMRANGEBYSCORE.
func (s *Store) BumpBurstWindow(ctx context.Context, email string, cost float64, at time.Time, window time.Duration) (float64, error) {
	if cost <= 0 {
		return s.SumBurstWindow(ctx, email, at, window)
	}
	key := burstKey(email)
	cutoff := at.Add(-window).UnixNano()
	member := fmt.Sprintf("%d:%f", at.UnixNano(), cost)
	pipe := s.rdb.Pipeline()
	pipe.ZAdd(ctx, key, redis.Z{Score: float64(at.UnixNano()), Member: member})
	pipe.ZRemRangeByScore(ctx, key, "-inf", strconv.FormatInt(cutoff, 10))
	pipe.Expire(ctx, key, 2*window) // safety: drop the whole key if no activity
	zrange := pipe.ZRangeByScore(ctx, key, &redis.ZRangeBy{
		Min: strconv.FormatInt(cutoff, 10),
		Max: "+inf",
	})
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, err
	}
	return sumZMembers(zrange.Val()), nil
}

// SumBurstWindow returns the current rolling-window sum without modifying it.
func (s *Store) SumBurstWindow(ctx context.Context, email string, at time.Time, window time.Duration) (float64, error) {
	key := burstKey(email)
	cutoff := at.Add(-window).UnixNano()
	members, err := s.rdb.ZRangeByScore(ctx, key, &redis.ZRangeBy{
		Min: strconv.FormatInt(cutoff, 10),
		Max: "+inf",
	}).Result()
	if err != nil {
		return 0, err
	}
	return sumZMembers(members), nil
}

func sumZMembers(members []string) float64 {
	var sum float64
	for _, m := range members {
		// member format: "<unixnano>:<cost>"
		for i := len(m) - 1; i >= 0; i-- {
			if m[i] == ':' {
				if f, err := strconv.ParseFloat(m[i+1:], 64); err == nil {
					sum += f
				}
				break
			}
		}
	}
	return sum
}

func burstKey(email string) string {
	return fmt.Sprintf("engineer:%s:burst_window", email)
}

// -----------------------------------------------------------------------------
// Baseline-rebuild queries (read from usage_events)
// -----------------------------------------------------------------------------

// DailySpendSeries returns one (date, cost) row per UTC day for the last
// `days` days for one engineer. Days with no activity are omitted; caller
// treats them as zeros when computing mean/stddev across the full window.
func (s *Store) DailySpendSeries(ctx context.Context, email string, days int) ([]DailyUsage, error) {
	return s.UsageHistory(ctx, email, days)
}

// HourlySpendSeries returns (hour-bucket-start, cost) rows over `days` days.
// Used to compute the engineer's hourly p95 for burst detection.
func (s *Store) HourlySpendSeries(ctx context.Context, email string, days int) ([]float64, error) {
	rows, err := s.pg.Query(ctx, `
		SELECT COALESCE(SUM(cost_usd), 0)::float8 AS hour_cost
		FROM usage_events
		WHERE engineer_id = $1
		  AND occurred_at >= NOW() - ($2::int * INTERVAL '1 day')
		  AND cost_usd IS NOT NULL
		GROUP BY DATE_TRUNC('hour', occurred_at AT TIME ZONE 'UTC')
		ORDER BY hour_cost
	`, email, days)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []float64
	for rows.Next() {
		var v float64
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// HourOfDayDistribution returns the fraction of activity (by event count)
// landing in each UTC hour 0..23 over the last `days` days. Used by the
// rhythm-break detector.
func (s *Store) HourOfDayDistribution(ctx context.Context, email string, days int) ([24]float64, error) {
	var dist [24]float64
	rows, err := s.pg.Query(ctx, `
		SELECT EXTRACT(HOUR FROM occurred_at AT TIME ZONE 'UTC')::int AS h,
		       COUNT(*)::bigint AS n
		FROM usage_events
		WHERE engineer_id = $1
		  AND occurred_at >= NOW() - ($2::int * INTERVAL '1 day')
		GROUP BY h
	`, email, days)
	if err != nil {
		return dist, err
	}
	defer rows.Close()
	var total int64
	var counts [24]int64
	for rows.Next() {
		var h int
		var n int64
		if err := rows.Scan(&h, &n); err != nil {
			return dist, err
		}
		if h >= 0 && h < 24 {
			counts[h] = n
			total += n
		}
	}
	if err := rows.Err(); err != nil {
		return dist, err
	}
	if total == 0 {
		return dist, nil
	}
	for i, c := range counts {
		dist[i] = float64(c) / float64(total)
	}
	return dist, nil
}

// -----------------------------------------------------------------------------
// Efficiency-rollup queries
// -----------------------------------------------------------------------------

// EngineerRollupAggregates is the raw numbers the efficiency-rollup job pulls
// per engineer × window from usage_events before computing derived ratios.
type EngineerRollupAggregates struct {
	CostUSD             float64
	LinesShipped        int64
	TokensInput         int64
	TokensOutput        int64
	TokensCacheRead     int64
	TokensCacheCreation int64
	ModelCost           map[string]float64 // cost in USD per model name
}

// EngineerUsageAggregate computes total cost/tokens/model-mix for one engineer
// in a half-open window [start, end). Used by the efficiency-rollup job.
func (s *Store) EngineerUsageAggregate(ctx context.Context, email string, start, end time.Time) (EngineerRollupAggregates, error) {
	var agg EngineerRollupAggregates
	agg.ModelCost = map[string]float64{}

	err := s.pg.QueryRow(ctx, `
		SELECT
		  COALESCE(SUM(cost_usd), 0)::float8,
		  COALESCE(SUM(tokens_input), 0)::bigint,
		  COALESCE(SUM(tokens_output), 0)::bigint,
		  COALESCE(SUM(tokens_cache_read), 0)::bigint,
		  COALESCE(SUM(tokens_cache_creation), 0)::bigint
		FROM usage_events
		WHERE engineer_id = $1
		  AND occurred_at >= $2 AND occurred_at < $3
	`, email, start, end).Scan(&agg.CostUSD, &agg.TokensInput, &agg.TokensOutput,
		&agg.TokensCacheRead, &agg.TokensCacheCreation)
	if err != nil {
		return agg, err
	}

	// lines-of-code is its own metric_name; sum the value out of `raw` if
	// present, otherwise treat as zero. OTel emits the raw value as the
	// metric value, which we don't persist on a dedicated column — we read
	// the `raw` JSONB for the `value` key if present.
	var lines int64
	err = s.pg.QueryRow(ctx, `
		SELECT COALESCE(COUNT(*), 0)::bigint
		FROM usage_events
		WHERE engineer_id = $1
		  AND occurred_at >= $2 AND occurred_at < $3
		  AND metric_name = 'claude_code.lines_of_code.count'
	`, email, start, end).Scan(&lines)
	if err == nil {
		agg.LinesShipped = lines
	}

	rows, err := s.pg.Query(ctx, `
		SELECT COALESCE(NULLIF(model, ''), 'unknown') AS m,
		       COALESCE(SUM(cost_usd), 0)::float8
		FROM usage_events
		WHERE engineer_id = $1
		  AND occurred_at >= $2 AND occurred_at < $3
		  AND cost_usd IS NOT NULL
		GROUP BY m
	`, email, start, end)
	if err != nil {
		return agg, err
	}
	defer rows.Close()
	for rows.Next() {
		var m string
		var c float64
		if err := rows.Scan(&m, &c); err != nil {
			return agg, err
		}
		agg.ModelCost[m] = c
	}
	return agg, rows.Err()
}

// PRCountsForEngineer returns the four PR counts for one engineer in a window,
// keyed by github_username. Counts use:
//   - opened: created_at in window
//   - merged: merged_at in window
//   - closed_unmerged: state='CLOSED' AND merged_at IS NULL AND created_at in window
//   - reverted: reverted=TRUE AND reverted_at in window (or merged_at if no revert ts)
type EngineerPRCounts struct {
	Opened         int
	Merged         int
	ClosedUnmerged int
	Reverted       int
}

func (s *Store) PRCountsForEngineer(ctx context.Context, githubUser string, start, end time.Time) (EngineerPRCounts, error) {
	var c EngineerPRCounts
	err := s.pg.QueryRow(ctx, `
		SELECT
		  COUNT(*) FILTER (WHERE created_at >= $2 AND created_at < $3),
		  COUNT(*) FILTER (WHERE merged_at  >= $2 AND merged_at  < $3),
		  COUNT(*) FILTER (WHERE state = 'CLOSED' AND merged_at IS NULL
		                    AND created_at >= $2 AND created_at < $3),
		  COUNT(*) FILTER (WHERE reverted = TRUE
		                    AND COALESCE(reverted_at, merged_at) >= $2
		                    AND COALESCE(reverted_at, merged_at) <  $3)
		FROM github_prs
		WHERE engineer_github = $1
	`, githubUser, start, end).Scan(&c.Opened, &c.Merged, &c.ClosedUnmerged, &c.Reverted)
	return c, err
}
