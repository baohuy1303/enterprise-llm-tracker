package service

import (
	"context"
	"log/slog"
	"math"
	"sort"
	"time"

	"enterprise-llm-tracker/internal/config"
	"enterprise-llm-tracker/internal/registry"
	"enterprise-llm-tracker/internal/store"
)

// BaselineRebuilder rebuilds per-engineer baselines (daily mean/stddev,
// hourly p95, hour-of-day distribution) on a cron tick. Output goes to Redis
// under engineer:{email}:baseline:* keys; the SignalDetector reads it on every
// event for Z-score and rhythm-break checks.
//
// Runs once at startup, then every BaselineRebuildIntervalSeconds (default 24h).
type BaselineRebuilder struct {
	store    *store.Store
	registry *registry.EngineerRegistry
	cfg      config.SignalsConfig
	logger   *slog.Logger
}

func NewBaselineRebuilder(st *store.Store, reg *registry.EngineerRegistry, cfg config.SignalsConfig, logger *slog.Logger) *BaselineRebuilder {
	if logger == nil {
		logger = slog.Default()
	}
	return &BaselineRebuilder{store: st, registry: reg, cfg: cfg, logger: logger}
}

// Run blocks until ctx is cancelled. Calls RunOnce immediately, then on each tick.
func (b *BaselineRebuilder) Run(ctx context.Context) {
	interval := time.Duration(b.cfg.BaselineRebuildIntervalSeconds) * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	b.RunOnce(ctx, "startup")
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			b.RunOnce(ctx, "cron")
		}
	}
}

// RunOnce rebuilds the baseline for every active engineer. Errors per engineer
// are logged and skipped so one broken engineer doesn't stall the whole job.
func (b *BaselineRebuilder) RunOnce(ctx context.Context, source string) {
	start := time.Now()
	engineers := b.registry.AllActive()
	b.logger.Info("baseline rebuild started",
		slog.String("source", source), slog.Int("engineers", len(engineers)))

	var built int
	for _, eng := range engineers {
		bl, err := b.computeBaseline(ctx, eng.Email)
		if err != nil {
			b.logger.Warn("baseline compute failed",
				slog.String("engineer", eng.Email), slog.String("err", err.Error()))
			continue
		}
		if bl == nil {
			continue
		}
		if err := b.store.SaveBaseline(ctx, eng.Email, *bl); err != nil {
			b.logger.Warn("baseline save failed",
				slog.String("engineer", eng.Email), slog.String("err", err.Error()))
			continue
		}
		built++
	}

	b.logger.Info("baseline rebuild finished",
		slog.String("source", source),
		slog.Int("built", built),
		slog.Duration("duration", time.Since(start)))
}

// computeBaseline produces the EngineerBaseline for one engineer. Returns nil
// (no error) when the engineer has no usage data at all — we don't write empty
// baselines.
func (b *BaselineRebuilder) computeBaseline(ctx context.Context, email string) (*store.EngineerBaseline, error) {
	daily, err := b.store.DailySpendSeries(ctx, email, b.cfg.BaselineWindowDays)
	if err != nil {
		return nil, err
	}

	if len(daily) == 0 {
		return nil, nil
	}

	// Fill missing days as zeros so the mean reflects the full window — an
	// engineer who used Claude on 5 of 30 days has a mean closer to their
	// real burn rate, not just their high days.
	costs := make([]float64, b.cfg.BaselineWindowDays)
	for _, d := range daily {
		// Bucket by days-ago; clamp to window
		ago := int(time.Since(d.Date).Hours() / 24)
		if ago >= 0 && ago < b.cfg.BaselineWindowDays {
			costs[ago] = d.CostUSD
		}
	}
	mean, stddev := meanStddev(costs)

	hourly, err := b.store.HourlySpendSeries(ctx, email, b.cfg.HourlyBaselineWindowDays)
	if err != nil {
		return nil, err
	}
	p95 := percentile(hourly, 0.95)

	dist, err := b.store.HourOfDayDistribution(ctx, email, b.cfg.BaselineWindowDays)
	if err != nil {
		return nil, err
	}

	return &store.EngineerBaseline{
		DailyMean:        mean,
		DailyStddev:      stddev,
		HourlyP95:        p95,
		ActiveDays:       len(daily),
		HourDistribution: dist,
		RebuiltAt:        time.Now().UTC(),
	}, nil
}

func meanStddev(xs []float64) (mean, stddev float64) {
	if len(xs) == 0 {
		return 0, 0
	}
	var sum float64
	for _, x := range xs {
		sum += x
	}
	mean = sum / float64(len(xs))
	var ss float64
	for _, x := range xs {
		d := x - mean
		ss += d * d
	}
	stddev = math.Sqrt(ss / float64(len(xs)))
	return mean, stddev
}

// percentile returns the p-th percentile of xs (0..1). xs is sorted in-place.
// Returns 0 for an empty slice. Uses nearest-rank since hourly samples are coarse.
func percentile(xs []float64, p float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	sort.Float64s(xs)
	if p <= 0 {
		return xs[0]
	}
	if p >= 1 {
		return xs[len(xs)-1]
	}
	idx := int(math.Ceil(p*float64(len(xs)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(xs) {
		idx = len(xs) - 1
	}
	return xs[idx]
}
