package service

import (
	"context"
	"log/slog"
	"sort"
	"time"

	"enterprise-llm-tracker/internal/config"
	"enterprise-llm-tracker/internal/registry"
	"enterprise-llm-tracker/internal/store"
)

// EfficiencyRollup aggregates usage_events + github_prs into engineer_signals
// rows on a nightly schedule. Produces one row per (engineer, window) for the
// 1d/7d/30d/180d windows defined in efficiency.go.
//
// Idempotent: re-running the same window overwrites the same row, so a missed
// tick can be replayed safely.
type EfficiencyRollup struct {
	store    *store.Store
	registry *registry.EngineerRegistry
	cfg      config.SignalsConfig
	logger   *slog.Logger
}

func NewEfficiencyRollup(st *store.Store, reg *registry.EngineerRegistry, cfg config.SignalsConfig, logger *slog.Logger) *EfficiencyRollup {
	if logger == nil {
		logger = slog.Default()
	}
	return &EfficiencyRollup{store: st, registry: reg, cfg: cfg, logger: logger}
}

func (r *EfficiencyRollup) Run(ctx context.Context) {
	interval := time.Duration(r.cfg.EfficiencyRollupIntervalSeconds) * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	r.RunOnce(ctx, "startup")
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.RunOnce(ctx, "cron")
		}
	}
}

func (r *EfficiencyRollup) RunOnce(ctx context.Context, source string) {
	start := time.Now()
	engineers := r.registry.AllActive()
	r.logger.Info("efficiency rollup started",
		slog.String("source", source), slog.Int("engineers", len(engineers)))

	now := time.Now().UTC()

	for _, w := range efficiencyWindows {
		end := truncateToDay(now)
		windowStart := truncateToDay(now.AddDate(0, 0, -w.days))

		// Pass 1: write per-engineer rollup rows. Track DPR for the cohort
		// pass that fills in peer_percentile + team median.
		type dprEntry struct {
			email string
			dpr   float64
			ok    bool
		}
		dprs := make([]dprEntry, 0, len(engineers))

		for _, eng := range engineers {
			sig, dpr, ok, err := r.computeOne(ctx, eng, w.name, windowStart, end, now)
			if err != nil {
				r.logger.Warn("rollup compute failed",
					slog.String("engineer", eng.Email),
					slog.String("window", w.name),
					slog.String("err", err.Error()))
				continue
			}
			if err := r.store.UpsertEngineerSignal(ctx, sig); err != nil {
				r.logger.Warn("rollup upsert failed",
					slog.String("engineer", eng.Email),
					slog.String("window", w.name),
					slog.String("err", err.Error()))
				continue
			}
			dprs = append(dprs, dprEntry{eng.Email, dpr, ok})
		}

		// Pass 2: compute cohort median + percentiles, then back-fill.
		valid := make([]float64, 0, len(dprs))
		for _, d := range dprs {
			if d.ok {
				valid = append(valid, d.dpr)
			}
		}
		if len(valid) < 3 {
			// Too few peers for a meaningful percentile. Skip the back-fill;
			// peer_percentile + team_dollars_per_pr_median stay NULL.
			continue
		}
		median := medianOf(valid)
		for _, d := range dprs {
			if !d.ok {
				continue
			}
			pct := percentileRank(valid, d.dpr)
			if err := r.store.SetPeerPercentile(ctx, d.email, w.name, end, median, pct); err != nil {
				r.logger.Warn("peer percentile back-fill failed",
					slog.String("engineer", d.email),
					slog.String("window", w.name),
					slog.String("err", err.Error()))
			}
		}
	}

	r.logger.Info("efficiency rollup finished",
		slog.String("source", source), slog.Duration("duration", time.Since(start)))
}

// computeOne builds the EngineerSignal row for one (engineer, window).
// Returns (sig, dpr, hasMerges, err). hasMerges flags whether $/PR is
// defined (engineer had >0 merged-non-reverted PRs in the window) so the
// cohort pass knows whether to include this engineer in the median.
func (r *EfficiencyRollup) computeOne(
	ctx context.Context,
	eng registry.Engineer,
	windowName string,
	start, end, now time.Time,
) (store.EngineerSignal, float64, bool, error) {
	agg, err := r.store.EngineerUsageAggregate(ctx, eng.Email, start, end)
	if err != nil {
		return store.EngineerSignal{}, 0, false, err
	}

	var prCounts store.EngineerPRCounts
	if eng.GitHubUsername != "" {
		prCounts, err = r.store.PRCountsForEngineer(ctx, eng.GitHubUsername, start, end)
		if err != nil {
			return store.EngineerSignal{}, 0, false, err
		}
	}

	sig := store.EngineerSignal{
		EngineerID:        eng.Email,
		WindowName:        windowName,
		WindowStart:       start,
		WindowEnd:         end,
		CostUSD:           agg.CostUSD,
		LinesShipped:      agg.LinesShipped,
		PRsOpened:         prCounts.Opened,
		PRsMerged:         prCounts.Merged,
		PRsClosedUnmerged: prCounts.ClosedUnmerged,
		PRsReverted:       prCounts.Reverted,
		ModelMix:          modelMixFractions(agg.ModelCost, agg.CostUSD),
		ComputedAt:        now,
	}

	// cache_hit_ratio: cache_read / (cache_read + cache_creation)
	cacheTotal := agg.TokensCacheRead + agg.TokensCacheCreation
	if cacheTotal > 0 {
		ratio := float64(agg.TokensCacheRead) / float64(cacheTotal)
		sig.CacheHitRatio = &ratio
	}

	// dollars_per_kloc — only meaningful when lines_shipped is non-trivial.
	// Treat <100 lines as too noisy to bother reporting.
	if agg.LinesShipped >= 100 {
		dpk := agg.CostUSD / (float64(agg.LinesShipped) / 1000.0)
		sig.DollarsPerKLOC = &dpk
	}

	effectiveMerged := prCounts.Merged - prCounts.Reverted
	if effectiveMerged > 0 {
		dpr := agg.CostUSD / float64(effectiveMerged)
		return sig, dpr, true, nil
	}
	return sig, 0, false, nil
}

// modelMixFractions returns each model's share of total cost.
func modelMixFractions(byModel map[string]float64, total float64) map[string]float64 {
	if total <= 0 {
		return nil
	}
	out := make(map[string]float64, len(byModel))
	for m, c := range byModel {
		out[m] = c / total
	}
	return out
}

func medianOf(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	cp := append([]float64(nil), xs...)
	sort.Float64s(cp)
	mid := len(cp) / 2
	if len(cp)%2 == 1 {
		return cp[mid]
	}
	return (cp[mid-1] + cp[mid]) / 2
}

// percentileRank returns 0..100: the percentage of the cohort whose $/PR is
// strictly greater than or equal to this engineer's (i.e. lower number = more
// expensive than peers). Inverted so "low rank" reads as "good ranking on
// efficiency leaderboard."
func percentileRank(cohort []float64, value float64) int {
	if len(cohort) == 0 {
		return 0
	}
	better := 0
	for _, v := range cohort {
		if v >= value {
			better++
		}
	}
	return int(float64(better) * 100.0 / float64(len(cohort)))
}
