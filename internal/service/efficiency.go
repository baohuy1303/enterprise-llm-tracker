package service

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"enterprise-llm-tracker/internal/config"
	gh "enterprise-llm-tracker/internal/github"
	"enterprise-llm-tracker/internal/registry"
	"enterprise-llm-tracker/internal/store"
)

// efficiencyWindow is one of the leaderboard rollup windows.
type efficiencyWindow struct {
	name string
	days int
}

var efficiencyWindows = []efficiencyWindow{
	{name: "1d", days: 1},
	{name: "7d", days: 7},
	{name: "30d", days: 30},
	{name: "180d", days: 180},
}

// EfficiencyCollector orchestrates the GitHub fetch → revert detect →
// efficiency compute pipeline. One instance runs inside sentinel-workers.
//
// Trigger model:
//   - Hourly cron tick (safety net)
//   - Kafka github-trigger consumer calls Trigger() on PR/commit events;
//     triggers are debounced and coalesced (channel buffered to 1)
//   - HTTP /admin/refresh-efficiency also calls Trigger() and returns 202
//
// Concurrency: runMu guarantees only one collector pass at a time. If a
// trigger arrives during a run, the buffered channel slot ensures we run
// once more after the current pass finishes (no missed triggers).
type EfficiencyCollector struct {
	gh       *gh.Client
	store    *store.Store
	registry *registry.EngineerRegistry
	cfg      config.GitHubConfig
	logger   *slog.Logger

	triggerCh chan struct{}

	runMu     sync.Mutex
	lastRunAt time.Time
	lastRunMu sync.Mutex
}

func NewEfficiencyCollector(
	ghClient *gh.Client,
	st *store.Store,
	reg *registry.EngineerRegistry,
	cfg config.GitHubConfig,
	logger *slog.Logger,
) *EfficiencyCollector {
	if logger == nil {
		logger = slog.Default()
	}
	return &EfficiencyCollector{
		gh:        ghClient,
		store:     st,
		registry:  reg,
		cfg:       cfg,
		logger:    logger,
		triggerCh: make(chan struct{}, 1),
	}
}

// Trigger requests a collector run. Non-blocking: if a trigger is already
// queued, the new one coalesces into it. Safe to call from any goroutine.
func (c *EfficiencyCollector) Trigger() {
	select {
	case c.triggerCh <- struct{}{}:
	default:
	}
}

// Run is the collector's main loop. Blocks until ctx is cancelled.
// Runs once at startup, then on every cron tick and trigger signal (with
// debouncing so a flood of triggers doesn't hammer GitHub).
func (c *EfficiencyCollector) Run(ctx context.Context) {
	interval := time.Duration(c.cfg.Scheduler.IntervalSeconds) * time.Second
	debounce := time.Duration(c.cfg.Scheduler.MinTriggerIntervalSeconds) * time.Second

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Cross-binary trigger bus — sentinel-api publishes here from the
	// /admin/refresh-efficiency endpoint. We funnel incoming messages through
	// Trigger() so they go through the same debounce path as Kafka triggers.
	pubsubCh, closeSub := c.store.SubscribeCollectorTriggers(ctx)
	defer func() { _ = closeSub() }()
	go func() {
		for range pubsubCh {
			c.Trigger()
		}
	}()

	c.runOnce(ctx, "startup")

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.runOnce(ctx, "cron")
		case <-c.triggerCh:
			c.lastRunMu.Lock()
			since := time.Since(c.lastRunAt)
			c.lastRunMu.Unlock()
			if since < debounce {
				c.logger.Info("collector trigger debounced",
					slog.Duration("since_last_run", since),
					slog.Duration("debounce_window", debounce))
				continue
			}
			c.runOnce(ctx, "trigger")
		}
	}
}

func (c *EfficiencyCollector) runOnce(ctx context.Context, source string) {
	if !c.runMu.TryLock() {
		c.logger.Info("collector run already in progress, skipping",
			slog.String("source", source))
		return
	}
	defer c.runMu.Unlock()

	start := time.Now()
	c.logger.Info("collector run started", slog.String("source", source))

	// Drain dirty set so concurrent triggers during this run aren't lost — the
	// next trigger after this run will pick them up. (For now the dirty set is
	// informational; we always do a full org scan because the GitHub search
	// query is cheap and ensures we never miss revert detection for engineers
	// who weren't recently active.)
	dirty, _ := c.store.DrainDirtyEngineers(ctx)
	if len(dirty) > 0 {
		c.logger.Info("collector dirty engineers drained",
			slog.Int("count", len(dirty)))
	}

	if err := c.fetchAndStore(ctx); err != nil {
		c.logger.Error("collector fetch failed", slog.String("err", err.Error()))
		// fall through to efficiency compute anyway — we may have partial fresh
		// data plus the existing PR table, so the snapshots are still useful.
	}

	c.detectAndMarkReverts(ctx)
	c.computeSnapshots(ctx)

	c.lastRunMu.Lock()
	c.lastRunAt = time.Now()
	c.lastRunMu.Unlock()

	c.logger.Info("collector run finished",
		slog.String("source", source),
		slog.Duration("duration", time.Since(start)))
}

// fetchAndStore walks every active engineer in the registry and pulls their
// authored PRs from GitHub. UPSERTs into github_prs.
func (c *EfficiencyCollector) fetchAndStore(ctx context.Context) error {
	if c.cfg.Org == "" {
		return nil
	}
	since := time.Now().UTC().AddDate(0, 0, -c.cfg.LookbackDays)
	var lastErr error

	for _, login := range c.registry.AllGitHubUsernames() {
		if login == "" {
			continue
		}
		prs, err := c.gh.FetchAuthoredPRs(ctx, c.cfg.Org, login, since, c.cfg.IsUser)
		if err != nil {
			c.logger.Warn("fetch PRs failed",
				slog.String("login", login), slog.String("err", err.Error()))
			lastErr = err
			continue
		}
		for _, pr := range prs {
			if err := c.store.UpsertGitHubPR(ctx, pr); err != nil {
				c.logger.Warn("upsert PR failed",
					slog.String("repo", pr.Repo),
					slog.Int("pr", pr.PRNumber),
					slog.String("err", err.Error()))
				lastErr = err
			}
		}
		c.logger.Info("fetched PRs",
			slog.String("login", login), slog.Int("count", len(prs)))
	}

	// Mark each configured repo as synced — useful for ops visibility even
	// though the search query above is cross-repo.
	now := time.Now().UTC()
	for _, repo := range c.cfg.Repos {
		if lastErr != nil {
			_ = c.store.RecordRepoSyncErr(ctx, repo, lastErr.Error(), now)
		} else {
			_ = c.store.RecordRepoSyncOK(ctx, repo, now)
		}
	}
	return lastErr
}

// detectAndMarkReverts runs the revert heuristics on each configured repo's
// recent PR window and writes findings back to the DB.
func (c *EfficiencyCollector) detectAndMarkReverts(ctx context.Context) {
	since := time.Now().UTC().AddDate(0, 0, -c.cfg.LookbackDays)
	for _, repo := range c.cfg.Repos {
		prs, err := c.store.ListRecentPRsByRepo(ctx, repo, since)
		if err != nil {
			c.logger.Warn("revert scan list failed",
				slog.String("repo", repo), slog.String("err", err.Error()))
			continue
		}
		findings := gh.DetectReverts(prs)
		for _, f := range findings {
			if err := c.store.MarkPRReverted(ctx, f.OriginalRepo, f.OriginalPR, f.RevertedAt); err != nil {
				c.logger.Warn("mark reverted failed",
					slog.String("repo", f.OriginalRepo),
					slog.Int("pr", f.OriginalPR),
					slog.String("err", err.Error()))
				continue
			}
			c.logger.Info("pr marked reverted",
				slog.String("repo", f.OriginalRepo),
				slog.Int("original_pr", f.OriginalPR),
				slog.Int("reverting_pr", f.RevertingPR),
				slog.String("heuristic", f.Heuristic))
		}
	}
}

// computeSnapshots writes one EfficiencySnapshot per (engineer × window).
// $/PR uses the engineer's *email* as the cost lookup key (matches the rest
// of the system) and the engineer's *github_username* as the PR lookup key.
func (c *EfficiencyCollector) computeSnapshots(ctx context.Context) {
	now := time.Now().UTC()
	for _, eng := range c.registry.AllActive() {
		for _, w := range efficiencyWindows {
			windowEnd := now
			windowStart := now.AddDate(0, 0, -w.days)

			cost, err := c.store.CostForEngineerWindow(ctx, eng.Email, windowStart, windowEnd)
			if err != nil {
				c.logger.Warn("cost lookup failed",
					slog.String("engineer", eng.Email),
					slog.String("window", w.name),
					slog.String("err", err.Error()))
				continue
			}

			merged := 0
			reverted := 0
			if eng.GitHubUsername != "" {
				prs, err := c.store.ListMergedPRsByEngineerGitHub(ctx, eng.GitHubUsername, windowStart)
				if err != nil {
					c.logger.Warn("pr lookup failed",
						slog.String("github", eng.GitHubUsername),
						slog.String("err", err.Error()))
					continue
				}
				for _, pr := range prs {
					merged++
					if pr.Reverted {
						reverted++
					}
				}
			}

			var revertRate, dollarsPerPR float64
			effectiveMerged := merged - reverted
			if merged > 0 {
				revertRate = float64(reverted) / float64(merged)
			}
			if effectiveMerged > 0 {
				dollarsPerPR = cost / float64(effectiveMerged)
			}

			snap := store.EfficiencySnapshot{
				EngineerID:         eng.Email,
				WindowStart:        truncateToDay(windowStart),
				WindowEnd:          truncateToDay(windowEnd),
				CostUSD:            cost,
				MergedPRCount:      effectiveMerged,
				RevertRate:         revertRate,
				DollarsPerMergedPR: dollarsPerPR,
				ComputedAt:         now,
			}
			if err := c.store.WriteEfficiencySnapshot(ctx, snap); err != nil {
				c.logger.Warn("snapshot write failed",
					slog.String("engineer", eng.Email),
					slog.String("err", err.Error()))
			}
		}
	}
}

func truncateToDay(t time.Time) time.Time {
	y, m, d := t.UTC().Date()
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

// WindowForName resolves a leaderboard window string ("1d", "7d", "30d") to
// the (start, end) pair the Leaderboard query expects. Returns false for
// unknown windows.
func WindowForName(name string, now time.Time) (start, end time.Time, ok bool) {
	for _, w := range efficiencyWindows {
		if w.name == name {
			end = truncateToDay(now)
			start = truncateToDay(now.AddDate(0, 0, -w.days))
			return start, end, true
		}
	}
	return time.Time{}, time.Time{}, false
}
