package service

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"enterprise-llm-tracker/internal/config"
	"enterprise-llm-tracker/internal/registry"
	"enterprise-llm-tracker/internal/slack"
	"enterprise-llm-tracker/internal/store"
)

const (
	SignalBurst       = "burst"
	SignalZScoreHigh  = "spend_zscore_high"
	SignalRhythmBreak = "rhythm_break"

	SeverityInfo     = "info"
	SeverityWarn     = "warn"
	SeverityCritical = "critical"
)

// SignalDetector is the Kafka consumer that emits per-event detection signals
// (burst, daily-spend Z-score, rhythm break). One instance runs in
// sentinel-workers behind its own consumer group, so it scales independently
// of the threshold-checker and postgres-writer.
//
// Per-event work is cheap: 1–2 Redis reads + at most a SET, then a write to
// signal_events only when something fires. Dedup keys keep the same signal
// from firing twice per UTC day for one engineer.
type SignalDetector struct {
	store    *store.Store
	registry *registry.EngineerRegistry
	slack    *slack.Client
	cfg      config.SignalsConfig
	logger   *slog.Logger
}

func NewSignalDetector(
	st *store.Store,
	reg *registry.EngineerRegistry,
	sl *slack.Client,
	cfg config.SignalsConfig,
	logger *slog.Logger,
) *SignalDetector {
	if logger == nil {
		logger = slog.Default()
	}
	return &SignalDetector{store: st, registry: reg, slack: sl, cfg: cfg, logger: logger}
}

// HandleEvent is the Kafka consumer entry point. Always returns nil so the
// offset commits — detection failures are logged, never replayed (replay would
// re-fire alerts on rehydration).
func (d *SignalDetector) HandleEvent(ctx context.Context, e store.Event) error {
	eng, ok := d.registry.LookupByEmail(e.EngineerID)
	if !ok {
		return nil
	}

	// Burst detection runs on every event with non-zero cost — cheapest signal
	// and most actionable (real-time spike).
	if e.CostUSD != nil && *e.CostUSD > 0 {
		d.checkBurst(ctx, eng, *e.CostUSD, e.OccurredAt, e.Model)
	}

	// Z-score and rhythm-break depend on the engineer having enough baseline
	// history to be meaningful — skip new hires.
	baseline, err := d.store.LoadBaseline(ctx, eng.Email)
	if err != nil {
		d.logger.Warn("baseline load failed",
			slog.String("engineer", eng.Email), slog.String("err", err.Error()))
		return nil
	}
	if baseline == nil || baseline.ActiveDays < d.cfg.MinBaselineDays {
		return nil
	}

	d.checkRhythmBreak(ctx, eng, e.OccurredAt, baseline)
	d.checkZScore(ctx, eng, baseline, e.OccurredAt)
	return nil
}

// checkBurst examines the engineer's last `BurstWindowMinutes` of spend and
// fires a signal when the rolling sum exceeds `BurstMultiplier * hourly_p95`.
// Critical at 1.5× the warn threshold.
func (d *SignalDetector) checkBurst(ctx context.Context, eng *registry.Engineer, cost float64, at time.Time, model string) {
	window := time.Duration(d.cfg.BurstWindowMinutes) * time.Minute
	rolling, err := d.store.BumpBurstWindow(ctx, eng.Email, cost, at, window)
	if err != nil {
		d.logger.Warn("burst window bump failed",
			slog.String("engineer", eng.Email), slog.String("err", err.Error()))
		return
	}

	// Need a baseline p95 to compare against. If no baseline yet, we can't
	// distinguish "high for them" from "normal heavy day."
	baseline, err := d.store.LoadBaseline(ctx, eng.Email)
	if err != nil || baseline == nil || baseline.HourlyP95 <= 0 {
		return
	}

	warnThreshold := d.cfg.BurstMultiplier * baseline.HourlyP95
	criticalThreshold := 1.5 * warnThreshold
	if rolling < warnThreshold {
		return
	}

	severity := SeverityWarn
	if rolling >= criticalThreshold {
		severity = SeverityCritical
	}

	d.fire(ctx, eng, SignalBurst, severity, at, rolling, baseline.HourlyP95, nil, map[string]any{
		"window_minutes": d.cfg.BurstWindowMinutes,
		"model":          model,
	})
}

// checkZScore reads today's Redis cost counter and compares against the
// engineer's 30-day daily mean/stddev. Fires at >=ZScoreWarn σ above mean.
func (d *SignalDetector) checkZScore(ctx context.Context, eng *registry.Engineer, baseline *store.EngineerBaseline, at time.Time) {
	if baseline.DailyStddev <= 0 {
		return // not enough variance to compute a Z-score
	}
	today, err := d.store.GetCostCounter(ctx, eng.Email, "today")
	if err != nil || today <= 0 {
		return
	}
	z := (today - baseline.DailyMean) / baseline.DailyStddev
	if z < d.cfg.ZScoreWarn {
		return
	}
	severity := SeverityWarn
	if z >= d.cfg.ZScoreCritical {
		severity = SeverityCritical
	} else if z >= (d.cfg.ZScoreWarn+d.cfg.ZScoreCritical)/2 {
		// midpoint between warn and critical = "info" upgraded to "warn"
		severity = SeverityWarn
	}
	zCopy := z
	d.fire(ctx, eng, SignalZScoreHigh, severity, at, today, baseline.DailyMean, &zCopy, map[string]any{
		"baseline_window_days": d.cfg.BaselineWindowDays,
	})
}

// checkRhythmBreak flags events occurring in an hour-of-day bucket holding
// less than RhythmBreakPct of the engineer's historical activity. Severity
// is graded by how cold the bucket is.
func (d *SignalDetector) checkRhythmBreak(ctx context.Context, eng *registry.Engineer, at time.Time, baseline *store.EngineerBaseline) {
	hour := at.UTC().Hour()
	frac := baseline.HourDistribution[hour]
	if frac == 0 || frac >= d.cfg.RhythmBreakPct {
		return
	}
	severity := SeverityInfo
	switch {
	case frac < 0.01:
		severity = SeverityCritical
	case frac < 0.02:
		severity = SeverityWarn
	}
	fracCopy := frac
	d.fire(ctx, eng, SignalRhythmBreak, severity, at, fracCopy, d.cfg.RhythmBreakPct, nil, map[string]any{
		"hour_utc": hour,
	})
}

// fire claims the dedup key, writes a signal_events row, and (for burst-critical)
// sends a Slack DM. Idempotent — second call same day is a no-op via dedup.
func (d *SignalDetector) fire(
	ctx context.Context,
	eng *registry.Engineer,
	signalType, severity string,
	at time.Time,
	observed, baselineVal float64,
	zScore *float64,
	context map[string]any,
) {
	claimed, err := d.store.ClaimSignalFired(ctx, eng.Email, signalType, severity, at)
	if err != nil {
		d.logger.Warn("signal claim failed",
			slog.String("engineer", eng.Email),
			slog.String("signal", signalType),
			slog.String("err", err.Error()))
		return
	}
	if !claimed {
		return
	}

	ev := store.SignalEvent{
		EngineerID:    eng.Email,
		SignalType:    signalType,
		Severity:      severity,
		OccurredAt:    at,
		ObservedValue: &observed,
		BaselineValue: &baselineVal,
		ZScore:        zScore,
		Context:       context,
	}
	id, err := d.store.WriteSignalEvent(ctx, ev)
	if err != nil {
		d.logger.Warn("signal write failed",
			slog.String("engineer", eng.Email),
			slog.String("signal", signalType),
			slog.String("err", err.Error()))
		return
	}

	d.logger.Info("signal fired",
		slog.String("engineer", eng.Email),
		slog.String("signal", signalType),
		slog.String("severity", severity),
		slog.Float64("observed", observed),
		slog.Float64("baseline", baselineVal),
	)

	// Slack DM only for burst-critical (the plan's loudest alert tier).
	if d.cfg.NotifyBurstCritical && signalType == SignalBurst && severity == SeverityCritical {
		d.notifyBurst(ctx, eng, observed, baselineVal, id)
	}
}

func (d *SignalDetector) notifyBurst(ctx context.Context, eng *registry.Engineer, observed, baseline float64, signalID int64) {
	if !d.slack.Configured() {
		return
	}
	notifiedAt := time.Now().UTC()
	engineerMsg := fmt.Sprintf(
		"🚨 Burst alert: $%.2f spent in the last %d min — that's %.1fx your normal hourly rate ($%.2f/hr). Check whether something's running away.",
		observed, d.cfg.BurstWindowMinutes, observed/baseline, baseline,
	)
	managerMsg := fmt.Sprintf(
		"%s just burned $%.2f in %d min (%.1fx their normal hourly rate). Worth a check.",
		eng.Name, observed, d.cfg.BurstWindowMinutes, observed/baseline,
	)

	sentAny := false
	if eng.SlackUserID != "" {
		if _, err := d.slack.PostDM(ctx, eng.SlackUserID, engineerMsg); err != nil {
			d.logger.Error("burst DM to engineer failed",
				slog.String("engineer", eng.Email), slog.String("err", err.Error()))
		} else {
			sentAny = true
		}
	}
	if eng.ManagerSlackID != "" {
		if _, err := d.slack.PostDM(ctx, eng.ManagerSlackID, managerMsg); err != nil {
			d.logger.Error("burst DM to manager failed",
				slog.String("engineer", eng.Email),
				slog.String("manager_slack_id", eng.ManagerSlackID),
				slog.String("err", err.Error()))
		} else {
			sentAny = true
		}
	}
	if sentAny {
		if err := d.store.MarkSignalNotified(ctx, signalID, notifiedAt); err != nil {
			d.logger.Warn("mark signal notified failed",
				slog.Int64("id", signalID), slog.String("err", err.Error()))
		}
	}
}
