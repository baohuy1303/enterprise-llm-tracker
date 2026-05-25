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

type ThresholdService struct {
	store    *store.Store
	registry *registry.EngineerRegistry
	slack    *slack.Client
	cfg      config.ThresholdsConfig
	logger   *slog.Logger
}

func NewThresholdService(
	st *store.Store,
	reg *registry.EngineerRegistry,
	sl *slack.Client,
	cfg config.ThresholdsConfig,
	logger *slog.Logger,
) *ThresholdService {
	if logger == nil {
		logger = slog.Default()
	}
	return &ThresholdService{
		store:    st,
		registry: reg,
		slack:    sl,
		cfg:      cfg,
		logger:   logger,
	}
}

// HandleEvent inspects the engineer's current daily/monthly Redis counters and
// fires a Slack DM + audit row for any newly-crossed threshold. Returns nil
// even if Slack delivery fails — the message is logged but the offset is still
// committed, since redelivery wouldn't help (Slack outage, not a transient
// consumer error).
func (t *ThresholdService) HandleEvent(ctx context.Context, e store.Event) error {
	eng, ok := t.registry.LookupByEmail(e.EngineerID)
	if !ok {
		// Unknown engineer (likely unattributed event); nothing to threshold.
		return nil
	}

	now := time.Now().UTC()
	t.checkPeriod(ctx, eng, "daily", eng.DailyBudgetUSD, store.EndOfDayUTC(now))
	t.checkPeriod(ctx, eng, "monthly", eng.MonthlyBudgetUSD, store.EndOfMonthUTC(now))
	return nil
}

func (t *ThresholdService) checkPeriod(ctx context.Context, eng *registry.Engineer, period string, budget float64, expireAt time.Time) {
	if budget <= 0 {
		return
	}
	redisPeriod := "today"
	if period == "monthly" {
		redisPeriod = "month"
	}

	spend, err := t.store.GetCostCounter(ctx, eng.Email, redisPeriod)
	if err != nil {
		t.logger.Error("threshold: read counter",
			slog.String("engineer", eng.Email),
			slog.String("period", period),
			slog.String("err", err.Error()))
		return
	}
	if spend <= 0 {
		return
	}

	pct := (spend / budget) * 100

	for _, threshold := range t.cfg.NotifyAtPercent {
		if pct < float64(threshold) {
			continue
		}

		claimed, err := t.store.ClaimThresholdFired(ctx, eng.Email, period, threshold, expireAt)
		if err != nil {
			t.logger.Error("threshold: claim",
				slog.String("engineer", eng.Email),
				slog.String("period", period),
				slog.Int("pct", threshold),
				slog.String("err", err.Error()))
			continue
		}
		if !claimed {
			continue // already fired this period
		}

		t.fireThreshold(ctx, eng, period, threshold, spend, budget)
	}
}

func (t *ThresholdService) fireThreshold(ctx context.Context, eng *registry.Engineer, period string, threshold int, spend, budget float64) {
	notifyManager := false
	for _, p := range t.cfg.NotifyManagerAtPercent {
		if p == threshold {
			notifyManager = true
			break
		}
	}

	engineerMsg := formatEngineerMessage(eng.Name, period, threshold, spend, budget)

	var ts string
	if t.slack.Configured() && eng.SlackUserID != "" {
		var err error
		ts, err = t.slack.PostDM(ctx, eng.SlackUserID, engineerMsg)
		if err != nil {
			t.logger.Error("threshold: slack DM failed",
				slog.String("engineer", eng.Email),
				slog.String("err", err.Error()))
		} else {
			t.logger.Info("threshold: slack DM sent",
				slog.String("engineer", eng.Email),
				slog.String("period", period),
				slog.Int("pct", threshold),
				slog.String("ts", ts))
		}
	} else {
		t.logger.Info("threshold_triggered",
			slog.String("engineer", eng.Email),
			slog.String("period", period),
			slog.Int("pct", threshold),
			slog.Float64("spend", spend),
			slog.Float64("budget", budget),
			slog.String("note", "slack not configured or user id missing"))
	}

	if notifyManager && t.slack.Configured() && eng.ManagerSlackID != "" {
		managerMsg := formatManagerMessage(eng.Name, period, threshold, spend, budget)
		if _, err := t.slack.PostDM(ctx, eng.ManagerSlackID, managerMsg); err != nil {
			t.logger.Error("threshold: manager slack DM failed",
				slog.String("engineer", eng.Email),
				slog.String("manager_slack_id", eng.ManagerSlackID),
				slog.String("err", err.Error()))
			notifyManager = false
		}
	}

	if err := t.store.WriteThresholdTrigger(ctx, store.ThresholdTrigger{
		EngineerID:        eng.Email,
		Period:            period,
		ThresholdPct:      threshold,
		TriggeredAt:       time.Now().UTC(),
		SpendAtTriggerUSD: spend,
		BudgetUSD:         budget,
		SlackMessageTS:    ts,
		NotifiedManager:   notifyManager,
	}); err != nil {
		t.logger.Error("threshold: audit write failed",
			slog.String("engineer", eng.Email),
			slog.String("err", err.Error()))
	}
}

func formatEngineerMessage(name, period string, pct int, spend, budget float64) string {
	periodLabel := "today's"
	if period == "monthly" {
		periodLabel = "this month's"
	}
	return fmt.Sprintf("Heads up %s — you've hit %d%% of %s $%.2f Claude budget ($%.2f spent).",
		name, pct, periodLabel, budget, spend)
}

func formatManagerMessage(name, period string, pct int, spend, budget float64) string {
	periodLabel := "daily"
	if period == "monthly" {
		periodLabel = "monthly"
	}
	return fmt.Sprintf("%s just hit %d%% of their $%.2f %s Claude budget ($%.2f spent). Worth a check-in.",
		name, pct, budget, periodLabel, spend)
}
