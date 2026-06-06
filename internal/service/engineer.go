package service

import (
	"context"
	"errors"
	"log/slog"
	"net/mail"
	"strings"
	"time"

	"enterprise-llm-tracker/internal/registry"
	"enterprise-llm-tracker/internal/store"
)

// EngineerService is the CRUD-and-aggregation layer for the engineers
// resource. It wraps the store with input validation, registry-cache
// invalidation, and read-side joins (Redis spend counters, usage history,
// recent threshold triggers, latest efficiency snapshot).
type EngineerService struct {
	store    *store.Store
	registry *registry.EngineerRegistry
	logger   *slog.Logger
}

func NewEngineerService(st *store.Store, reg *registry.EngineerRegistry, logger *slog.Logger) *EngineerService {
	if logger == nil {
		logger = slog.Default()
	}
	return &EngineerService{store: st, registry: reg, logger: logger}
}

// ErrValidation wraps a user-correctable input error. Handlers translate it
// to HTTP 400. Sentinel-other errors propagate as 500.
type ErrValidation struct{ Msg string }

func (e ErrValidation) Error() string { return e.Msg }

// EngineerWithUsage is the list-view model: persistent fields + live spend
// counters joined from Redis. Fits the dashboard table.
type EngineerWithUsage struct {
	store.Engineer
	CostTodayUSD float64    `json:"cost_today_usd"`
	CostMonthUSD float64    `json:"cost_month_usd"`
	LastOTelAt   *time.Time `json:"last_otel_at,omitempty"`
}

// EngineerDetail is the detail-view model: list view + 30-day history +
// recent threshold triggers + latest efficiency snapshot.
type EngineerDetail struct {
	EngineerWithUsage
	UsageHistory   []store.DailyUsage        `json:"usage_history"`
	RecentTriggers []store.ThresholdTrigger  `json:"recent_triggers"`
	EfficiencySnap *store.EfficiencySnapshot `json:"efficiency_snapshot,omitempty"`
}

// Create validates inputs and inserts a new engineer, then forces a registry
// refresh so the ingest hot path can attribute traffic to them immediately.
func (s *EngineerService) Create(ctx context.Context, in store.EngineerCreate) (*store.Engineer, error) {
	if err := validateCreate(&in); err != nil {
		return nil, err
	}
	eng, err := s.store.CreateEngineer(ctx, in)
	if err != nil {
		return nil, err
	}
	s.refreshRegistry(ctx, "create")
	return eng, nil
}

// List returns every active engineer joined with their live Redis spend.
// Spend lookups happen sequentially — fine for the expected scale (hundreds
// of engineers, not millions).
func (s *EngineerService) List(ctx context.Context) ([]EngineerWithUsage, error) {
	rows, err := s.store.ListEngineers(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]EngineerWithUsage, 0, len(rows))
	for _, e := range rows {
		out = append(out, s.attachUsage(ctx, e))
	}
	return out, nil
}

// Get returns the detail view for one engineer.
func (s *EngineerService) Get(ctx context.Context, email string) (*EngineerDetail, error) {
	eng, err := s.store.GetEngineerByEmail(ctx, email)
	if err != nil {
		return nil, err
	}
	base := s.attachUsage(ctx, *eng)

	history, err := s.store.UsageHistory(ctx, email, 30)
	if err != nil {
		s.logger.Warn("usage history failed", slog.String("email", email), slog.String("err", err.Error()))
	}
	triggers, err := s.store.ListRecentTriggers(ctx, email, 20)
	if err != nil {
		s.logger.Warn("recent triggers failed", slog.String("email", email), slog.String("err", err.Error()))
	}
	snap, err := s.store.LatestEfficiencySnapshot(ctx, email)
	if err != nil {
		s.logger.Warn("snapshot fetch failed", slog.String("email", email), slog.String("err", err.Error()))
	}

	return &EngineerDetail{
		EngineerWithUsage: base,
		UsageHistory:      history,
		RecentTriggers:    triggers,
		EfficiencySnap:    snap,
	}, nil
}

// Update applies a partial update and refreshes the registry so the new
// budgets/team/etc. are picked up immediately by the ingest path.
func (s *EngineerService) Update(ctx context.Context, email string, in store.EngineerUpdate) (*store.Engineer, error) {
	if err := validateUpdate(&in); err != nil {
		return nil, err
	}
	eng, err := s.store.UpdateEngineer(ctx, email, in)
	if err != nil {
		return nil, err
	}
	s.refreshRegistry(ctx, "update")
	return eng, nil
}

// Deactivate soft-deletes the engineer and refreshes the registry. The
// ingest path will stop attributing new events to them; historical data
// stays untouched.
func (s *EngineerService) Deactivate(ctx context.Context, email string) error {
	if err := s.store.DeactivateEngineer(ctx, email); err != nil {
		return err
	}
	s.refreshRegistry(ctx, "deactivate")
	return nil
}

// RefreshRegistry forces an immediate registry reload. Wired into the
// /admin/registry/refresh endpoint for bulk-onboarding scenarios.
func (s *EngineerService) RefreshRegistry(ctx context.Context) error {
	return s.registry.Load(ctx)
}

// attachUsage joins one Engineer row with their live Redis counters.
func (s *EngineerService) attachUsage(ctx context.Context, e store.Engineer) EngineerWithUsage {
	out := EngineerWithUsage{Engineer: e}
	if v, err := s.store.GetCostCounter(ctx, e.Email, "today"); err == nil {
		out.CostTodayUSD = v
	}
	if v, err := s.store.GetCostCounter(ctx, e.Email, "month"); err == nil {
		out.CostMonthUSD = v
	}
	return out
}

func (s *EngineerService) refreshRegistry(ctx context.Context, op string) {
	if err := s.registry.Load(ctx); err != nil {
		s.logger.Warn("registry refresh after mutation failed",
			slog.String("op", op), slog.String("err", err.Error()))
	}
}

// --- Validation -------------------------------------------------------------

func validateCreate(in *store.EngineerCreate) error {
	in.Email = strings.TrimSpace(strings.ToLower(in.Email))
	in.Name = strings.TrimSpace(in.Name)
	in.GitHubUsername = strings.TrimSpace(in.GitHubUsername)
	in.Team = strings.TrimSpace(in.Team)
	in.SlackUserID = strings.TrimSpace(in.SlackUserID)
	in.ManagerSlackID = strings.TrimSpace(in.ManagerSlackID)

	if _, err := mail.ParseAddress(in.Email); err != nil {
		return ErrValidation{Msg: "email is required and must be a valid address"}
	}
	if in.Name == "" {
		return ErrValidation{Msg: "name is required"}
	}
	if in.GitHubUsername == "" {
		return ErrValidation{Msg: "github_username is required"}
	}
	if in.DailyBudgetUSD < 0 || in.MonthlyBudgetUSD < 0 {
		return ErrValidation{Msg: "budgets must be non-negative"}
	}
	return nil
}

func validateUpdate(in *store.EngineerUpdate) error {
	// trim whitespace on any string fields the caller provided
	if in.Name != nil {
		v := strings.TrimSpace(*in.Name)
		if v == "" {
			return ErrValidation{Msg: "name cannot be set to empty"}
		}
		in.Name = &v
	}
	if in.GitHubUsername != nil {
		v := strings.TrimSpace(*in.GitHubUsername)
		if v == "" {
			return ErrValidation{Msg: "github_username cannot be set to empty"}
		}
		in.GitHubUsername = &v
	}
	if in.SlackUserID != nil {
		v := strings.TrimSpace(*in.SlackUserID)
		in.SlackUserID = &v
	}
	if in.ManagerSlackID != nil {
		v := strings.TrimSpace(*in.ManagerSlackID)
		in.ManagerSlackID = &v
	}
	if in.Team != nil {
		v := strings.TrimSpace(*in.Team)
		in.Team = &v
	}
	if in.DailyBudgetUSD != nil && *in.DailyBudgetUSD < 0 {
		return ErrValidation{Msg: "daily_budget_usd must be non-negative"}
	}
	if in.MonthlyBudgetUSD != nil && *in.MonthlyBudgetUSD < 0 {
		return ErrValidation{Msg: "monthly_budget_usd must be non-negative"}
	}
	return nil
}

// IsValidation reports whether err is a service-layer validation error.
// Used by handlers to pick the right HTTP status code.
func IsValidation(err error) bool {
	var v ErrValidation
	return errors.As(err, &v)
}
