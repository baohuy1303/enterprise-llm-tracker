package store

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// Engineer mirrors the engineers row. Distinct from registry.Engineer so the
// store layer stays free of registry-package imports — the service layer
// translates between the two.
type Engineer struct {
	ID               string    `json:"id"`
	Email            string    `json:"email"`
	Name             string    `json:"name"`
	GitHubUsername   string    `json:"github_username"`
	SlackUserID      string    `json:"slack_user_id"`
	ManagerSlackID   string    `json:"manager_slack_id"`
	DailyBudgetUSD   float64   `json:"daily_budget_usd"`
	MonthlyBudgetUSD float64   `json:"monthly_budget_usd"`
	Team             string    `json:"team"`
	Active           bool      `json:"active"`
	CreatedAt        time.Time `json:"created_at"`
	UpdatedAt        time.Time `json:"updated_at"`
}

// EngineerCreate is the input for CreateEngineer. All non-budget fields are
// required; budgets fall back to the table defaults when zero.
type EngineerCreate struct {
	Email            string
	Name             string
	GitHubUsername   string
	SlackUserID      string
	ManagerSlackID   string
	DailyBudgetUSD   float64
	MonthlyBudgetUSD float64
	Team             string
}

// EngineerUpdate is a partial update — nil fields are left untouched. Using
// pointers lets us distinguish "not provided" from "set to zero value".
type EngineerUpdate struct {
	Name             *string
	GitHubUsername   *string
	SlackUserID      *string
	ManagerSlackID   *string
	DailyBudgetUSD   *float64
	MonthlyBudgetUSD *float64
	Team             *string
}

// ErrEngineerNotFound is returned by store methods when no row matches.
// Service layer translates this into HTTP 404.
var ErrEngineerNotFound = errors.New("engineer not found")

// ErrEngineerExists is returned on unique-constraint violations during create.
// Service layer translates this into HTTP 409.
var ErrEngineerExists = errors.New("engineer already exists")

// CreateEngineer inserts a new row and returns the persisted record (with
// generated ID + timestamps). Duplicate email or github_username yields
// ErrEngineerExists.
func (s *Store) CreateEngineer(ctx context.Context, in EngineerCreate) (*Engineer, error) {
	const q = `
		INSERT INTO engineers
		  (email, name, github_username, slack_user_id, manager_slack_id,
		   daily_budget_usd, monthly_budget_usd, team, active)
		VALUES ($1,$2,$3, NULLIF($4,''), NULLIF($5,''),
		        COALESCE(NULLIF($6, 0), 25.00),
		        COALESCE(NULLIF($7, 0), 500.00),
		        NULLIF($8,''), TRUE)
		RETURNING id::text, email, name, github_username,
		          COALESCE(slack_user_id,''), COALESCE(manager_slack_id,''),
		          daily_budget_usd, monthly_budget_usd, COALESCE(team,''),
		          active, created_at, updated_at
	`
	row := s.pg.QueryRow(ctx, q,
		in.Email, in.Name, in.GitHubUsername, in.SlackUserID, in.ManagerSlackID,
		in.DailyBudgetUSD, in.MonthlyBudgetUSD, in.Team)
	e, err := scanEngineer(row)
	if err != nil {
		if isUniqueViolation(err) {
			return nil, ErrEngineerExists
		}
		return nil, err
	}
	return e, nil
}

// ListEngineers returns active engineers ordered by name.
func (s *Store) ListEngineers(ctx context.Context) ([]Engineer, error) {
	const q = `
		SELECT id::text, email, name, github_username,
		       COALESCE(slack_user_id,''), COALESCE(manager_slack_id,''),
		       daily_budget_usd, monthly_budget_usd, COALESCE(team,''),
		       active, created_at, updated_at
		FROM engineers
		WHERE active = TRUE
		ORDER BY name
	`
	rows, err := s.pg.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]Engineer, 0, 32)
	for rows.Next() {
		e, err := scanEngineer(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *e)
	}
	return out, rows.Err()
}

// GetEngineerByEmail fetches a single (possibly inactive) engineer by email.
// Returns ErrEngineerNotFound when no row matches.
func (s *Store) GetEngineerByEmail(ctx context.Context, email string) (*Engineer, error) {
	const q = `
		SELECT id::text, email, name, github_username,
		       COALESCE(slack_user_id,''), COALESCE(manager_slack_id,''),
		       daily_budget_usd, monthly_budget_usd, COALESCE(team,''),
		       active, created_at, updated_at
		FROM engineers
		WHERE email = $1
	`
	e, err := scanEngineer(s.pg.QueryRow(ctx, q, email))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrEngineerNotFound
	}
	return e, err
}

// UpdateEngineer applies a partial update via the COALESCE-on-NULL pattern:
// fields that come in nil are left at their current value. Only active rows
// are mutated — deactivated engineers must be reactivated through a separate
// path if/when needed.
func (s *Store) UpdateEngineer(ctx context.Context, email string, in EngineerUpdate) (*Engineer, error) {
	const q = `
		UPDATE engineers SET
		  name              = COALESCE($2, name),
		  github_username   = COALESCE($3, github_username),
		  slack_user_id     = COALESCE($4, slack_user_id),
		  manager_slack_id  = COALESCE($5, manager_slack_id),
		  daily_budget_usd  = COALESCE($6, daily_budget_usd),
		  monthly_budget_usd= COALESCE($7, monthly_budget_usd),
		  team              = COALESCE($8, team),
		  updated_at        = NOW()
		WHERE email = $1 AND active = TRUE
		RETURNING id::text, email, name, github_username,
		          COALESCE(slack_user_id,''), COALESCE(manager_slack_id,''),
		          daily_budget_usd, monthly_budget_usd, COALESCE(team,''),
		          active, created_at, updated_at
	`
	e, err := scanEngineer(s.pg.QueryRow(ctx, q, email,
		in.Name, in.GitHubUsername, in.SlackUserID, in.ManagerSlackID,
		in.DailyBudgetUSD, in.MonthlyBudgetUSD, in.Team))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrEngineerNotFound
	}
	if err != nil {
		if isUniqueViolation(err) {
			return nil, ErrEngineerExists
		}
		return nil, err
	}
	return e, nil
}

// DeactivateEngineer is a soft delete: flips active=FALSE and bumps
// updated_at. Historical usage_events and threshold_triggers rows are
// preserved so reporting continues to work.
func (s *Store) DeactivateEngineer(ctx context.Context, email string) error {
	ct, err := s.pg.Exec(ctx, `
		UPDATE engineers SET active = FALSE, updated_at = NOW()
		WHERE email = $1 AND active = TRUE
	`, email)
	if err != nil {
		return err
	}
	if ct.RowsAffected() == 0 {
		return ErrEngineerNotFound
	}
	return nil
}

// scannable is satisfied by both pgx.Row (single) and pgx.Rows (loop).
type scannable interface {
	Scan(...any) error
}

func scanEngineer(s scannable) (*Engineer, error) {
	var e Engineer
	if err := s.Scan(
		&e.ID, &e.Email, &e.Name, &e.GitHubUsername,
		&e.SlackUserID, &e.ManagerSlackID,
		&e.DailyBudgetUSD, &e.MonthlyBudgetUSD, &e.Team,
		&e.Active, &e.CreatedAt, &e.UpdatedAt,
	); err != nil {
		return nil, err
	}
	return &e, nil
}

// isUniqueViolation detects Postgres SQLSTATE 23505 (unique_violation).
// pgconn.PgError exposes the SQLSTATE, but to keep this layer free of an
// extra import we string-match the prefix the driver attaches to its message.
func isUniqueViolation(err error) bool {
	return err != nil && strings.Contains(err.Error(), "SQLSTATE 23505")
}

// --- Read helpers used by the engineer-detail endpoint ----------------------

// DailyUsage is one row of an engineer's cost time series. Computed on the
// fly from usage_events since the daily_rollups table isn't backfilled.
type DailyUsage struct {
	Date     time.Time `json:"date"`
	CostUSD  float64   `json:"cost_usd"`
	Tokens   int64     `json:"tokens"`
	EventCnt int64     `json:"event_count"`
}

// UsageHistory returns one row per UTC day for the past `days` days for a
// given engineer. Days with no activity are omitted (front-end fills gaps).
func (s *Store) UsageHistory(ctx context.Context, email string, days int) ([]DailyUsage, error) {
	if days <= 0 {
		days = 30
	}
	rows, err := s.pg.Query(ctx, `
		SELECT
		  DATE_TRUNC('day', occurred_at AT TIME ZONE 'UTC')::date AS d,
		  COALESCE(SUM(cost_usd), 0)::float8,
		  COALESCE(SUM(
		    COALESCE(tokens_input,0)+COALESCE(tokens_output,0)+
		    COALESCE(tokens_cache_read,0)+COALESCE(tokens_cache_creation,0)
		  ), 0)::bigint,
		  COUNT(*)::bigint
		FROM usage_events
		WHERE engineer_id = $1
		  AND occurred_at >= NOW() - ($2::int * INTERVAL '1 day')
		GROUP BY d
		ORDER BY d DESC
	`, email, days)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]DailyUsage, 0, days)
	for rows.Next() {
		var u DailyUsage
		if err := rows.Scan(&u.Date, &u.CostUSD, &u.Tokens, &u.EventCnt); err != nil {
			return nil, err
		}
		out = append(out, u)
	}
	return out, rows.Err()
}

// ListRecentTriggers returns the N most recent threshold_triggers rows for
// an engineer, newest first. Used by the engineer-detail endpoint.
func (s *Store) ListRecentTriggers(ctx context.Context, email string, limit int) ([]ThresholdTrigger, error) {
	if limit <= 0 {
		limit = 20
	}
	rows, err := s.pg.Query(ctx, `
		SELECT engineer_id, period, threshold_pct, triggered_at,
		       spend_at_trigger_usd, budget_usd,
		       COALESCE(slack_message_ts,''),
		       COALESCE(notified_manager, FALSE)
		FROM threshold_triggers
		WHERE engineer_id = $1
		ORDER BY triggered_at DESC
		LIMIT $2
	`, email, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]ThresholdTrigger, 0, limit)
	for rows.Next() {
		var t ThresholdTrigger
		if err := rows.Scan(&t.EngineerID, &t.Period, &t.ThresholdPct, &t.TriggeredAt,
			&t.SpendAtTriggerUSD, &t.BudgetUSD, &t.SlackMessageTS, &t.NotifiedManager); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// LatestEfficiencySnapshot returns the most recent snapshot for an engineer
// across all windows, or nil if none have been computed yet.
func (s *Store) LatestEfficiencySnapshot(ctx context.Context, email string) (*EfficiencySnapshot, error) {
	const q = `
		SELECT engineer_id, window_start, window_end, cost_usd, merged_pr_count,
		       COALESCE(revert_rate, 0)::float8,
		       COALESCE(dollars_per_merged_pr, 0)::float8,
		       computed_at
		FROM efficiency_snapshots
		WHERE engineer_id = $1
		ORDER BY computed_at DESC
		LIMIT 1
	`
	var snap EfficiencySnapshot
	err := s.pg.QueryRow(ctx, q, email).Scan(
		&snap.EngineerID, &snap.WindowStart, &snap.WindowEnd, &snap.CostUSD,
		&snap.MergedPRCount, &snap.RevertRate, &snap.DollarsPerMergedPR, &snap.ComputedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &snap, nil
}

// RecentEvent is the read model for /admin/usage/recent — pared down so
// large `raw` JSONB blobs don't bloat the response.
type RecentEvent struct {
	ID         int64     `json:"id"`
	EngineerID string    `json:"engineer_id"`
	OccurredAt time.Time `json:"occurred_at"`
	Source     string    `json:"source"`
	MetricName string    `json:"metric_name"`
	CostUSD    *float64  `json:"cost_usd,omitempty"`
	Model      string    `json:"model,omitempty"`
}

// ListRecentEvents returns the N most recent usage_events rows globally.
// Used for the admin debugging surface.
func (s *Store) ListRecentEvents(ctx context.Context, limit int) ([]RecentEvent, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	rows, err := s.pg.Query(ctx, `
		SELECT id, engineer_id, occurred_at, source, metric_name,
		       cost_usd, COALESCE(model,'')
		FROM usage_events
		ORDER BY id DESC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]RecentEvent, 0, limit)
	for rows.Next() {
		var e RecentEvent
		if err := rows.Scan(&e.ID, &e.EngineerID, &e.OccurredAt, &e.Source, &e.MetricName,
			&e.CostUSD, &e.Model); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

