package registry

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Engineer struct {
	ID               string
	Email            string
	Name             string
	GitHubUsername   string
	SlackUserID      string
	ManagerSlackID   string
	DailyBudgetUSD   float64
	MonthlyBudgetUSD float64
	Team             string
	Active           bool
}

type Stats struct {
	Count            int
	LastRefreshAt    time.Time
	LastRefreshError string
}

type snapshot struct {
	byEmail  map[string]*Engineer
	byGitHub map[string]*Engineer
}

type EngineerRegistry struct {
	db              *pgxpool.Pool
	refreshInterval time.Duration

	mu    sync.RWMutex
	snap  *snapshot
	stats Stats
}

func New(db *pgxpool.Pool, refreshInterval time.Duration) *EngineerRegistry {
	return &EngineerRegistry{
		db:              db,
		refreshInterval: refreshInterval,
		snap: &snapshot{
			byEmail:  map[string]*Engineer{},
			byGitHub: map[string]*Engineer{},
		},
	}
}

func (r *EngineerRegistry) Load(ctx context.Context) error {
	const q = `
		SELECT id::text, email, name, github_username,
		       COALESCE(slack_user_id, ''), COALESCE(manager_slack_id, ''),
		       daily_budget_usd, monthly_budget_usd,
		       COALESCE(team, ''), active
		FROM engineers
		WHERE active = TRUE
	`
	rows, err := r.db.Query(ctx, q)
	if err != nil {
		r.recordError(err)
		return err
	}
	defer rows.Close()

	byEmail := map[string]*Engineer{}
	byGitHub := map[string]*Engineer{}
	for rows.Next() {
		e := &Engineer{}
		if err := rows.Scan(
			&e.ID, &e.Email, &e.Name, &e.GitHubUsername,
			&e.SlackUserID, &e.ManagerSlackID,
			&e.DailyBudgetUSD, &e.MonthlyBudgetUSD,
			&e.Team, &e.Active,
		); err != nil {
			r.recordError(err)
			return err
		}
		byEmail[e.Email] = e
		byGitHub[e.GitHubUsername] = e
	}
	if err := rows.Err(); err != nil {
		r.recordError(err)
		return err
	}

	r.mu.Lock()
	r.snap = &snapshot{byEmail: byEmail, byGitHub: byGitHub}
	r.stats.Count = len(byEmail)
	r.stats.LastRefreshAt = time.Now()
	r.stats.LastRefreshError = ""
	r.mu.Unlock()
	return nil
}

func (r *EngineerRegistry) recordError(err error) {
	r.mu.Lock()
	r.stats.LastRefreshError = err.Error()
	r.mu.Unlock()
}

func (r *EngineerRegistry) LookupByEmail(email string) (*Engineer, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	e, ok := r.snap.byEmail[email]
	return e, ok
}

func (r *EngineerRegistry) LookupByGitHub(username string) (*Engineer, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	e, ok := r.snap.byGitHub[username]
	return e, ok
}

func (r *EngineerRegistry) AllEmails() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.snap.byEmail))
	for email := range r.snap.byEmail {
		out = append(out, email)
	}
	return out
}

func (r *EngineerRegistry) Stats() Stats {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.stats
}

func (r *EngineerRegistry) StartRefresh(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(r.refreshInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := r.Load(ctx); err != nil {
					log.Printf("registry refresh failed: %v", err)
				}
			}
		}
	}()
}
