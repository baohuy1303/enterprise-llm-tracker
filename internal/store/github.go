package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// dirty set: Redis SET of github_usernames pending refresh. The Kafka
// github-trigger consumer SADDs into it; the collector SMEMBERS+DEL it at run
// start so concurrent triggers during a long run aren't lost.
const githubDirtyKey = "github:dirty_engineers"

// collectorTriggerChannel is the Redis Pub/Sub channel used to signal the
// efficiency collector from outside its own binary (e.g. sentinel-api's
// /admin/refresh-efficiency endpoint).
const collectorTriggerChannel = "sentinel:collector:trigger"

// UpsertGitHubPR writes a single PR record. Uses (repo, pr_number) as the
// natural key — re-syncing the same PR overwrites its state/merged_at/etc.
func (s *Store) UpsertGitHubPR(ctx context.Context, pr GitHubPR) error {
	files, _ := json.Marshal(pr.Files)
	_, err := s.pg.Exec(ctx, `
		INSERT INTO github_prs
		  (engineer_github, repo, pr_number, title, state,
		   created_at, merged_at, review_count, files_changed,
		   reverted, reverted_at, last_synced_at, files)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
		ON CONFLICT (repo, pr_number) DO UPDATE SET
		  engineer_github = EXCLUDED.engineer_github,
		  title           = EXCLUDED.title,
		  state           = EXCLUDED.state,
		  merged_at       = EXCLUDED.merged_at,
		  review_count    = EXCLUDED.review_count,
		  files_changed   = EXCLUDED.files_changed,
		  last_synced_at  = EXCLUDED.last_synced_at,
		  files           = EXCLUDED.files
	`, pr.EngineerGitHub, pr.Repo, pr.PRNumber, pr.Title, pr.State,
		pr.CreatedAt, pr.MergedAt, pr.ReviewCount, pr.FilesChanged,
		pr.Reverted, pr.RevertedAt, pr.LastSyncedAt, files)
	return err
}

// MarkPRReverted flips the original PR's reverted flag. Called after the
// revert detector identifies an offending PR.
func (s *Store) MarkPRReverted(ctx context.Context, repo string, prNumber int, at time.Time) error {
	_, err := s.pg.Exec(ctx, `
		UPDATE github_prs
		SET reverted = TRUE, reverted_at = $3
		WHERE repo = $1 AND pr_number = $2 AND reverted = FALSE
	`, repo, prNumber, at)
	return err
}

// ListMergedPRsByEngineerGitHub returns merged PRs by an engineer in a window.
// Used by the revert detector (file-overlap scan) and the efficiency computer.
func (s *Store) ListMergedPRsByEngineerGitHub(ctx context.Context, githubUser string, since time.Time) ([]GitHubPR, error) {
	rows, err := s.pg.Query(ctx, `
		SELECT engineer_github, repo, pr_number, COALESCE(title,''), COALESCE(state,''),
		       created_at, merged_at, COALESCE(review_count,0), COALESCE(files_changed,0),
		       COALESCE(files,'[]'::jsonb), reverted, reverted_at, last_synced_at
		FROM github_prs
		WHERE engineer_github = $1
		  AND merged_at IS NOT NULL
		  AND merged_at >= $2
		ORDER BY merged_at DESC
	`, githubUser, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanPRs(rows)
}

// ListRecentPRsByRepo is used by the revert detector to pull a repo-wide
// recent window for the title-based "Revert ..." scan.
func (s *Store) ListRecentPRsByRepo(ctx context.Context, repo string, since time.Time) ([]GitHubPR, error) {
	rows, err := s.pg.Query(ctx, `
		SELECT engineer_github, repo, pr_number, COALESCE(title,''), COALESCE(state,''),
		       created_at, merged_at, COALESCE(review_count,0), COALESCE(files_changed,0),
		       COALESCE(files,'[]'::jsonb), reverted, reverted_at, last_synced_at
		FROM github_prs
		WHERE repo = $1 AND created_at >= $2
		ORDER BY created_at DESC
	`, repo, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanPRs(rows)
}

func scanPRs(rows interface {
	Next() bool
	Scan(...any) error
	Err() error
}) ([]GitHubPR, error) {
	var out []GitHubPR
	for rows.Next() {
		var pr GitHubPR
		var filesJSON []byte
		if err := rows.Scan(
			&pr.EngineerGitHub, &pr.Repo, &pr.PRNumber, &pr.Title, &pr.State,
			&pr.CreatedAt, &pr.MergedAt, &pr.ReviewCount, &pr.FilesChanged,
			&filesJSON, &pr.Reverted, &pr.RevertedAt, &pr.LastSyncedAt,
		); err != nil {
			return nil, err
		}
		if len(filesJSON) > 0 {
			_ = json.Unmarshal(filesJSON, &pr.Files)
		}
		out = append(out, pr)
	}
	return out, rows.Err()
}

// WriteEfficiencySnapshot appends a snapshot row. Snapshots are append-only;
// reads pick the latest by computed_at for each (engineer, window).
func (s *Store) WriteEfficiencySnapshot(ctx context.Context, snap EfficiencySnapshot) error {
	_, err := s.pg.Exec(ctx, `
		INSERT INTO efficiency_snapshots
		  (engineer_id, window_start, window_end, cost_usd, merged_pr_count,
		   revert_rate, dollars_per_merged_pr, computed_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
	`, snap.EngineerID, snap.WindowStart, snap.WindowEnd, snap.CostUSD,
		snap.MergedPRCount, snap.RevertRate, snap.DollarsPerMergedPR, snap.ComputedAt)
	return err
}

// LeaderboardEntry is the per-engineer row returned by /admin/leaderboard.
type LeaderboardEntry struct {
	EngineerID         string    `json:"engineer_id"`
	WindowStart        time.Time `json:"window_start"`
	WindowEnd          time.Time `json:"window_end"`
	CostUSD            float64   `json:"cost_usd"`
	MergedPRCount      int       `json:"merged_pr_count"`
	RevertRate         float64   `json:"revert_rate"`
	DollarsPerMergedPR float64   `json:"dollars_per_merged_pr"`
	ComputedAt         time.Time `json:"computed_at"`
}

// Leaderboard returns the latest snapshot per engineer for a given window,
// sorted by $/merged-PR ascending (engineers with no merges go last).
func (s *Store) Leaderboard(ctx context.Context, windowStart, windowEnd time.Time, limit int) ([]LeaderboardEntry, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.pg.Query(ctx, `
		SELECT DISTINCT ON (engineer_id)
		  engineer_id, window_start, window_end, cost_usd, merged_pr_count,
		  COALESCE(revert_rate, 0)::float8, COALESCE(dollars_per_merged_pr, 0)::float8,
		  computed_at
		FROM efficiency_snapshots
		WHERE window_start = $1 AND window_end = $2
		ORDER BY engineer_id, computed_at DESC
	`, windowStart, windowEnd)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []LeaderboardEntry
	for rows.Next() {
		var e LeaderboardEntry
		if err := rows.Scan(
			&e.EngineerID, &e.WindowStart, &e.WindowEnd, &e.CostUSD, &e.MergedPRCount,
			&e.RevertRate, &e.DollarsPerMergedPR, &e.ComputedAt,
		); err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Sort: engineers with merges by $/PR asc, then engineers with 0 merges last.
	sortLeaderboard(entries)
	if len(entries) > limit {
		entries = entries[:limit]
	}
	return entries, nil
}

func sortLeaderboard(es []LeaderboardEntry) {
	// in-place insertion sort — leaderboards are small (hundreds, not millions)
	// and this keeps the file dep-free
	for i := 1; i < len(es); i++ {
		for j := i; j > 0 && lessLB(es[j], es[j-1]); j-- {
			es[j], es[j-1] = es[j-1], es[j]
		}
	}
}

func lessLB(a, b LeaderboardEntry) bool {
	if a.MergedPRCount == 0 && b.MergedPRCount > 0 {
		return false
	}
	if a.MergedPRCount > 0 && b.MergedPRCount == 0 {
		return true
	}
	return a.DollarsPerMergedPR < b.DollarsPerMergedPR
}

// CostForEngineerWindow sums usage_events.cost_usd for one engineer over a
// window. Used by the efficiency computer.
func (s *Store) CostForEngineerWindow(ctx context.Context, engineerID string, start, end time.Time) (float64, error) {
	var sum float64
	err := s.pg.QueryRow(ctx, `
		SELECT COALESCE(SUM(cost_usd), 0)::float8
		FROM usage_events
		WHERE engineer_id = $1
		  AND occurred_at >= $2 AND occurred_at < $3
	`, engineerID, start, end).Scan(&sum)
	return sum, err
}

// RecordRepoSyncOK / RecordRepoSyncErr maintain the github_repo_sync row.
func (s *Store) RecordRepoSyncOK(ctx context.Context, repo string, at time.Time) error {
	_, err := s.pg.Exec(ctx, `
		INSERT INTO github_repo_sync (repo, last_synced_at, last_error, last_error_at)
		VALUES ($1, $2, NULL, NULL)
		ON CONFLICT (repo) DO UPDATE SET
		  last_synced_at = EXCLUDED.last_synced_at,
		  last_error = NULL,
		  last_error_at = NULL
	`, repo, at)
	return err
}

func (s *Store) RecordRepoSyncErr(ctx context.Context, repo string, errMsg string, at time.Time) error {
	_, err := s.pg.Exec(ctx, `
		INSERT INTO github_repo_sync (repo, last_error, last_error_at)
		VALUES ($1, $2, $3)
		ON CONFLICT (repo) DO UPDATE SET
		  last_error = EXCLUDED.last_error,
		  last_error_at = EXCLUDED.last_error_at
	`, repo, errMsg, at)
	return err
}

// MarkEngineerDirty / DrainDirtyEngineers manage the Kafka-driven refresh set.
// Trigger fires when a commit/PR event flows; collector drains the set at run
// start so concurrent triggers during a slow run aren't lost.
func (s *Store) MarkEngineerDirty(ctx context.Context, githubUser string) error {
	if githubUser == "" {
		return nil
	}
	return s.rdb.SAdd(ctx, githubDirtyKey, githubUser).Err()
}

// PublishCollectorTrigger sends a refresh signal to any subscribed collector.
// Pub/Sub is fire-and-forget — if no collector is listening, the message is
// dropped. Called from sentinel-api's /admin/refresh-efficiency endpoint.
func (s *Store) PublishCollectorTrigger(ctx context.Context) error {
	return s.rdb.Publish(ctx, collectorTriggerChannel, "refresh").Err()
}

// SubscribeCollectorTriggers returns a channel that fires once per incoming
// trigger message. Caller must keep the returned closer reachable for the
// lifetime of the subscription; closing it unsubscribes. Errors from the
// underlying pub/sub are logged but not surfaced — the collector still has
// its cron tick as a safety net.
func (s *Store) SubscribeCollectorTriggers(ctx context.Context) (<-chan struct{}, func() error) {
	sub := s.rdb.Subscribe(ctx, collectorTriggerChannel)
	msgs := sub.Channel()
	out := make(chan struct{}, 1)
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case _, ok := <-msgs:
				if !ok {
					return
				}
				select {
				case out <- struct{}{}:
				default:
				}
			}
		}
	}()
	return out, sub.Close
}

func (s *Store) DrainDirtyEngineers(ctx context.Context) ([]string, error) {
	members, err := s.rdb.SMembers(ctx, githubDirtyKey).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}
	if len(members) == 0 {
		return nil, nil
	}
	if err := s.rdb.Del(ctx, githubDirtyKey).Err(); err != nil {
		return nil, fmt.Errorf("drain dirty: %w", err)
	}
	return members, nil
}
