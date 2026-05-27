-- Per-repo sync metadata used by the GitHub collector to resume between runs
-- and record the last error for ops visibility.
CREATE TABLE IF NOT EXISTS github_repo_sync (
  repo TEXT PRIMARY KEY,
  last_synced_at TIMESTAMPTZ,
  last_error TEXT,
  last_error_at TIMESTAMPTZ
);

-- github_prs already exists from 002_persistence.sql; add a covering index for
-- the per-engineer / per-window queries the efficiency computer issues.
CREATE INDEX IF NOT EXISTS ix_github_prs_engineer_merged
  ON github_prs(engineer_github, merged_at DESC);

-- efficiency_snapshots — covering index for leaderboard reads (latest snapshot
-- per engineer per window).
CREATE INDEX IF NOT EXISTS ix_efficiency_snapshots_window_computed
  ON efficiency_snapshots(window_start, window_end, computed_at DESC);

-- Track files touched per PR so the revert detector can compute file overlap.
-- Stored as a JSONB array of file paths — small, queryable, and PRs rarely
-- touch enough files to need a normalized side table.
ALTER TABLE github_prs ADD COLUMN IF NOT EXISTS files JSONB;
