CREATE TABLE IF NOT EXISTS usage_events (
  id BIGSERIAL PRIMARY KEY,
  engineer_id TEXT NOT NULL,
  occurred_at TIMESTAMPTZ NOT NULL,
  source TEXT NOT NULL,
  metric_name TEXT NOT NULL,
  cost_usd NUMERIC(10, 6),
  tokens_input INT,
  tokens_output INT,
  tokens_cache_read INT,
  tokens_cache_creation INT,
  model TEXT,
  raw JSONB
);
CREATE INDEX IF NOT EXISTS ix_usage_events_engineer_time ON usage_events(engineer_id, occurred_at DESC);

CREATE TABLE IF NOT EXISTS daily_rollups (
  engineer_id TEXT NOT NULL,
  date DATE NOT NULL,
  total_cost_usd NUMERIC(10, 4) NOT NULL DEFAULT 0,
  total_tokens BIGINT NOT NULL DEFAULT 0,
  session_count INT NOT NULL DEFAULT 0,
  pr_count INT NOT NULL DEFAULT 0,
  commit_count INT NOT NULL DEFAULT 0,
  lines_added INT NOT NULL DEFAULT 0,
  lines_removed INT NOT NULL DEFAULT 0,
  PRIMARY KEY (engineer_id, date)
);

CREATE TABLE IF NOT EXISTS github_prs (
  id BIGSERIAL PRIMARY KEY,
  engineer_github TEXT NOT NULL,
  repo TEXT NOT NULL,
  pr_number INT NOT NULL,
  title TEXT,
  state TEXT,
  created_at TIMESTAMPTZ,
  merged_at TIMESTAMPTZ,
  review_count INT,
  files_changed INT,
  reverted BOOLEAN DEFAULT FALSE,
  reverted_at TIMESTAMPTZ,
  last_synced_at TIMESTAMPTZ NOT NULL,
  UNIQUE (repo, pr_number)
);

CREATE TABLE IF NOT EXISTS efficiency_snapshots (
  id BIGSERIAL PRIMARY KEY,
  engineer_id TEXT NOT NULL,
  window_start DATE NOT NULL,
  window_end DATE NOT NULL,
  cost_usd NUMERIC(10, 2) NOT NULL,
  merged_pr_count INT NOT NULL,
  revert_rate NUMERIC(5, 4),
  dollars_per_merged_pr NUMERIC(10, 2),
  computed_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS threshold_triggers (
  id BIGSERIAL PRIMARY KEY,
  engineer_id TEXT NOT NULL,
  period TEXT NOT NULL,
  threshold_pct INT NOT NULL,
  triggered_at TIMESTAMPTZ NOT NULL,
  spend_at_trigger_usd NUMERIC(10, 4) NOT NULL,
  budget_usd NUMERIC(10, 2) NOT NULL,
  slack_message_ts TEXT,
  notified_manager BOOLEAN DEFAULT FALSE
);
