-- Per-engineer × per-window efficiency rollup. Replaces the lighter
-- efficiency_snapshots table for new reads; old table left intact for now.
CREATE TABLE IF NOT EXISTS engineer_signals (
  id BIGSERIAL PRIMARY KEY,
  engineer_id TEXT NOT NULL,
  window_name TEXT NOT NULL,
  window_start DATE NOT NULL,
  window_end DATE NOT NULL,

  prs_opened INT NOT NULL DEFAULT 0,
  prs_merged INT NOT NULL DEFAULT 0,
  prs_closed_unmerged INT NOT NULL DEFAULT 0,
  prs_reverted INT NOT NULL DEFAULT 0,

  cost_usd NUMERIC(10, 2) NOT NULL DEFAULT 0,
  lines_shipped BIGINT NOT NULL DEFAULT 0,
  cache_hit_ratio NUMERIC(5, 4),
  dollars_per_kloc NUMERIC(10, 2),
  model_mix JSONB,

  team_dollars_per_pr_median NUMERIC(10, 2),
  peer_percentile INT,

  computed_at TIMESTAMPTZ NOT NULL,
  UNIQUE (engineer_id, window_name, window_end)
);

CREATE INDEX IF NOT EXISTS ix_engineer_signals_engineer
  ON engineer_signals(engineer_id, window_end DESC);

-- Detected near-real-time signals: burst, spend_zscore_high, rhythm_break.
CREATE TABLE IF NOT EXISTS signal_events (
  id BIGSERIAL PRIMARY KEY,
  engineer_id TEXT NOT NULL,
  signal_type TEXT NOT NULL,
  severity TEXT NOT NULL,
  occurred_at TIMESTAMPTZ NOT NULL,
  observed_value NUMERIC(12, 4),
  baseline_value NUMERIC(12, 4),
  z_score NUMERIC(6, 2),
  context JSONB,
  notified BOOLEAN NOT NULL DEFAULT FALSE,
  notified_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS ix_signal_events_engineer_time
  ON signal_events(engineer_id, occurred_at DESC);

CREATE INDEX IF NOT EXISTS ix_signal_events_type_severity
  ON signal_events(signal_type, severity, occurred_at DESC);
