# Signal Analytics — Planning

Detect efficiency patterns and behavioral anomalies in Claude Code usage
without relying on absolute thresholds. Signals are surfaced to the engineer
themselves (self-service) and to managers (full visibility, no tenancy split).

---

## 1. Signals we track

### Efficiency signals (rollup, per engineer × window)

Windows: 1d / 7d / 30d / 180d. Computed nightly, written to `engineer_signals`.

| Field | Definition | Source |
|---|---|---|
| `prs_opened` | PRs created in window | `github_prs.created_at` |
| `prs_merged` | PRs merged in window | `github_prs.state = 'MERGED'` |
| `prs_closed_unmerged` | PRs closed without merge | `state = 'CLOSED' AND merged_at IS NULL` |
| `prs_reverted` | merged then reverted | `github_prs.reverted = TRUE` |
| `cost_usd` | total Claude cost in window | `SUM(usage_events.cost_usd)` |
| `merge_rate` | `prs_merged / prs_opened` | derived |
| `revert_rate` | `prs_reverted / prs_merged` | derived |
| `dollars_per_merged_pr` | `cost_usd / prs_merged` | derived (current metric) |
| `dollars_per_opened_pr` | `cost_usd / prs_opened` | derived — **catches the "churned 20, merged 5" pattern** |
| `cache_hit_ratio` | `cache_read / (cache_read + cache_creation)` | `usage_events` token cols |
| `dollars_per_kloc` | `cost_usd / lines_shipped` | `usage_events` cost + `claude_code.lines_of_code.count` OTel metric |
| `model_mix` | `{opus: pct, sonnet: pct, haiku: pct}` of cost | `usage_events.model` |
| `peer_percentile` | rank of `dollars_per_merged_pr` across all engineers in window | derived last |

The four PR counts together tell the story you flagged: low merge rate +
high opened count = churn; low opened + high merge rate = focused shipping.

### Pattern signals (near real-time, per event)

Detected by a new `signal-detector` Kafka consumer. Written to `signal_events`.

| Signal | Trigger | Baseline source |
|---|---|---|
| `spend_zscore_high` | today's cumulative spend > 2σ above engineer's 30-day daily-spend mean | nightly rebuild from `usage_events` |
| `burst` | engineer's spend in any rolling 30-min window > 2× their 14-day hourly p95 | Redis rolling window |
| `rhythm_break` | event arrives in an hour-of-day bucket holding <5% of engineer's historical activity | nightly rebuild |

Each signal row carries a `severity` field set at detection time (table below).

---

## 2. Data we need

| Data | Where it lives today | Gap |
|---|---|---|
| Per-event cost + tokens | `usage_events` | — |
| Cache read/creation split | `usage_events.tokens_cache_*` | — |
| Model used | `usage_events.model` | — |
| PR state + reverts | `github_prs` | — |
| Lines of code shipped | OTel metric `claude_code.lines_of_code.count` | currently ingested but not aggregated; needs a rollup column |
| Engineer's 30-day baseline (mean, stddev, hourly p95, hour-of-day distribution) | nowhere | new — cached in Redis under `engineer:{email}:baseline:*`, rebuilt nightly |
| Team median $/PR for peer percentile | nowhere | new — computed at rollup time |

No new ingest fields needed. Everything derives from `usage_events` + `github_prs`.

---

## 3. Storage

### New tables

```sql
-- migrations/005_signal_analytics.sql

CREATE TABLE engineer_signals (
  id BIGSERIAL PRIMARY KEY,
  engineer_id TEXT NOT NULL,
  window_start DATE NOT NULL,
  window_end DATE NOT NULL,
  window_name TEXT NOT NULL,            -- '1d' | '7d' | '30d' | '180d'

  -- PR shape
  prs_opened INT NOT NULL DEFAULT 0,
  prs_merged INT NOT NULL DEFAULT 0,
  prs_closed_unmerged INT NOT NULL DEFAULT 0,
  prs_reverted INT NOT NULL DEFAULT 0,

  -- cost / token shape
  cost_usd NUMERIC(10,2) NOT NULL DEFAULT 0,
  lines_shipped INT NOT NULL DEFAULT 0,
  cache_hit_ratio NUMERIC(5,4),
  dollars_per_kloc NUMERIC(10,2),
  model_mix JSONB,                       -- {"opus": 0.42, "sonnet": 0.51, "haiku": 0.07}

  -- peer comparison
  team_dollars_per_pr_median NUMERIC(10,2),
  peer_percentile INT,                   -- 0..100, NULL if window has <3 peers

  computed_at TIMESTAMPTZ NOT NULL,
  UNIQUE (engineer_id, window_name, window_end)
);
CREATE INDEX ix_engineer_signals_engineer ON engineer_signals(engineer_id, window_end DESC);

CREATE TABLE signal_events (
  id BIGSERIAL PRIMARY KEY,
  engineer_id TEXT NOT NULL,
  signal_type TEXT NOT NULL,             -- 'spend_zscore_high' | 'burst' | 'rhythm_break'
  severity TEXT NOT NULL,                -- 'info' | 'warn' | 'critical'
  occurred_at TIMESTAMPTZ NOT NULL,
  observed_value NUMERIC(12,4),          -- the metric that fired (spend, z, etc.)
  baseline_value NUMERIC(12,4),          -- what was expected
  z_score NUMERIC(6,2),                  -- nullable, populated for zscore signals
  context JSONB,                         -- model, recent files, top metric_name, etc.
  notified BOOLEAN DEFAULT FALSE,
  notified_at TIMESTAMPTZ
);
CREATE INDEX ix_signal_events_engineer_time ON signal_events(engineer_id, occurred_at DESC);
CREATE INDEX ix_signal_events_type_severity ON signal_events(signal_type, severity, occurred_at DESC);
```

### Baseline cache (Redis)

```
engineer:{email}:baseline:daily_mean       float — 30-day mean of daily spend
engineer:{email}:baseline:daily_stddev     float
engineer:{email}:baseline:hourly_p95       float — for burst detection
engineer:{email}:baseline:hour_distribution hash {0..23 → pct}
engineer:{email}:baseline:rebuilt_at       RFC3339 timestamp
```

Rebuilt nightly. TTL 48h so an engineer with no data drops out cleanly.

### Existing tables — no changes
`usage_events`, `github_prs`, `efficiency_snapshots` stay as-is. The old
`efficiency_snapshots` table can be kept for backwards compat or dropped once
`engineer_signals` is populated — decision at implementation time.

---

## 4. Computation

### `signal-detector` (new Kafka consumer in `sentinel-workers`)
Subscribes to existing `claude.usage.events` topic. Per event:
1. Check engineer's `hour_distribution[H]` — if <5%, emit `rhythm_break`.
2. INCRBYFLOAT into a 30-min sliding window Redis key; if cumulative > 2× `hourly_p95`, emit `burst`.
3. (Cheaper to compute on a 5-min tick than per-event) Re-read `cost:today`,
   compute Z-score against `daily_mean` / `daily_stddev`; if >2σ, emit
   `spend_zscore_high`.

Severity assigned at emit time (table in §5). Dedup via SETNX
`engineer:{email}:signal_fired:{type}:{severity}:{day}` — same idea as
existing threshold dedup.

### `baseline-rebuilder` (new scheduled job, nightly 00:30 UTC)
For each active engineer:
- Query `usage_events` for the last 30 days
- Compute daily-spend mean + stddev
- Compute hourly p95 over 14 days
- Compute hour-of-day distribution (normalize to fractions)
- Write to Redis under `baseline:*` keys

Can live in `sentinel-workers` as a cron-style goroutine, or as a separate
short-lived binary triggered by k8s CronJob. Lean toward the former for now.

### `efficiency-rollup` (new scheduled job, nightly 01:00 UTC)
For each active engineer × window:
- Aggregate `usage_events` (cost, tokens, cache, model mix)
- Aggregate `github_prs` (4 PR counts)
- Compute derived ratios
- After all engineers done, compute team medians and back-fill `peer_percentile`
- Upsert into `engineer_signals` keyed on `(engineer_id, window_name, window_end)`

Idempotent re-runs are fine — same date keys overwrite.

---

## 5. Severity & alerting

| Signal | Info | Warn | Critical |
|---|---|---|---|
| `spend_zscore_high` | 2–2.5σ | 2.5–3σ | >3σ |
| `burst` | — | 2–3× hourly p95 | **>3× hourly p95** |
| `rhythm_break` | <5% bucket | <2% bucket | <1% bucket |

### Alerting rules
- **`burst` critical** → immediate Slack DM to engineer + manager. No
  sustained-period gate. (Burst is by definition a now-thing.)
- **`spend_zscore_high` critical** AND sustained for ≥2 consecutive days →
  DM manager. Single-day spikes get logged but don't page.
- **All other signals** → stored in `signal_events`, surfaced via dashboard.
  No DMs.

Dedup keys (24h TTL) prevent the same signal+severity firing twice in a day
for an engineer.

### Reusing existing infra
- Slack client (`internal/slack`) — already exists
- Dedup pattern (SETNX with EXPIREAT) — already used by `ClaimThresholdFired`
- `signal_events.notified` mirrors the audit pattern from `threshold_triggers`

---

## 6. API surface

### Manager / admin (existing bearer auth)
```
GET  /admin/signals/efficiency?window=30d           — leaderboard-style table of all engineers
GET  /admin/signals/efficiency/{email}?window=30d   — single engineer detail
GET  /admin/signals/events?since=2026-05-20&severity=critical
                                                    — recent detected signals across org
GET  /admin/signals/events/{email}                  — recent signals for one engineer
```

### Engineer self-service (auth model TBD)
```
GET  /me/signals/efficiency?window=30d              — their own efficiency rollup
GET  /me/signals/events                             — their own detected events
```

Self-service auth is out of scope for this stage — placeholder. For now,
admins can hit the `{email}` endpoints on the engineer's behalf.

---

## 7. Phasing

1. **Migration + tables** (`005_signal_analytics.sql`)
2. **`baseline-rebuilder` job** — fills Redis baseline keys nightly
3. **`efficiency-rollup` job** — fills `engineer_signals` nightly
4. **Admin read endpoints** — `/admin/signals/*`
5. **`signal-detector` consumer** — writes to `signal_events`
6. **Burst-critical Slack alerting**
7. **Z-score sustained-day alerting** (needs 2-day history → can ship later)
8. **Engineer self-service endpoints + auth model**

Steps 1–4 deliver the manager view with zero alerting risk. Steps 5–7 add
the real-time signal stream once we're comfortable with the rollups looking
sane.

---

## 8. Open questions

- **Lines-of-code attribution** — the OTel metric `claude_code.lines_of_code.count` exists but its semantics under partial accepts / rejected suggestions need verification before `dollars_per_kloc` is trustworthy.
- **New-engineer baselines** — Z-score on a <14-day baseline is noisy. Options: suppress pattern signals until baseline maturity, or substitute team median for engineer-specific stats. Lean toward suppression with a config flag.
- **Model mix scoring** — "should have used Sonnet" requires a task-difficulty proxy. Not in scope; we surface the raw mix and let humans interpret.
- **Peer percentile fairness** — comparing across teams of very different size or seniority may mislead. Until we add a `team` filter to the percentile calculation, treat percentile as advisory.

---

## 9. Out of scope (decided)

- Onboarding curve
- File-area / repo-health concentration signals
- Cache-cliff pattern signal
- Session-frequency pattern signal
- Prompt / response content storage (privacy + liability — see prior discussion)
- Multi-tenant manager scoping (everyone-sees-everyone for now)
