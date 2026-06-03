# Redis Counter Reconciliation Cron — Plan

## Problem

Redis holds the live cost counters (`cost:today`, `cost:month`) that the hot
path increments and that threshold alerts + the manager dashboard read. These
can **drift** from the Postgres ledger:

- A failed Redis increment on the ingest hot path is non-fatal — it's logged and
  skipped, so the counter silently under-counts for the rest of the period.
- Nothing re-derives Redis from its source of truth except `RebuildCounters` on
  process startup.

**Postgres `usage_events` is the source of truth** (durable, complete, idempotent
via `event_id`). Redis is a derived hot-cache. We need to periodically re-derive
the cache from the ledger so it can't diverge unboundedly during a day/month.

## Why not a plain hourly TTL

Expiring the counter hourly corrupts it: the increment path uses `INCRBYFLOAT`,
which starts from zero on a missing key. An engineer who spends $24 then goes
idle would have the key expire, read as $0, and the next event would reset it to
just that event's cost — a massive under-count. A correct lazy-expiry approach
requires read-through reseed on the hot path, which adds a Postgres query to
ingest. We're avoiding that.

## Chosen approach: sharded reconciliation cron (upward-only)

A background service (same cron style as `BaselineRebuilder`) that periodically
re-derives each engineer's `cost:today` / `cost:month` from Postgres and corrects
Redis — **spread across the hour** to avoid a synchronized spike, and **only ever
raising** the counter to avoid clobbering in-flight events.

### Sharding (no thundering herd)

- Each engineer maps to a bucket: `bucket = hash(email) % 60`.
- The job ticks every minute; on minute-of-hour `m` it processes only engineers
  whose `bucket == m`.
- Net effect: every engineer is reconciled once per hour, but the load is a
  smooth trickle (~N/60 engineers per minute) instead of all N at once.

### Upward-only guard (race-safe correction)

The cron reads `SELECT SUM(cost_usd)` from Postgres, which **lags** Redis (the
postgres-writer consumer is async). A naive `SET` could move the counter
*backward* and erase an in-flight event.

Guard: only correct when the Postgres sum is **higher** than the Redis value by
more than a threshold (e.g. `> $0.50` or `> 1%`). Drift from failed increments
always makes Redis *lower* than the true sum, so "raise to match Postgres, never
lower" fixes real drift and sidesteps the lag race entirely. Redis being slightly
ahead of Postgres is the normal healthy state — leave it alone.

## Component sketch

```
internal/service/reconciler.go   (NEW)
  type CounterReconciler struct { store, registry, cfg, logger }
  func (r) Run(ctx)                 // minute ticker; cancels on ctx
  func (r) reconcileBucket(ctx, m)  // engineers where hash(email)%60 == m
  func (r) reconcileOne(ctx, eng)   // today + month, upward-only

internal/store/  (NEW methods, reuse existing SUM queries from RebuildCounters)
  CostSumToday(ctx, email) (float64, error)
  CostSumMonth(ctx, email) (float64, error)
  ReconcileCounterUp(ctx, email, period, pgSum, minDelta) (corrected bool, err)
    // GET current; if pgSum - current > minDelta: SET pgSum w/ correct EOD/EOM TTL

cmd/sentinel-workers/main.go  (MODIFY)
  reconciler := service.NewCounterReconciler(st, reg, cfg.Signals, slog.Default())
  go reconciler.Run(rootCtx)
```

## Config (add to SignalsConfig or a new ReconcileConfig)

| Key | Default | Meaning |
|---|---|---|
| `reconcile_enabled` | true | master switch |
| `reconcile_min_delta_usd` | 0.50 | only correct if PG sum exceeds Redis by this |
| `reconcile_min_delta_pct` | 0.01 | …or by this fraction, whichever applies |
| `reconcile_buckets` | 60 | shard count (== minutes spread) |

## Correctness notes

- **TTL preserved on correction.** When reconciling, re-`SET` with the correct
  remaining TTL (`EndOfDayUTC` / `EndOfMonthUTC`) so the day/month reset behavior
  is unchanged — never write the key without a TTL.
- **Idle engineers handled.** The key keeps its EOD/EOM TTL and survives idle
  periods; the cron corrects in place and never lets it drop to zero mid-period.
- **Month counter is heavier.** `SUM` over the month is a larger scan than the
  day. Sharding keeps even the month pass to ~N/60 engineers per minute.
- **Index.** Ensure `usage_events(engineer_id, occurred_at)` is indexed so the
  per-engineer day/month sums stay fast under the per-minute cadence.

## Out of scope

- Read-through reseed on dashboard GETs (separate concern; idle-then-expired keys
  reading 0 on the dashboard is a known gap but rare given EOD TTL).
- Redis outage fallback queue / circuit breaker for the broader system (tracked
  separately — this cron only addresses drift, not total Redis unavailability).
- Reconciling token counters (`tokens:today`) — same pattern can extend later;
  cost is the high-stakes one for budgets.

## Verification

1. Manually under-count: `DECRBYFLOAT engineer:<you>:cost:today 5` after some real
   spend so Redis is $5 below the Postgres sum.
2. Wait for that engineer's bucket minute (or temporarily set `reconcile_buckets:1`
   to run all every minute).
3. Confirm Redis counter snaps up to match `SELECT SUM(cost_usd) ... WHERE date=today`.
4. Inverse check: manually `INCRBYFLOAT` Redis *above* the PG sum → confirm the
   cron does **not** lower it (upward-only guard holds).
```