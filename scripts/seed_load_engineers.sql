-- =============================================================================
-- seed_load_engineers.sql — bulk synthetic engineers for STRESS / LOAD testing.
--
-- Inserts N active engineers  loadtest-0001@sentinel.local .. loadtest-NNNN@...
-- so that cmd/loadtest traffic is *attributed* (an unattributed event is dropped
-- before it ever touches Redis/Kafka — see internal/service/ingest.go
-- lookupEngineer). Spreading load across many engineer ids also fans Redis
-- counter keys across hash slots and produces many distinct Postgres rows,
-- instead of hammering one key/row.
--
-- Unlike seed_test_data.sql this writes NO backdated history — these engineers
-- exist only to receive live load-test traffic. Budgets are set absurdly high so
-- the threshold-checker doesn't fire a Slack DM storm during a load run.
--
-- Idempotent: removes any prior loadtest-% engineers first. Safe to re-run.
--
-- Usage (psql):
--   psql "$DATABASE_URL" -v n=200 -f scripts/seed_load_engineers.sql
-- Default N is 200 if -v n=... is omitted.
-- After seeding, wait one registry refresh interval (default 30s, or
-- registry.refreshIntervalSeconds in your config) before starting the load run,
-- or POST /admin/registry/refresh to pick them up immediately.
-- =============================================================================

\if :{?n}
\else
  \set n 200
\endif

BEGIN;

DELETE FROM usage_events WHERE engineer_id    LIKE 'loadtest-%@sentinel.local';
DELETE FROM engineers    WHERE email          LIKE 'loadtest-%@sentinel.local';

INSERT INTO engineers
  (email, name, github_username, slack_user_id, manager_slack_id,
   daily_budget_usd, monthly_budget_usd, team, active)
SELECT
  format('loadtest-%s@sentinel.local', lpad(g::text, 4, '0')),
  format('Load Test %s', lpad(g::text, 4, '0')),
  format('loadtest-%s', lpad(g::text, 4, '0')),
  NULL, NULL,
  1000000, 100000000,                 -- budgets high enough to never alert
  'loadtest',
  TRUE
FROM generate_series(1, :n) AS g;

COMMIT;

-- ---- readback ---------------------------------------------------------------
SELECT count(*) AS loadtest_engineers
FROM engineers
WHERE email LIKE 'loadtest-%@sentinel.local';
