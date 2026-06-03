-- =============================================================================
-- seed_test_data.sql — synthetic warehouse history for the Stage 0–9 test.
--
-- Inserts 3 fake engineers with ~20 days of BACKDATED usage_events + github_prs,
-- modeled on the shape of the real rows already in the DB. Backdated history is
-- written straight to Postgres (it represents data already accumulated — you
-- cannot replay it through the live hot path without corrupting today's Redis
-- counters). Live "today" traffic is generated separately by cmd/loadgen.
--
-- Idempotent: deletes anything from a prior run first. Safe to re-run.
-- The 3 fakes use YOUR slack id for both slack_user_id and manager_slack_id, so
-- burst-critical / threshold DMs land in your Slack DMs during the test.
--
-- Profiles (so the leaderboard + peer percentile are meaningful):
--   test-alice  cheap + ships    → low  $/merged-PR  (efficient)
--   test-bob    mid                → mid  $/merged-PR
--   test-carol  pricey + few merges→ high $/merged-PR (inefficient)
-- =============================================================================

\set slack '\'U0B5VKZ9LMR\''

BEGIN;

-- ---- clean prior test rows ---------------------------------------------------
DELETE FROM signal_events    WHERE engineer_id    LIKE 'test-%@sentinel.local';
DELETE FROM engineer_signals WHERE engineer_id    LIKE 'test-%@sentinel.local';
DELETE FROM usage_events     WHERE engineer_id    LIKE 'test-%@sentinel.local';
DELETE FROM github_prs       WHERE engineer_github LIKE 'test-%';
DELETE FROM engineers        WHERE email          LIKE 'test-%@sentinel.local';

-- ---- 3 fake engineers --------------------------------------------------------
INSERT INTO engineers
  (email, name, github_username, slack_user_id, manager_slack_id,
   daily_budget_usd, monthly_budget_usd, team, active)
VALUES
  ('test-alice@sentinel.local','Test Alice','test-alice', :slack, :slack, 25, 500,'platform',     TRUE),
  ('test-bob@sentinel.local',  'Test Bob',  'test-bob',   :slack, :slack, 25, 500,'platform',     TRUE),
  ('test-carol@sentinel.local','Test Carol','test-carol', :slack, :slack, 25, 500,'infrastructure',TRUE);

-- ---- backdated COST events: 20 days, work hours 14–17 UTC, 2 models ----------
-- Per-day variance (random multiplier) gives a non-zero daily stddev → z-score.
INSERT INTO usage_events
  (engineer_id, occurred_at, source, metric_name, cost_usd, model, event_id)
SELECT e.email,
       date_trunc('day', now() AT TIME ZONE 'utc')
         - make_interval(days => d)
         + make_interval(hours => h)
         + make_interval(mins  => (random()*50)::int),
       'otel_metric', 'claude_code.cost.usage',
       round((e.base * (0.6 + random()*0.9))::numeric, 6),
       (ARRAY['claude-opus-4-7','claude-sonnet-4-6'])[1 + floor(random()*2)::int],
       gen_random_uuid()
FROM (VALUES
        ('test-alice@sentinel.local', 0.40),
        ('test-bob@sentinel.local',   0.80),
        ('test-carol@sentinel.local', 1.40)
     ) AS e(email, base)
CROSS JOIN generate_series(1,20) AS d
CROSS JOIN (VALUES (14),(15),(16),(17)) AS hh(h);

-- ---- a FEW cold-hour (03:00 UTC) events so hour-3 holds <5% (not 0%) ---------
-- Rhythm-break only fires for buckets with 0 < fraction < rhythm_break_pct.
INSERT INTO usage_events
  (engineer_id, occurred_at, source, metric_name, cost_usd, model, event_id)
SELECT e.email,
       date_trunc('day', now() AT TIME ZONE 'utc') - make_interval(days => d) + make_interval(hours => 3),
       'otel_metric', 'claude_code.cost.usage', 0.10, 'claude-sonnet-4-6', gen_random_uuid()
FROM (VALUES ('test-alice@sentinel.local'),('test-bob@sentinel.local'),('test-carol@sentinel.local')) AS e(email)
CROSS JOIN generate_series(1,2) AS d;

-- ---- TOKEN events (cache read/creation) → cache_hit_ratio --------------------
INSERT INTO usage_events
  (engineer_id, occurred_at, source, metric_name,
   tokens_input, tokens_output, tokens_cache_read, tokens_cache_creation, model, event_id)
SELECT e.email,
       date_trunc('day', now() AT TIME ZONE 'utc') - make_interval(days => d) + make_interval(hours => h),
       'otel_metric', 'claude_code.token.usage',
       (random()*3000)::int, (random()*2000)::int,
       (random()*6000)::int, (random()*1500)::int,
       'claude-opus-4-7', gen_random_uuid()
FROM (VALUES ('test-alice@sentinel.local'),('test-bob@sentinel.local'),('test-carol@sentinel.local')) AS e(email)
CROSS JOIN generate_series(1,20) AS d
CROSS JOIN (VALUES (14),(15),(16),(17)) AS hh(h);

-- ---- LINES_OF_CODE events (>=100 rows each → dollars_per_kloc populates) ------
INSERT INTO usage_events
  (engineer_id, occurred_at, source, metric_name, event_id)
SELECT e.email,
       date_trunc('day', now() AT TIME ZONE 'utc') - make_interval(days => d) + make_interval(hours => h),
       'otel_metric', 'claude_code.lines_of_code.count', gen_random_uuid()
FROM (VALUES ('test-alice@sentinel.local'),('test-bob@sentinel.local'),('test-carol@sentinel.local')) AS e(email)
CROSS JOIN generate_series(1,20) AS d
CROSS JOIN (VALUES (14),(15),(16),(17),(18),(19)) AS hh(h);

-- ---- github_prs --------------------------------------------------------------
-- alice: 6 merged (0 reverted) + 1 closed-unmerged   → effective 6
INSERT INTO github_prs
  (engineer_github, repo, pr_number, title, state, created_at, merged_at,
   review_count, files_changed, reverted, last_synced_at, files)
SELECT 'test-alice','sentinel/alice', n, 'feat alice '||n, 'MERGED',
       now()-make_interval(days => 20-n), now()-make_interval(days => 19-n),
       2, 4, FALSE, now(), '[]'::jsonb
FROM generate_series(1,6) AS n;
INSERT INTO github_prs
  (engineer_github, repo, pr_number, title, state, created_at, merged_at,
   review_count, files_changed, reverted, last_synced_at, files)
VALUES ('test-alice','sentinel/alice',7,'wip alice','CLOSED', now()-interval '4 days', NULL,1,2,FALSE,now(),'[]'::jsonb);

-- bob: 4 merged (1 reverted) + 1 closed-unmerged      → effective 3
INSERT INTO github_prs
  (engineer_github, repo, pr_number, title, state, created_at, merged_at,
   review_count, files_changed, reverted, reverted_at, last_synced_at, files)
SELECT 'test-bob','sentinel/bob', n, 'feat bob '||n, 'MERGED',
       now()-make_interval(days => 18-n), now()-make_interval(days => 17-n),
       1, 6, (n = 1), CASE WHEN n = 1 THEN now()-interval '2 days' END, now(), '[]'::jsonb
FROM generate_series(1,4) AS n;
INSERT INTO github_prs
  (engineer_github, repo, pr_number, title, state, created_at, merged_at,
   review_count, files_changed, reverted, last_synced_at, files)
VALUES ('test-bob','sentinel/bob',5,'wip bob','CLOSED', now()-interval '6 days', NULL,0,3,FALSE,now(),'[]'::jsonb);

-- carol: 2 merged (1 reverted) + 2 closed-unmerged    → effective 1
INSERT INTO github_prs
  (engineer_github, repo, pr_number, title, state, created_at, merged_at,
   review_count, files_changed, reverted, reverted_at, last_synced_at, files)
SELECT 'test-carol','sentinel/carol', n, 'feat carol '||n, 'MERGED',
       now()-make_interval(days => 16-n), now()-make_interval(days => 15-n),
       1, 9, (n = 1), CASE WHEN n = 1 THEN now()-interval '1 day' END, now(), '[]'::jsonb
FROM generate_series(1,2) AS n;
INSERT INTO github_prs
  (engineer_github, repo, pr_number, title, state, created_at, merged_at,
   review_count, files_changed, reverted, last_synced_at, files)
VALUES
  ('test-carol','sentinel/carol',3,'wip carol a','CLOSED', now()-interval '5 days', NULL,0,4,FALSE,now(),'[]'::jsonb),
  ('test-carol','sentinel/carol',4,'wip carol b','CLOSED', now()-interval '3 days', NULL,0,5,FALSE,now(),'[]'::jsonb);

COMMIT;

-- ---- quick readback ----------------------------------------------------------
SELECT engineer_id,
       count(*)                                                    AS events,
       round(sum(cost_usd)::numeric,2)                             AS cost_usd,
       count(*) FILTER (WHERE metric_name='claude_code.lines_of_code.count') AS loc_rows
FROM usage_events
WHERE engineer_id LIKE 'test-%@sentinel.local'
GROUP BY engineer_id ORDER BY engineer_id;

SELECT engineer_github,
       count(*) FILTER (WHERE state='MERGED')                  AS merged,
       count(*) FILTER (WHERE reverted)                        AS reverted,
       count(*) FILTER (WHERE state='CLOSED' AND merged_at IS NULL) AS closed_unmerged
FROM github_prs WHERE engineer_github LIKE 'test-%'
GROUP BY engineer_github ORDER BY engineer_github;
