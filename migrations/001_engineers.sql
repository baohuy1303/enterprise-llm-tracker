CREATE TABLE IF NOT EXISTS engineers (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  email TEXT NOT NULL UNIQUE,
  name TEXT NOT NULL,
  github_username TEXT NOT NULL UNIQUE,
  slack_user_id TEXT,
  manager_slack_id TEXT,
  daily_budget_usd NUMERIC(10, 2) NOT NULL DEFAULT 25.00,
  monthly_budget_usd NUMERIC(10, 2) NOT NULL DEFAULT 500.00,
  team TEXT,
  active BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_engineers_email ON engineers(email);
CREATE INDEX IF NOT EXISTS ix_engineers_github ON engineers(github_username);
