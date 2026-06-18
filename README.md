# Sentinel

[![CI](https://github.com/baohuy1303/enterprise-llm-tracker/actions/workflows/ci.yml/badge.svg)](https://github.com/baohuy1303/enterprise-llm-tracker/actions/workflows/ci.yml)
![Go](https://img.shields.io/badge/Go-1.26%2B-00ADD8?logo=go&logoColor=white)
![Next.js](https://img.shields.io/badge/Next.js-000000?logo=nextdotjs&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-336791?logo=postgresql&logoColor=white)
![Redis](https://img.shields.io/badge/Redis-DC382D?logo=redis&logoColor=white)
![Kafka](https://img.shields.io/badge/Kafka-231F20?logo=apachekafka&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?logo=docker&logoColor=white)
![Kubernetes](https://img.shields.io/badge/Kubernetes-326CE5?logo=kubernetes&logoColor=white)

AI usage management platform: ingests Claude Code OpenTelemetry data, attributes spend per engineer, fires Slack budget alerts, and joins spend against GitHub PR output for a cost-efficiency view.

<p align="center">
  <img src="screenshots/dashboard.png" alt="Dashboard" width="49%" />
  <img src="screenshots/leaderboard.png" alt="Leaderboard" width="49%" />
</p>

## Architecture

- **sentinel-api**: ingest HTTP endpoint (OTLP) + admin/dashboard REST API
- **sentinel-workers**: Kafka consumers (threshold checks, Postgres writes, signal detection) + nightly rollup jobs
- **Postgres, Redis, Kafka, OTel Collector**: storage and pipeline infra
- **sentinel-admin**: Next.js dashboard (see [sentinel-admin/README.md](sentinel-admin/README.md))

Code layout: `internal/http` (HTTP handlers) calls `internal/service` (business logic) calls `internal/store` (Postgres/Redis access). Entry points are `cmd/sentinel-api` and `cmd/sentinel-workers`.

## Prerequisites

- Go 1.26+
- Docker + Docker Compose
- `kubectl` + `helm` (for the Kubernetes path)

## Quick start (Docker Compose + local Go)

Compose brings up infra only (Postgres, Redis, Kafka, OTel Collector); the two Go binaries run locally so you can iterate fast.

```bash
docker compose up -d

cp sentinel.yaml.example sentinel.yaml   # adjust if you changed compose ports
export ADMIN_TOKEN=dev-token             # matches admin.token_env in sentinel.yaml

go run ./cmd/sentinel-api      # listens on :8081, runs DB migrations on startup
go run ./cmd/sentinel-workers  # Kafka consumers + nightly jobs, health on :8082
```

Send a test event with the included load generator:

```bash
go run ./cmd/loadgen --email you@example.com --kind cost --value 5
```

Health checks: `GET /healthz` and `GET /readyz` on both `:8081` (api) and `:8082` (workers).

## Kubernetes (Helm)

The chart in `charts/sentinel` bundles the full stack: sentinel-api, sentinel-workers, OTel Collector, Postgres, Redis, Kafka (KRaft), Prometheus, Grafana. Each piece is toggleable via `values.yaml`.

```bash
helm install sentinel ./charts/sentinel \
  --set secrets.adminToken=<token> \
  --set secrets.slackBotToken=<token> \
  --set secrets.githubToken=<token>
```

Point an infra `enabled: false` flag (e.g. `postgres.enabled=false`) at a managed equivalent instead of the bundled one, and override its connection details in `values.yaml`.

## Configuration

All runtime config lives in `sentinel.yaml` (see `sentinel.yaml.example` for every key). Secrets (Slack bot token, GitHub PAT, admin bearer token) are read from environment variables named in the config, never written to the file directly.

## Setup

Get the stack running first (Quick start or Kubernetes above), then onboard your team.

**Managers: use the dashboard.** Run sentinel-admin (`npm run dev` in `sentinel-admin/`, http://localhost:3000) to register engineers (**Engineers → New**), and view the **Leaderboard** and **Signals** pages for spend ranking and burst/anomaly alerts. Unattributed events are dropped silently if an engineer isn't registered first.

Or register via the API directly (useful for scripting bulk onboarding):

```bash
curl -X POST http://localhost:8081/admin/engineers \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "email": "alice@yourcompany.com",
    "name": "Alice Smith",
    "github_username": "alice-gh",
    "slack_user_id": "U012ABC3DE",
    "daily_budget_usd": 25,
    "monthly_budget_usd": 500,
    "team": "platform"
  }'
```

Budgets default to $25/day and $500/month if omitted. `slack_user_id`/`manager_slack_id` are optional: without them, threshold alerts are logged instead of DM'd. Full CRUD: `GET/PUT/DELETE /admin/engineers/{email}`.

**Platform team: Grafana.** The Helm chart bundles Prometheus + Grafana for inspecting cluster/pod health, HPA scaling, and service metrics (see `charts/sentinel/templates/grafana.yaml`; default admin password is set via `values.yaml`).

**Developers: point Claude Code at the collector.** Add to your Claude Code settings (env vars, or the `env` block in `~/.claude/settings.json`):

```bash
CLAUDE_CODE_ENABLE_TELEMETRY=1
OTEL_METRICS_EXPORTER=otlp
OTEL_LOGS_EXPORTER=otlp
OTEL_EXPORTER_OTLP_PROTOCOL=grpc
OTEL_EXPORTER_OTLP_ENDPOINT=http://<collector-host>:4317
```

Your email must already be registered by a manager (above) and must match the `user.email` Claude Code attaches, or events drop as unattributed. Restart Claude Code after changing settings. Check Claude Code's telemetry docs for the current env var names if these don't take effect.

## Tested at scale

Open-loop load test (`cmd/loadtest`) against the ingest hot path, one `sentinel-api` replica:

| Engineers | Target RPS | Actual RPS | Events/sec | p95 (ms) | p99 (ms) |
|---|---|---|---|---|---|
| 300 | 1,500 | 1,495 | 7,477 | 4.64 | 6.24 |
| 450 | 2,250 | 2,245 | 11,224 | 9.41 | 15.55 |
| **600** | **3,000** | **2,969** | **14,844** | **43.78** | **58.62** |

Zero errors, zero overruns, zero message loss across all three runs.

## Tests & lint

```bash
go test ./...
golangci-lint run ./...
```
