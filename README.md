# Sentinel

AI usage management platform: ingests Claude Code OpenTelemetry data, attributes spend per engineer, fires Slack budget alerts, and joins spend against GitHub PR output for a cost-efficiency view.

## Architecture

- **sentinel-api** — ingest HTTP endpoint (OTLP) + admin/dashboard REST API
- **sentinel-workers** — Kafka consumers (threshold checks, Postgres writes, signal detection) + nightly rollup jobs
- **Postgres, Redis, Kafka, OTel Collector** — storage and pipeline infra
- **sentinel-admin** — Next.js dashboard (see [sentinel-admin/README.md](sentinel-admin/README.md))

Code layout: `internal/http` (HTTP handlers) → `internal/service` (business logic) → `internal/store` (Postgres/Redis access). Entry points are `cmd/sentinel-api` and `cmd/sentinel-workers`.

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

The chart in `charts/sentinel` bundles the full stack — sentinel-api, sentinel-workers, OTel Collector, Postgres, Redis, Kafka (KRaft), Prometheus, Grafana — each toggleable via `values.yaml`.

```bash
helm install sentinel ./charts/sentinel \
  --set secrets.adminToken=<token> \
  --set secrets.slackBotToken=<token> \
  --set secrets.githubToken=<token>
```

Point an infra `enabled: false` flag (e.g. `postgres.enabled=false`) at a managed equivalent instead of the bundled one, and override its connection details in `values.yaml`.

## Configuration

All runtime config lives in `sentinel.yaml` (see `sentinel.yaml.example` for every key). Secrets (Slack bot token, GitHub PAT, admin bearer token) are read from environment variables named in the config, never written to the file directly.

## Tests & lint

```bash
go test ./...
golangci-lint run ./...
```
