# Stage 11 — Kubernetes / Helm Walkthrough

This documents what's in `charts/sentinel/` and the two Go changes made to
support it. Read top to bottom — later sections assume earlier ones.

---

## 1. Two code fixes (made *before* the chart)

### `internal/migrate/migrate.go` — Postgres advisory lock

**Problem:** `sentinel-api` runs DB migrations on startup. With 2 replicas
starting at once, both would race to apply the same `.sql` files.

**Fix:** `Apply()` now grabs a Postgres **session-level advisory lock**
before doing anything:

```go
const migrateLockKey int64 = 20240601

conn, _ := db.Acquire(ctx)              // dedicated connection, not pool-shared
conn.Exec(ctx, "SELECT pg_advisory_lock($1)", migrateLockKey)
defer conn.Exec(context.Background(), "SELECT pg_advisory_unlock($1)", migrateLockKey)
```

- `pg_advisory_lock` blocks until free — the second replica just waits.
- Lock lives on **one dedicated connection** (`db.Acquire`), because returning
  a connection to the pool does *not* release a session lock.
- Unlock uses `context.Background()` so a cancelled startup can't strand the lock.
- `migrateLockKey` is an arbitrary fixed number — it's the only advisory lock
  the app takes, so any constant works.

**Verified in the real deploy:** one replica logged
`migrations applied: [001...005]`, the other logged nothing — exactly the
expected behavior.

### `cmd/sentinel-workers/main.go` — `/healthz` + `/readyz`

**Problem:** `sentinel-workers` is a pure background process (Kafka consumers
+ cron goroutines) — no HTTP server, so Kubernetes had nothing to probe.

**Fix:** added a tiny HTTP server (default `:8082`, override via
`WORKERS_HEALTH_ADDR`):

- `GET /healthz` → always `200 ok` (liveness — "is the process alive")
- `GET /readyz` → pings Postgres + Redis (2s timeout each), `200` or `503`
  (readiness — "can it actually do work")
- Shut down gracefully (5s timeout) before the rest of the app exits.

This is what the `sentinel-workers` Deployment's probes point at.

---

## 2. Chart layout

```
charts/sentinel/
  Chart.yaml              # name, version
  values.yaml             # every tunable — the "dial board"
  .helmignore
  templates/
    _helpers.tpl          # shared template snippets
    config.yaml           # sentinel.yaml ConfigMap + secrets Secret
    postgres.yaml          redis.yaml          kafka.yaml
    otelcol.yaml
    sentinel-api.yaml      sentinel-workers.yaml
    prometheus.yaml        grafana.yaml
    NOTES.txt              # printed after install/upgrade
```

One Deployment + Service per infra component (postgres/redis/kafka/otelcol/
prometheus/grafana), plus the two app components (api/workers). 8 services
total, all in one chart, one release.

---

## 3. `_helpers.tpl` — shared building blocks

Four reusable snippets, used via `{{ include "name" . }}`:

- **`sentinel.name`** → `"sentinel"` (or `nameOverride`). Used for the
  `app.kubernetes.io/name` label.

- **`sentinel.fullname`** → defaults to `.Release.Name` (e.g. `sentinel`).
  **This is the single source of truth for every resource name and every
  in-cluster DNS name** — `sentinel-postgres`, `sentinel-redis`,
  `sentinel-kafka`, `sentinel-api`, etc. Both the rendered `sentinel.yaml`
  and Kafka's advertised-listener address derive from this same helper, so
  they can never disagree.

- **`sentinel.labels`** → the standard `app.kubernetes.io/*` labels
  (`name`, `instance`, `version`, `managed-by`, `helm.sh/chart`). Applied to
  every object's own `metadata.labels` (Deployment, Service, ConfigMap...).
  ⚠️ **Not** applied to pod templates — pods use a simpler `app: <fullname>-<component>`
  label instead (used for `selector.matchLabels`). This is why
  `kubectl get pods -l app.kubernetes.io/instance=sentinel` returns nothing —
  a known inconsistency, harmless for a single release.

- **`sentinel.waitForDeps`** → a shared `initContainer` (busybox) used by
  both `sentinel-api` and `sentinel-workers`:
  ```sh
  until nc -z sentinel-postgres 5432; do sleep 2; done
  until nc -z sentinel-redis 6379; do sleep 2; done
  until nc -z sentinel-kafka 9092; do sleep 2; done
  sleep 3   # let Kafka finish settling after the port opens
  ```
  Without this, the app container could start (and run migrations / call
  Kafka `EnsureTopic`) before its dependencies are reachable.

---

## 4. `values.yaml` — the dial board

Every tunable lives here, grouped by component. Highlights:

- **`secrets:`** — `adminToken`, `slackBotToken`, `githubToken`,
  `postgresPassword`. **Always empty/placeholder in this file** — real values
  go in via `--set` at install time, never committed. (We learned this the
  hard way — a real Slack token briefly ended up here and had to be removed
  before committing.)

- **`<component>.enabled`** — `postgres`, `redis`, `kafka`, `prometheus`,
  `grafana` each have one. Set to `false` to skip deploying that piece (e.g.
  point at a managed Postgres instead). `sentinel-api`/`workers`/`otelcol`
  have no flag — they're always deployed (it's the actual app).

- **`api.image.repository: ghcr.io/baohuy1303/sentinel-api`** +
  **`pullPolicy: IfNotPresent`** — this is the real image your CI pushes to
  GHCR. For minikube, we **build the same name+tag locally**
  (`minikube image build -t ghcr.io/.../sentinel-api:latest ...`), so
  `IfNotPresent` finds it already present and never touches the network. In
  a real cluster (no local image with that name), it would actually pull
  from GHCR.

- **`workers.replicas: 1`** — pinned, with a comment explaining why: the
  in-process cron goroutines (baseline rebuild, efficiency rollup, GitHub
  collector) aren't leader-elected. Two replicas would double-fire them.

- **`api.hpa`** — `minReplicas: 2`, `maxReplicas: 5`, `70%` CPU target.

- **`config:`** — mirrors `sentinel.yaml.example`'s tunables (thresholds,
  signals, github org/repos, etc.). Infra *addresses* (postgres/redis/kafka
  hosts) are **not** here — they're derived automatically from
  `sentinel.fullname` in `config.yaml`.

---

## 5. `config.yaml` — ConfigMap + Secret

Two Kubernetes objects in one file:

**ConfigMap `<fullname>-config`** — renders the entire `sentinel.yaml` that
both `sentinel-api` and `sentinel-workers` read. Every infra address is
built from `sentinel.fullname`, e.g.:
```yaml
postgres:
  url: "postgres://sentinel:{{ .Values.secrets.postgresPassword }}@sentinel-postgres:5432/sentinel?sslmode=disable"
redis:
  addr: "sentinel-redis:6379"
kafka:
  brokers: ["sentinel-kafka:9092"]
```

One important conditional — **`slack.bot_token_env`**:
```yaml
bot_token_env: {{ if .Values.secrets.slackBotToken }}"SLACK_BOT_TOKEN"{{ else }}""{{ end }}
```
**Why this matters:** `internal/config.Validate()` *fatally* errors if
`bot_token_env` points at an env var that's empty. The Secret always injects
`SLACK_BOT_TOKEN` (even if `""`), so if `bot_token_env` were hardcoded to
`"SLACK_BOT_TOKEN"`, a no-Slack install would **crash-loop both
sentinel-api and sentinel-workers forever**. Making it conditional means: no
token configured → `bot_token_env: ""` → validation skipped → app boots and
just logs instead of DMing. This was caught and fixed before the first deploy.

**Secret `<fullname>-secrets`** — `stringData` (auto-base64'd) holding
`ADMIN_TOKEN`, `SLACK_BOT_TOKEN`, `GITHUB_TOKEN`, `POSTGRES_PASSWORD`. The
config above references these by *name* (`*_token_env` fields), the actual
values are injected as env vars in the Deployments.

---

## 6. Infra services — postgres / redis / kafka / otelcol

All four follow the same shape: `{{- if .Values.X.enabled }} ... {{- end }}`,
one Deployment + one Service, `replicas: 1`.

**`postgres.yaml`**
- A `PersistentVolumeClaim` (5Gi, RWO) — actual data survives pod restarts.
- `strategy: { type: Recreate }` — an RWO volume can't attach to two pods at
  once, so the old pod must fully terminate before the new one starts (vs.
  the default `RollingUpdate` which briefly runs both).
- `PGDATA=/var/lib/postgresql/data/pgdata` — a subdirectory of the mount, so
  Postgres doesn't choke on the volume's auto-created `lost+found/` dir.
- Probes: `pg_isready`.

**`redis.yaml`**
- Simplest file in the chart. No PVC — Redis here is a *derived cache*,
  rebuilt from Postgres on demand, so losing it on restart is fine.
- Probes: `redis-cli ping`.

**`kafka.yaml`** — single-broker KRaft (no Zookeeper). The two env vars that
matter most:
```yaml
KAFKA_ADVERTISED_LISTENERS: "PLAINTEXT://sentinel-kafka:9092"
KAFKA_CONTROLLER_QUORUM_VOTERS: "1@localhost:9093"
```
- **Advertised listener = Service DNS** (`sentinel-kafka:9092`). This is the
  address Kafka hands back to clients during the bootstrap→metadata→reconnect
  flow. If it were `localhost`, every other pod's reconnect would resolve to
  *itself* and fail.
- **Controller quorum = `localhost`**, deliberately. Broker and controller
  are the same process in this single pod. Using Service DNS here would
  deadlock: a ClusterIP only routes to pods that are already `Ready`, but the
  pod can't become `Ready` until its controller (itself) is reachable —
  chicken-and-egg.
- `KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"` — topics are created explicitly
  by `sentinel-api` on startup (`EnsureTopic`), which is why `waitForDeps`
  matters: if the API's `EnsureTopic` call races Kafka's startup, the
  12-partition topic config would be lost.
- Probes: `tcpSocket` on the client port (Kafka has no HTTP healthcheck).

**`otelcol.yaml`** — ConfigMap + Deployment + Service (`type: LoadBalancer`,
the only externally-reachable component). The collector config:
- **receivers**: `otlp` (gRPC `:4317`, HTTP `:4318`) — what Claude Code sends to.
- **processor `filter/sentinel`**: drops every metric except `claude_code.*`.
- **exporters**:
  - `debug` — dumps to `kubectl logs` for sanity-checking.
  - `prometheus` (`:8889`) — re-exposes `claude_code.*` as scrapeable metrics.
  - `otlphttp/sentinel` → `http://sentinel-api:8081/ingest/otel` — forwards
    to the Go ingest handler. *(Note: the repo's root `collector-config.yaml`
    used for local docker-compose has this exporter misnamed `otlp_http`
    — not a valid type. The chart's version uses the correct `otlphttp`.
    The root file hasn't been fixed — separate cleanup item.)*
- `checksum/config` annotation — hashes `.Values.otelcol` so the pod restarts
  if its config-relevant values change. (Originally written as a self-include
  of this same file, which caused infinite recursion in `helm lint` — fixed
  to hash the values subtree instead.)

---

## 7. The application — sentinel-api / sentinel-workers

**`sentinel-api.yaml`**
- `replicas: {{ .Values.api.replicas }}` (2).
- `checksum/config` annotation = hash of the **rendered `config.yaml`
  template** (`include (print $.Template.BasePath "/config.yaml") .`) — if
  you change anything in `values.config` or the secrets, this hash changes,
  Kubernetes sees a pod-template diff, and does a rolling restart. Without
  this, a `helm upgrade` that only changes the ConfigMap's *contents* (same
  object name) wouldn't trigger a restart — pods would keep running with the
  old config until manually killed.
- `initContainers: [waitForDeps]` — blocks until postgres/redis/kafka are up.
- Env: `SENTINEL_CONFIG`, `SENTINEL_MIGRATIONS_DIR`, plus
  `ADMIN_TOKEN`/`SLACK_BOT_TOKEN`/`GITHUB_TOKEN` from the Secret.
- Probes: `livenessProbe` → `/healthz`, `readinessProbe` → `/readyz` (checks
  Postgres + Redis — confirmed working: returned
  `{"postgres":"ok","redis":"ok","status":"ok",...}`).
- Service on `:8081`.
- Conditional **HPA** (`autoscaling/v2`) — scales `2→5` replicas on `70%` CPU.
  Confirmed working: `kubectl get hpa` showed `cpu: 1%/70%` with real numbers
  from `metrics-server`.

**`sentinel-workers.yaml`**
- Same shape, but `replicas: {{ .Values.workers.replicas }}` (pinned to 1,
  see values.yaml section above).
- No Service — nothing else needs to reach it; its health port is only for
  kubelet probes (reach it directly via `kubectl port-forward pod/...` if
  needed).
- Env adds `WORKERS_HEALTH_ADDR=":8082"`.
- Probes hit the `/healthz`/`/readyz` added in the Go fix (Section 1).

---

## 8. Observability — prometheus / grafana

**`prometheus.yaml`**
- ConfigMap with `prometheus.yml` — 3 scrape jobs:
  - `sentinel-api:8081/metrics` (Go runtime + promhttp metrics — the
    `sentinel_*` custom metrics from the original plan aren't instrumented
    yet, so this is mostly goroutines/memory/GC stats for now).
  - `sentinel-otelcol:8889` (the `claude_code.*` product metrics).
  - itself (`localhost:9090`).
- Storage: `emptyDir` — metrics are lost on pod restart. Fine for a demo;
  add a PVC for anything long-lived.
- Probes: `/-/ready`, `/-/healthy`.
- `checksum/config: {{ .Values.prometheus | toYaml | sha256sum }}` — same
  recursion-avoidance fix as otelcol.

**`grafana.yaml`** — 3 ConfigMaps + 1 Deployment + 1 Service:
- `datasource.yaml` — registers Prometheus, **fixed `uid: sentinel-prom`**
  (so dashboard panels can reference it by a stable ID regardless of
  datasource name).
- `provider.yaml` — tells Grafana to auto-load dashboards from
  `/var/lib/grafana/dashboards`.
- `sentinel.json` — one dashboard, "Sentinel — Pipeline Health", 4 panels:
  Targets Up (`up`), goroutines, resident memory, scrape duration — all
  using metrics that actually exist today.
- **Resource limit bumped 256Mi → 512Mi** after a real OOMKill in testing:
  it ran fine for ~4.5 hours idle, then got killed the moment the dashboard
  was first loaded (provisioning + datasource queries spiked memory past
  256Mi).
- Probes: `/api/health`.

---

## 9. `NOTES.txt` — post-install instructions

Printed after every `helm install`/`helm upgrade`. Walks through:
1. Watch pods come up (`kubectl get pods -w` — *not* the
   `-l app.kubernetes.io/instance=...` filter shown in one line, which
   doesn't match pods — see Section 3 caveat).
2. Check the HPA.
3. Port-forward the API and hit `/healthz`.
4. Port-forward Grafana (if enabled).
5. Get the otelcol LoadBalancer's external IP (needs `minikube tunnel`) and
   point `OTEL_EXPORTER_OTLP_ENDPOINT` at it.
6. Conditional warning if `secrets.adminToken` is empty.

All template expressions in this file (`{{ include ... }}`) are rendered by
Helm into real values — copy the command from the *output* of
`helm install`/`helm upgrade`/`helm get notes <release>`, not from the raw
file.

---

## 10. End-to-end deploy sequence (what we actually ran)

```powershell
minikube start --driver=docker --cpus=4 --memory=6g --disk-size=20g
minikube addons enable metrics-server

minikube image build -t ghcr.io/baohuy1303/sentinel-api:latest -f Dockerfile.api .
minikube image build -t ghcr.io/baohuy1303/sentinel-workers:latest -f Dockerfile.workers .

helm install sentinel ./charts/sentinel
kubectl get pods -w
```

Result: all 8 pods reached `1/1 Running`. Postgres became ready first, then
Kafka (slowest — KRaft startup), then the `wait-for-deps` init containers on
api/workers passed and the apps started. One API replica applied all 5
migrations under the advisory lock; the other found nothing to do.

---

## Key concepts recap

| Concept | Where | Why |
|---|---|---|
| `sentinel.fullname` | `_helpers.tpl` | one name → consistent DNS everywhere |
| `waitForDeps` initContainer | api, workers | don't race Postgres/Redis/Kafka startup |
| `checksum/config` annotation | api, workers, otelcol, prometheus, grafana | force rolling restart when config changes |
| `IfNotPresent` + matching image name | values.yaml | local minikube build shadows the real GHCR image |
| Conditional `bot_token_env` | config.yaml | avoid fatal config-validation crash-loop with no Slack token |
| `pg_advisory_lock` | migrate.go | safe migrations across 2 API replicas |
| `/healthz` + `/readyz` | sentinel-workers main.go | gives kubelet something to probe |
| `<component>.enabled` flags | values.yaml | swap bundled infra for managed services later |

---

## 11. Command reference

### Cluster lifecycle (minikube)

```powershell
minikube start --driver=docker --cpus=4 --memory=6g --disk-size=20g
minikube status                    # is it running?
minikube stop                      # stop the VM/container, keeps all state
minikube start                     # resume a stopped cluster
minikube delete                    # nuke everything, start fresh next time

minikube addons enable metrics-server   # required for HPA to show real numbers
minikube addons list

minikube image build -t <name>:<tag> -f <Dockerfile> .   # build into minikube's image store
minikube image ls                  # list images minikube has cached

minikube tunnel                    # foreground; makes LoadBalancer Services get a real, routable EXTERNAL-IP
minikube dashboard                 # opens the web UI
```

### Helm — release lifecycle

```powershell
helm install sentinel ./charts/sentinel              # first install
helm upgrade sentinel ./charts/sentinel              # apply changes to values.yaml/templates
helm upgrade --install sentinel ./charts/sentinel    # install if missing, upgrade if present
helm uninstall sentinel                              # tear down everything the release created

helm list                          # releases in current namespace
helm status sentinel               # current state + re-print NOTES.txt
helm get notes sentinel            # just the rendered NOTES.txt
helm get values sentinel           # values actually in use (merged with --set overrides)
helm get manifest sentinel         # every K8s object this release rendered

helm history sentinel              # revision history
helm rollback sentinel <REVISION>  # roll back to a previous revision
```

### Helm — authoring / dry-run (no cluster needed)

```powershell
helm lint ./charts/sentinel                          # static checks on templates
helm template sentinel ./charts/sentinel             # render to stdout, eyeball the YAML
helm template sentinel ./charts/sentinel --set secrets.adminToken=x   # render with overrides
helm install sentinel ./charts/sentinel --dry-run --debug             # render + validate against the live cluster
```

### Pods

```powershell
kubectl get pods                   # list, with READY/STATUS/RESTARTS/AGE
kubectl get pods -w                # same, but live-streams changes (Ctrl+C to stop)
kubectl get pods -o wide           # + node + pod IP

kubectl describe pod <name>        # full detail: events, probe config, last termination reason
kubectl logs <name>                # current container's logs
kubectl logs <name> -f             # follow/stream
kubectl logs <name> -c <container> # specific container (e.g. wait-for-deps init container)
kubectl logs <name> --previous     # logs from BEFORE the last restart (crash investigation)

kubectl exec -it <name> -- sh      # shell into a pod (if the image has one — distroless images don't)
kubectl delete pod <name>          # force-kill; Deployment immediately recreates it

kubectl port-forward <name> 8081:8081          # pod-direct
kubectl port-forward svc/<name> 8081:8081      # via Service (picks a ready pod for you)
```

Shortcuts that avoid typing the random pod-hash suffix (works when the
Deployment has exactly one pod, or you don't care which):
```powershell
kubectl get pods -l app=sentinel-grafana
kubectl logs deploy/sentinel-grafana
kubectl describe pod -l app=sentinel-grafana
```

### Deployments / rollouts

```powershell
kubectl get deployments
kubectl describe deployment <name>
kubectl scale deployment <name> --replicas=3
kubectl rollout status deployment/<name>     # watch a rollout finish
kubectl rollout history deployment/<name>
kubectl rollout undo deployment/<name>       # roll back to previous pod template
kubectl rollout restart deployment/<name>    # force new pods without changing the spec
```

### Services / networking

```powershell
kubectl get svc                    # ClusterIP / LoadBalancer / NodePort + ports
kubectl describe svc <name>        # endpoints (which pod IPs it's routing to)
kubectl get endpoints <name>       # just the pod IPs behind a Service
```

### Nodes / cluster-wide

```powershell
kubectl get nodes
kubectl describe node <name>       # capacity, allocatable, what's scheduled on it
kubectl top nodes                  # CPU/memory usage (needs metrics-server)
kubectl top pods                   # per-pod CPU/memory (needs metrics-server)
kubectl get events --sort-by=.lastTimestamp   # cluster-wide event feed, newest last
```

### ConfigMaps / Secrets / HPA

```powershell
kubectl get configmaps
kubectl get configmap <name> -o yaml         # see the rendered sentinel.yaml etc.

kubectl get secrets
kubectl get secret <name> -o jsonpath="{.data.ADMIN_TOKEN}" | base64 -d   # decode a secret value

kubectl get hpa                    # current/target CPU, replica counts
kubectl describe hpa <name>        # scaling events history
```

### "Something's wrong" checklist

```powershell
kubectl get pods                                   # who's not Running/Ready?
kubectl describe pod <name>                        # check Events at the bottom + Last State
kubectl logs <name>                                # app-level errors
kubectl logs <name> -c wait-for-deps               # stuck in Init? check this
kubectl logs <name> --previous                     # crashed and restarted? see why
kubectl get events --sort-by=.lastTimestamp | tail -20   # cluster-level scheduling/pull errors
```
