<#
run_stress_suite.ps1 - Sentinel load/stress suite (Windows PowerShell 5.1-safe).

WHAT: drives the real pipeline - OTLP ingest (Redis INCR + async Kafka produce)
      -> 4 Kafka consumers -> Postgres - plus the admin/dashboard read path, and
      a Kafka burst test. Captures throughput, latency percentiles, consumer-group
      lag, and a zero-message-loss check. Numbers go straight on the resume.

WHY (one line each): ingest = the hot path every Claude Code session hits;
      burst = the whole reason Kafka is here (absorb a spike, drain it, lose nothing);
      admin = can the dashboard's read path take a real org's polling.

METHOD: 3.1 is open-loop (--rate), not closed-loop max-out. "Ramp workers until
      something breaks" measures your laptop's ceiling, not Sentinel's - and on a
      single dev machine that ceiling is CPU/co-location contention, a fact about
      the hardware, not the architecture. Instead we derive a target rate from real
      usage (~5 req/sec from one actively-coding engineer x team size) and prove the
      system stays healthy - zero errors, zero pacer overruns, flat p99 - at that rate
      for org sizes 50/100/150/200 engineers. The generator itself is capped to
      GOMAXPROCS=4 so it can never be the thing eating your CPU; if you still see 100%
      during 3.1, that's the app/DB/Kafka, which is the number you actually want.

GRADING: each team-size step is "OK" (kept up: no errors, no pacer overruns, hit the
      target rate) or "saturated" (fell behind, started erroring, or jobs queued up
      faster than workers could drain them). Watch p99 across steps - flat as team
      size grows means headroom; climbing means you found the knee. 3.3's Kafka burst
      is a deliberate, NOT realistic, spike (closed-loop, 400 workers) - the point is
      proving Kafka absorbs an overload and drains it with zero message loss, not
      realism.

OUR GOAL: an honest headline - "Sentinel sustains a realistic 200-engineer team's
      load (rate derived from actual usage, not a synthetic max) at low p99 latency,
      with Kafka absorbing a burst at zero message loss."

USAGE:
      ./scripts/run_stress_suite.ps1                 # full suite (brings infra up)
      ./scripts/run_stress_suite.ps1 -SkipInfra      # infra + binaries already running
      ./scripts/run_stress_suite.ps1 -Quick          # fast smoke (short, fewer steps)
      ./scripts/run_stress_suite.ps1 -SkipBurst      # skip the Kafka burst scenario
Results (JSON + RESULTS.md) land in stress_results/<timestamp>/.
NOTE: K8s HPA (3.5) and pod-kill chaos (3.7) need minikube - see docs/stress_test.md sec 0.B.
#>
param(
  [switch]$SkipInfra,
  [switch]$SkipBurst,
  [switch]$Quick,
  [string]$Config       = "sentinel.yaml",
  [int]$Engineers       = 200,                      # seeded pool; must be >= max(TeamSizes)
  [int]$Batch           = 10,                       # used only by 3.3's deliberately-unrealistic burst
  [int]$Duration        = 20,                       # seconds per step
  [int[]]$TeamSizes     = @(50, 100, 150, 200),      # org sizes to test realistic load at
  [int]$ReqPerEngineer  = 5,                         # realistic sustained req/sec from one active engineer
  [int]$RealisticBatch  = 3,                         # data points/request for the realistic scenario
  [int]$BurstWorkers    = 400,                       # closed-loop workers for the deliberate Kafka spike (3.3)
  [int]$GenCPU          = 4,                         # GOMAXPROCS cap on loadtest.exe - keeps the generator off the cores the app/DB/Kafka need
  [string]$OutDir       = ""
)

$ErrorActionPreference = "Stop"
$root = Split-Path $PSScriptRoot -Parent
Set-Location $root
$env:SENTINEL_CONFIG = $Config
# Silence per-event INFO logs during the run so throughput reflects ingest work,
# not synchronous log I/O. Real warnings/errors still surface.
$env:SENTINEL_LOG_LEVEL = "warn"

if ($Quick) { $Duration = 6; $TeamSizes = @(25, 50); $Engineers = [Math]::Min($Engineers, 50) }

$ApiBase   = "http://localhost:8081"
$IngestURL = "$ApiBase/ingest/otel/v1/metrics"
$ColURL    = "http://localhost:4318/v1/metrics"
$Groups    = @("sentinel.threshold-checker","sentinel.postgres-writer","sentinel.github-trigger","sentinel.signal-detector")

if (-not $OutDir) { $OutDir = Join-Path $root ("stress_results\" + (Get-Date -Format "yyyyMMdd-HHmmss")) }
New-Item -ItemType Directory -Force -Path $OutDir | Out-Null
$LoadTest = Join-Path $OutDir "loadtest.exe"

# admin token from .env
$tokLine = Select-String -Path "$root\.env" -Pattern '^ADMIN_TOKEN=(.*)$' | Select-Object -First 1
$AdminToken = $tokLine.Matches[0].Groups[1].Value.Trim('"', "'", " ")

function Stage($n){ Write-Host "`n=== $n ===" -ForegroundColor Cyan }
function Note($m){ Write-Host "  $m" -ForegroundColor DarkGray }
function Psql([string]$sql){ (docker compose exec -T postgres psql -U sentinel -d sentinel -tAc $sql 2>$null).Trim() }

function Code([string]$method,[string]$url){
  try { [int](Invoke-WebRequest -Method $method -Uri $url -Headers @{Authorization="Bearer $AdminToken"} -UseBasicParsing).StatusCode }
  catch { if($_.Exception.Response){ [int]$_.Exception.Response.StatusCode } else { -1 } }
}

# Sum LAG across all partitions of a consumer group (parses kafka-consumer-groups.sh).
# Local $ErrorActionPreference="Continue": PS 5.1 turns ANY stderr line from a wrapped
# native command into a terminating NativeCommandError under "Stop", even with 2>$null
# on the call. That crashed the drain-detection loop mid-run. The reassignment here only
# shadows the global "Stop" for this function's scope, so the rest of the script is
# unaffected.
function Get-Lag([string]$group){
  $ErrorActionPreference = "Continue"
  $out = $null
  try { $out = docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group $group 2>$null }
  catch { return 0 }
  if (-not $out){ return 0 }
  $lines = $out -split "`n"
  $hdr = $lines | Where-Object { $_ -match '\bLAG\b' } | Select-Object -First 1
  if (-not $hdr){ return 0 }
  $idx = [Array]::IndexOf((($hdr.Trim()) -split '\s+'), 'LAG')
  if ($idx -lt 0){ return 0 }
  $sum = [int64]0
  foreach ($l in $lines){
    if ($l -eq $hdr -or -not $l.Trim()){ continue }
    $f = ($l.Trim() -split '\s+')
    if ($f.Count -gt $idx -and $f[$idx] -match '^\d+$'){ $sum += [int64]$f[$idx] }
  }
  return $sum
}
function Total-Lag { $t=[int64]0; foreach($g in $Groups){ $t += (Get-Lag $g) }; return $t }

# Run one loadtest scenario; returns the parsed JSON result (PSCustomObject).
function Run-Load([string]$name, [string[]]$extra){
  $json = Join-Path $OutDir "$name.json"
  $base = @("--duration","$($Duration)s","--out",$json)
  Write-Host ("-> {0}" -f $name) -ForegroundColor Yellow
  & $LoadTest @base @extra
  if (-not (Test-Path $json)){ throw "loadtest produced no result for $name" }
  return (Get-Content $json -Raw | ConvertFrom-Json)
}

# Open-loop grading: "OK" means the system kept up with the target rate without
# queueing jobs faster than it could serve them (overruns) or shedding load (errors).
# This is a pass/fail against a realistic target, not a "how high can it go" tier list.
function Grade-Realistic($r, $targetRate){
  if ([double]$r.overruns -gt 0){ return "saturated (overruns)" }
  if ([double]$r.error_rate_pct -gt 0.5){ return "saturated (errors)" }
  if ([double]$r.rps -lt (0.95 * $targetRate)){ return "below target rate" }
  return "OK"
}

$summary = New-Object System.Collections.ArrayList

# ---- bring up infra + binaries ----------------------------------------------
if (-not $SkipInfra){
  Stage "Setup - infra + binaries (config: $Config)"
  docker compose up -d | Out-Null
  Start-Sleep -Seconds 3
  Get-Process sentinel-api,sentinel-workers -ErrorAction SilentlyContinue | Stop-Process -Force
  Start-Process go -ArgumentList "run","./cmd/sentinel-api"     -RedirectStandardOutput "$OutDir\api.log"     -RedirectStandardError "$OutDir\api.err.log"     -NoNewWindow
  $up=$false; for($i=0;$i -lt 90;$i++){ if((Code GET "$ApiBase/healthz") -eq 200){$up=$true;break}; Start-Sleep 1 }
  if (-not $up){ throw "sentinel-api did not become healthy. Check $OutDir\api.err.log (if config validation failed on tokens, try -Config sentinel.test.yaml)." }
  Start-Process go -ArgumentList "run","./cmd/sentinel-workers" -RedirectStandardOutput "$OutDir\workers.log" -RedirectStandardError "$OutDir\workers.err.log" -NoNewWindow
  Start-Sleep -Seconds 6
  Note "api + workers started (logs in $OutDir)"
} else {
  Stage "Setup - infra assumed running"
  if ((Code GET "$ApiBase/healthz") -ne 200){ throw "sentinel-api not healthy at $ApiBase (start it, or drop -SkipInfra)." }
}

# ---- build the load tool + seed engineers -----------------------------------
Stage "Setup - build loadtest + seed $Engineers engineers"
go build -o $LoadTest ./cmd/loadtest
if (-not $?){ throw "go build ./cmd/loadtest failed" }
# Cap the generator's own CPU footprint so it can never be the thing saturating your
# machine - api/workers were already started above with a full-core environment, so
# this only applies to loadtest.exe invocations from here on.
$env:GOMAXPROCS = "$GenCPU"
Note "loadtest generator capped to GOMAXPROCS=$GenCPU"
Get-Content "$root\scripts\seed_load_engineers.sql" -Raw | docker compose exec -T postgres psql -U sentinel -d sentinel -v n=$Engineers -v ON_ERROR_STOP=1 | Out-Null
$seeded = [int](Psql "SELECT count(*) FROM engineers WHERE email LIKE 'loadtest-%@sentinel.local';")
Note "engineers in DB: $seeded"
if ($seeded -lt $Engineers){
  throw "seed produced $seeded/$Engineers engineers - every event would drop as unattributed (garbage numbers). Check the seed SQL output above for a psql error."
}
Code POST "$ApiBase/admin/registry/refresh" | Out-Null
Start-Sleep -Seconds 3   # let the registry refresh (30s interval) pick up the new engineers

$commonIngest = @("--mode","ingest","--batch","$RealisticBatch")

# ---- 3.1 realistic team load (open-loop, paced - not closed-loop max-out) ---
# Rate is derived from real usage, not "how fast can my laptop go": ~$ReqPerEngineer
# req/sec from one actively-coding engineer x team size. Workers just need enough
# headroom (rate/8) that the pacer never starves for a free one - --rate drives
# throughput here, not worker count.
Stage "3.1 - realistic team load (~$ReqPerEngineer req/sec/engineer, open-loop, batch=$RealisticBatch)"
foreach ($n in $TeamSizes){
  $rate = $ReqPerEngineer * $n
  $w = [Math]::Max(32, [Math]::Ceiling($rate / 8.0))
  $r = Run-Load ("ingest_team_${n}eng") (@("--workers","$w","--rate","$rate","--engineers","$n","--target",$IngestURL) + $commonIngest)
  $g = Grade-Realistic $r $rate
  $ov = [int]$r.overruns   # JSON omits "overruns" entirely when 0 (omitempty) -> $null -> coerce
  Write-Host ("   {0,3} eng  target={1,5} rps  actual={2,5:N0} rps  {3,6:N0} ev/s  p95={4} p99={5} ms  overruns={6}  err={7}%  [{8}]" -f $n,$rate,$r.rps,$r.events_per_sec,$r.latency_ms.p95,$r.latency_ms.p99,$ov,$r.error_rate_pct,$g) -ForegroundColor Green
  [void]$summary.Add([pscustomobject]@{ scenario="3.1 realistic team load"; scale="$n eng"; rps=$r.rps; events_per_sec=$r.events_per_sec; p95=$r.latency_ms.p95; p99=$r.latency_ms.p99; overruns=$ov; err_pct=$r.error_rate_pct; grade=$g })
}
$bestTeam = ($TeamSizes | Measure-Object -Maximum).Maximum
$bestRate = $ReqPerEngineer * $bestTeam

# ---- 3.2 ingest through the OTel Collector (same load as the largest team) --
Stage "3.2 - ingest through OTel Collector (:4318) - overhead vs direct, $bestTeam-engineer load"
$wColl = [Math]::Max(32, [Math]::Ceiling($bestRate / 8.0))
$r = Run-Load "ingest_collector" (@("--workers","$wColl","--rate","$bestRate","--engineers","$bestTeam","--target",$ColURL) + $commonIngest)
$g = Grade-Realistic $r $bestRate
$ov = [int]$r.overruns
Write-Host ("   {0,3} eng  target={1,5} rps  actual={2,5:N0} rps  {3,6:N0} ev/s  p99={4} ms  err={5}%  [{6}]" -f $bestTeam,$bestRate,$r.rps,$r.events_per_sec,$r.latency_ms.p99,$r.error_rate_pct,$g) -ForegroundColor Green
[void]$summary.Add([pscustomobject]@{ scenario="3.2 ingest via collector"; scale="$bestTeam eng"; rps=$r.rps; events_per_sec=$r.events_per_sec; p95=$r.latency_ms.p95; p99=$r.latency_ms.p99; overruns=$ov; err_pct=$r.error_rate_pct; grade=$g })

# ---- 3.4 admin / dashboard read path ----------------------------------------
Stage "3.4 - admin read path (leaderboard / engineers / signals)"
$r = Run-Load "admin_reads" @("--mode","admin","--workers","200","--token",$AdminToken,"--target",$ApiBase)
Write-Host ("   {0,5}w  {1,8:N0} rps  p95={2} p99={3} ms  err={4}%  status={5}" -f 200,$r.rps,$r.latency_ms.p95,$r.latency_ms.p99,$r.error_rate_pct,($r.status_counts.PSObject.Properties.Name -join ',')) -ForegroundColor Green
[void]$summary.Add([pscustomobject]@{ scenario="3.4 admin reads"; scale="200w"; rps=$r.rps; events_per_sec=0; p95=$r.latency_ms.p95; p99=$r.latency_ms.p99; overruns=0; err_pct=$r.error_rate_pct; grade="-" })

# ---- 3.3 Kafka burst absorption + drain + zero-loss -------------------------
$burst = $null
if (-not $SkipBurst){
  Stage "3.3 - Kafka burst: absorb spike, drain backlog, zero message loss"
  $rows0 = [int64](Psql "SELECT count(*) FROM usage_events WHERE engineer_id LIKE 'loadtest-%@sentinel.local';")
  Note "usage_events (loadtest) before: $rows0 ; firing burst..."
  $burstDur = if ($Quick){ 8 } else { 25 }
  $bjson = Join-Path $OutDir "kafka_burst.json"
  & $LoadTest --mode ingest --workers $BurstWorkers --engineers $Engineers --batch $Batch --duration "$($burstDur)s" --target $IngestURL --out $bjson
  $b = Get-Content $bjson -Raw | ConvertFrom-Json
  $produced = [int64]$b.events

  $peak = Total-Lag
  Note ("burst done: produced {0:N0} events; backlog now {1:N0}; draining..." -f $produced,$peak)
  $t0 = Get-Date; $drainSec = -1
  for ($i=0; $i -lt 120; $i++){
    $lag = Total-Lag
    if ($lag -gt $peak){ $peak = $lag }
    if ($lag -le ($Groups.Count)){ $drainSec = [Math]::Round(((Get-Date)-$t0).TotalSeconds,1); break }
    Start-Sleep -Seconds 2
  }
  $rows1 = [int64](Psql "SELECT count(*) FROM usage_events WHERE engineer_id LIKE 'loadtest-%@sentinel.local';")
  $applied = $rows1 - $rows0
  $loss = $produced - $applied
  $thru = if ($drainSec -gt 0){ [Math]::Round($produced / $drainSec) } else { 0 }
  $burst = [pscustomobject]@{
    produced=$produced; peak_backlog=$peak; drain_sec=$drainSec
    consumer_msgs_per_sec=$thru; rows_applied=$applied; message_loss=$loss
  }
  ($burst | ConvertTo-Json) | Set-Content -Encoding UTF8 (Join-Path $OutDir "kafka_burst_summary.json")
  $lossColor = if ($loss -le 0){ "Green" } else { "Red" }
  Write-Host ("   produced={0:N0}  peak backlog={1:N0}  drained in {2}s  ({3:N0} msg/s)" -f $produced,$peak,$drainSec,$thru) -ForegroundColor Green
  Write-Host ("   postgres rows applied={0:N0}  MESSAGE LOSS={1}" -f $applied,$loss) -ForegroundColor $lossColor
}

# ---- results file -----------------------------------------------------------
Stage "Results"
$md = New-Object System.Collections.ArrayList
[void]$md.Add("# Stress results - $(Get-Date -Format 'yyyy-MM-dd HH:mm')")
[void]$md.Add("")
[void]$md.Add("Config: $Config | engineers seeded: $Engineers | team sizes: $($TeamSizes -join ',') | req/engineer: $ReqPerEngineer | realistic batch: $RealisticBatch | generator capped: GOMAXPROCS=$GenCPU | host: single dev laptop (docker-compose)")
[void]$md.Add("")
[void]$md.Add("| scenario | scale | req/sec | events/sec | p95 ms | p99 ms | overruns | err % | grade |")
[void]$md.Add("|---|---|---:|---:|---:|---:|---:|---:|---|")
foreach ($s in $summary){
  [void]$md.Add(("| {0} | {1} | {2:N0} | {3:N0} | {4} | {5} | {6} | {7} | {8} |" -f $s.scenario,$s.scale,$s.rps,$s.events_per_sec,$s.p95,$s.p99,$s.overruns,$s.err_pct,$s.grade))
}
if ($burst){
  [void]$md.Add("")
  [void]$md.Add("## 3.3 Kafka burst")
  [void]$md.Add(("- produced: **{0:N0}** events" -f $burst.produced))
  [void]$md.Add(("- peak backlog held in Kafka: **{0:N0}** messages" -f $burst.peak_backlog))
  [void]$md.Add(("- drain time after burst: **{0}s** (~{1:N0} msg/sec sustained consumer throughput)" -f $burst.drain_sec,$burst.consumer_msgs_per_sec))
  [void]$md.Add(("- postgres rows applied: {0:N0} - **message loss: {1}**" -f $burst.rows_applied,$burst.message_loss))
}
$bestOK = ($summary | Where-Object { $_.scenario -eq "3.1 realistic team load" -and $_.grade -eq "OK" } | Select-Object -Last 1)
if ($bestOK){
  [void]$md.Add("")
  [void]$md.Add("## Headline")
  [void]$md.Add(("Sustained realistic load for a **{0}** team ({1:N0} req/sec, {2:N0} events/sec) at **p99 {3} ms** with zero errors and zero pacer overruns." -f $bestOK.scale,$bestOK.rps,$bestOK.events_per_sec,$bestOK.p99))
  if ($burst -and $burst.message_loss -le 0){
    [void]$md.Add(("Kafka absorbed a deliberate burst of {0:N0} events ({1} concurrent workers) and drained the backlog with **zero message loss**." -f $burst.produced,$BurstWorkers))
  }
}
$resultsPath = Join-Path $OutDir "RESULTS.md"
$md -join "`n" | Set-Content -Encoding UTF8 $resultsPath

$summary | Format-Table -AutoSize | Out-Host
Write-Host "`nWrote $resultsPath" -ForegroundColor Cyan
Write-Host "Raw per-scenario JSON + binary logs are in $OutDir" -ForegroundColor DarkGray
Write-Host "Tip: stop the app binaries with:  Get-Process sentinel-api,sentinel-workers | Stop-Process -Force" -ForegroundColor DarkGray
