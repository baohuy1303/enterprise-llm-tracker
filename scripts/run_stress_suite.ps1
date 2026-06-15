<#
run_stress_suite.ps1 - Sentinel load/stress suite (Windows PowerShell 5.1-safe).

WHAT: drives the real pipeline - OTLP ingest (Redis INCR + async Kafka produce)
      -> 4 Kafka consumers -> Postgres - plus the admin/dashboard read path, and
      a Kafka burst test. Captures throughput, latency percentiles, consumer-group
      lag, and a zero-message-loss check. Numbers go straight on the resume.

WHY (one line each): ingest = the hot path every Claude Code session hits;
      burst = the whole reason Kafka is here (absorb a spike, drain it, lose nothing);
      admin = can the dashboard's read path take a real org's polling.

BENCHMARKS  (single dev laptop, docker-compose, 1 API instance, --batch 10):
                     req/sec      p99 latency     events/sec
      average        ~3,000        < 50 ms         ~30,000
      good           ~8,000        < 20 ms         ~80,000
      excellent     ~15,000+       < 10 ms        ~150,000+
      Kafka burst:   good = backlog drains > 20k msg/sec with ZERO loss.

OUR GOAL: one headline bullet - "sustained 100k+ OTLP events/sec at p99 < 20 ms on
      a single instance, Kafka absorbing a 10x burst with zero message loss." Hit
      that and stop; going higher needs real cloud hardware, not this laptop.

USAGE:
      ./scripts/run_stress_suite.ps1                 # full suite (brings infra up)
      ./scripts/run_stress_suite.ps1 -SkipInfra      # infra + binaries already running
      ./scripts/run_stress_suite.ps1 -Quick          # fast smoke (short, fewer workers)
      ./scripts/run_stress_suite.ps1 -SkipBurst      # skip the Kafka burst scenario
Results (JSON + RESULTS.md) land in stress_results/<timestamp>/.
NOTE: K8s HPA (3.5) and pod-kill chaos (3.7) need minikube - see docs/stress_test.md sec 0.B.
#>
param(
  [switch]$SkipInfra,
  [switch]$SkipBurst,
  [switch]$Quick,
  [string]$Config   = "sentinel.yaml",
  [int]$Engineers   = 200,
  [int]$Batch       = 10,
  [int]$Duration    = 20,                       # seconds per ramp step
  [int[]]$Workers   = @(200, 500, 1000, 2000),
  [string]$OutDir   = ""
)

$ErrorActionPreference = "Stop"
$root = Split-Path $PSScriptRoot -Parent
Set-Location $root
$env:SENTINEL_CONFIG = $Config

if ($Quick) { $Duration = 6; $Workers = @(100, 500); $Engineers = [Math]::Min($Engineers, 50) }

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
function Get-Lag([string]$group){
  $out = docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group $group 2>$null
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

function Grade($r){
  $rps = [double]$r.rps; $p99 = [double]$r.latency_ms.p99
  if ($rps -ge 15000 -and $p99 -lt 10){ return "EXCELLENT" }
  if ($rps -ge 8000  -and $p99 -lt 20){ return "good" }
  if ($rps -ge 3000  -and $p99 -lt 50){ return "average" }
  return "below-average"
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
Get-Content "$root\scripts\seed_load_engineers.sql" -Raw | docker compose exec -T postgres psql -U sentinel -d sentinel -v n=$Engineers -v ON_ERROR_STOP=1 | Out-Null
$seeded = Psql "SELECT count(*) FROM engineers WHERE email LIKE 'loadtest-%@sentinel.local';"
Note "engineers in DB: $seeded"
Code POST "$ApiBase/admin/registry/refresh" | Out-Null
Start-Sleep -Seconds 2

$commonIngest = @("--mode","ingest","--engineers","$Engineers","--batch","$Batch","--target",$IngestURL)

# ---- 3.1 ingest throughput ramp (direct to sentinel-api) --------------------
Stage "3.1 - ingest ramp (direct to sentinel-api, batch=$Batch)"
foreach ($w in $Workers){
  $r = Run-Load ("ingest_direct_${w}w") (@("--workers","$w") + $commonIngest)
  $g = Grade $r
  Write-Host ("   {0,5}w  {1,8:N0} rps  {2,9:N0} ev/s  p95={3} p99={4} ms  err={5}%  [{6}]" -f $w,$r.rps,$r.events_per_sec,$r.latency_ms.p95,$r.latency_ms.p99,$r.error_rate_pct,$g) -ForegroundColor Green
  [void]$summary.Add([pscustomobject]@{ scenario="3.1 ingest direct"; workers=$w; rps=$r.rps; events_per_sec=$r.events_per_sec; p95=$r.latency_ms.p95; p99=$r.latency_ms.p99; err_pct=$r.error_rate_pct; grade=$g })
}

# ---- 3.2 ingest through the OTel Collector (one run at best worker count) ----
Stage "3.2 - ingest through OTel Collector (:4318) - overhead vs direct"
$wBest = ($Workers | Measure-Object -Maximum).Maximum
$r = Run-Load "ingest_collector" (@("--workers","$wBest","--mode","ingest","--engineers","$Engineers","--batch","$Batch","--target",$ColURL))
Write-Host ("   {0,5}w  {1,8:N0} rps  {2,9:N0} ev/s  p99={3} ms  err={4}%" -f $wBest,$r.rps,$r.events_per_sec,$r.latency_ms.p99,$r.error_rate_pct) -ForegroundColor Green
[void]$summary.Add([pscustomobject]@{ scenario="3.2 ingest via collector"; workers=$wBest; rps=$r.rps; events_per_sec=$r.events_per_sec; p95=$r.latency_ms.p95; p99=$r.latency_ms.p99; err_pct=$r.error_rate_pct; grade=(Grade $r) })

# ---- 3.4 admin / dashboard read path ----------------------------------------
Stage "3.4 - admin read path (leaderboard / engineers / signals)"
$r = Run-Load "admin_reads" @("--mode","admin","--workers","200","--token",$AdminToken,"--target",$ApiBase)
Write-Host ("   {0,5}w  {1,8:N0} rps  p95={2} p99={3} ms  err={4}%  status={5}" -f 200,$r.rps,$r.latency_ms.p95,$r.latency_ms.p99,$r.error_rate_pct,($r.status_counts.PSObject.Properties.Name -join ',')) -ForegroundColor Green
[void]$summary.Add([pscustomobject]@{ scenario="3.4 admin reads"; workers=200; rps=$r.rps; events_per_sec=0; p95=$r.latency_ms.p95; p99=$r.latency_ms.p99; err_pct=$r.error_rate_pct; grade="-" })

# ---- 3.3 Kafka burst absorption + drain + zero-loss -------------------------
$burst = $null
if (-not $SkipBurst){
  Stage "3.3 - Kafka burst: absorb spike, drain backlog, zero message loss"
  $rows0 = [int64](Psql "SELECT count(*) FROM usage_events WHERE engineer_id LIKE 'loadtest-%@sentinel.local';")
  Note "usage_events (loadtest) before: $rows0 ; firing burst..."
  $burstDur = if ($Quick){ 8 } else { 25 }
  $bjson = Join-Path $OutDir "kafka_burst.json"
  & $LoadTest --mode ingest --workers $wBest --engineers $Engineers --batch $Batch --duration "$($burstDur)s" --target $IngestURL --out $bjson
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
[void]$md.Add("Config: $Config | engineers: $Engineers | batch: $Batch | duration/step: ${Duration}s | host: single dev laptop (docker-compose)")
[void]$md.Add("")
[void]$md.Add("| scenario | workers | req/sec | events/sec | p95 ms | p99 ms | err % | grade |")
[void]$md.Add("|---|---:|---:|---:|---:|---:|---:|---|")
foreach ($s in $summary){
  [void]$md.Add(("| {0} | {1} | {2:N0} | {3:N0} | {4} | {5} | {6} | {7} |" -f $s.scenario,$s.workers,$s.rps,$s.events_per_sec,$s.p95,$s.p99,$s.err_pct,$s.grade))
}
if ($burst){
  [void]$md.Add("")
  [void]$md.Add("## 3.3 Kafka burst")
  [void]$md.Add(("- produced: **{0:N0}** events" -f $burst.produced))
  [void]$md.Add(("- peak backlog held in Kafka: **{0:N0}** messages" -f $burst.peak_backlog))
  [void]$md.Add(("- drain time after burst: **{0}s** (~{1:N0} msg/sec sustained consumer throughput)" -f $burst.drain_sec,$burst.consumer_msgs_per_sec))
  [void]$md.Add(("- postgres rows applied: {0:N0} - **message loss: {1}**" -f $burst.rows_applied,$burst.message_loss))
}
$best = ($summary | Where-Object { $_.events_per_sec -gt 0 } | Sort-Object events_per_sec -Descending | Select-Object -First 1)
if ($best){
  [void]$md.Add("")
  [void]$md.Add("## Headline")
  [void]$md.Add(("Sustained **{0:N0} OTLP events/sec** ({1:N0} req/sec) at **p99 {2} ms** on a single API instance." -f $best.events_per_sec,$best.rps,$best.p99))
  if ($burst -and $burst.message_loss -le 0){
    [void]$md.Add(("Kafka absorbed a burst of {0:N0} events and drained the backlog with **zero message loss**." -f $burst.produced))
  }
}
$resultsPath = Join-Path $OutDir "RESULTS.md"
$md -join "`n" | Set-Content -Encoding UTF8 $resultsPath

$summary | Format-Table -AutoSize | Out-Host
Write-Host "`nWrote $resultsPath" -ForegroundColor Cyan
Write-Host "Raw per-scenario JSON + binary logs are in $OutDir" -ForegroundColor DarkGray
Write-Host "Tip: stop the app binaries with:  Get-Process sentinel-api,sentinel-workers | Stop-Process -Force" -ForegroundColor DarkGray
