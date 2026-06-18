<#
run_stress_suite_v2.ps1 - Sentinel load/stress suite v2 (Windows PowerShell 5.1-safe).

WHAT: drives the real pipeline - OTLP ingest (Redis INCR + async Kafka produce)
      -> 4 Kafka consumers -> Postgres - plus the admin/dashboard read path, and
      a Kafka burst test. Captures throughput, latency percentiles, consumer-group
      lag, and a zero-message-loss check. Numbers go straight on the resume.

WHY (one line each): ingest = the hot path every Claude Code session hits;
      burst = the whole reason Kafka is here (absorb a spike, drain it, lose nothing);
      admin = can the dashboard's read path take a real org's polling.

METHOD: both 3.1 and 3.3 are open-loop (--rate), not closed-loop max-out. "Ramp
      workers until something breaks" measures your laptop's ceiling, not Sentinel's.
      Instead we derive target rates from real usage (~5 req/sec from one actively-
      coding engineer) and prove the system stays healthy - zero errors, zero pacer
      overruns, flat p99 - at realistic org sizes (default 200/500/1000, matching the
      "500-1000+ engineers" customer profile in docs/sentinel-plan-final.md). The
      generator is capped to GOMAXPROCS=4 so it can never be the thing eating your
      CPU; if you still see 100% during 3.1, that's the app/DB/Kafka, the number you
      actually want. 3.3's burst is a deliberate spike (more req/sec than steady
      state, briefly) but still rate-shaped, not a closed-loop firehose.

WHAT'S NEW IN V2 (fixes found running v1):
      - 3.3 burst is now open-loop (-BurstRate/-BurstDurationSec) instead of closed-
        loop --workers - models a believable spike ("N engineers momentarily firing
        harder"), not an arbitrary "throw 400 workers at it."
      - Lag checking now makes ONE "--all-groups" call per poll instead of 4
        sequential per-group calls. Each kafka-consumer-groups.sh call costs ~1.8s of
        JVM startup alone - the old per-group loop made every drain-check iteration
        take ~9s instead of ~2s, so a "120-iteration, ~4-minute" budget actually took
        ~18-20 real minutes regardless of how fast the real drain was. v1 also nearly
        always timed out (drain_sec=-1) because of this, not because draining was
        actually slow.
      - The pre-burst Postgres row baseline now waits for 3.1/3.2's own events to
        fully drain first. v1 sampled the baseline immediately, so leftover in-flight
        events from earlier scenarios landed during the burst's measurement window
        and produced a nonsensical NEGATIVE "message loss" number.
      - Peak backlog is now reported as both a total (sum of catch-up work across all
        4 independent consumer groups) AND a per-group max (the actual single-copy
        queue depth in the topic) - v1's "peak_backlog" was the sum only, which read
        like "2 million messages stuck" when the real number was ~1/4 of that.
      - The "zero message loss" headline now requires the drain to have actually
        finished (drain_sec > 0), not just message_loss <= 0 - v1's headline claimed
        zero loss even on a timed-out run because a negative number happens to pass
        a "<= 0" check.
      - -RealisticBatch 5 (cost + 4 token kinds - the realistic ceiling of distinct
        metric kinds Claude Code emits per flush), -BurstRate 2000 / -BurstDurationSec
        10 (200 "engineers" momentarily firing 10 req/sec for 10s).
      - Same settle-before-sampling fix as the burst, now applied to EVERY scenario
        (each 3.1 step, 3.2, 3.4), not just 3.3 - a step that falls behind leaves
        Kafka lag that pollutes the NEXT step's numbers, because Kafka burns CPU
        catching up and starves Redis/the app of scheduling on a single laptop.
        Confirmed empirically: running an actual 1000-eng/5000rps step pegged the
        Kafka container at 94-106% CPU even at rest afterward (catching up a
        backlog), which is what made p99 cliff from ~37ms (500 eng) to ~2,100ms
        (1000 eng) - not application code, not Redis itself (its own slowlog showed
        a trivial Lua script taking 14ms+ purely from not being scheduled in time).
      - -TeamSizes now bisects the known range (500 eng was clean at p99=37ms in a
        v2 run before this fix; 1000 eng broke down) to find where the real wall is
        on this hardware: default 300,450,600,750,900.

GRADING: each step is "OK" (kept up: no errors, no pacer overruns, hit the target
      rate) or "saturated" (fell behind, started erroring, or jobs queued up faster
      than workers could drain them). Watch p99 across steps - flat as load grows
      means headroom; climbing means you found the knee.

USAGE:
      ./scripts/run_stress_suite_v2.ps1                 # full suite (brings infra up)
      ./scripts/run_stress_suite_v2.ps1 -SkipInfra      # infra + binaries already running
      ./scripts/run_stress_suite_v2.ps1 -Quick          # fast smoke (short, fewer steps)
      ./scripts/run_stress_suite_v2.ps1 -SkipBurst      # skip the Kafka burst scenario
Results (JSON + RESULTS.md) land in stress_results/<timestamp>/.
NOTE: K8s HPA (3.5) and pod-kill chaos (3.7) need minikube - see docs/stress_test.md sec 0.B.
#>
param(
  [switch]$SkipInfra,
  [switch]$SkipBurst,
  [switch]$Quick,
  [string]$Config         = "sentinel.yaml",
  [int]$Engineers         = 1000,                     # seeded pool; must be >= max(TeamSizes)
  [int]$Duration          = 15,                        # seconds per 3.1 step (5 steps now, plus settle waits - kept shorter to bound total runtime)
  [int[]]$TeamSizes       = @(300, 450, 600, 750, 900),  # bisecting the known range: 500=clean, 1000=Kafka-CPU cliff
  [int]$ReqPerEngineer    = 5,                          # realistic sustained req/sec from one active engineer
  [int]$RealisticBatch    = 5,                          # data points/request (cost + 4 token kinds)
  [int]$BurstRate         = 2000,                       # open-loop target req/sec for the deliberate spike (3.3)
  [int]$BurstDurationSec  = 10,                         # how long the spike lasts
  [int]$MaxDrainMinutes   = 15,                         # how long to wait for the burst backlog to drain before giving up
  [int]$GenCPU            = 4,                          # GOMAXPROCS cap on loadtest.exe - keeps the generator off the cores the app/DB/Kafka need
  [string]$OutDir         = ""
)

$ErrorActionPreference = "Stop"
$root = Split-Path $PSScriptRoot -Parent
Set-Location $root
$env:SENTINEL_CONFIG = $Config
# Silence per-event INFO logs during the run so throughput reflects ingest work,
# not synchronous log I/O. Real warnings/errors still surface.
$env:SENTINEL_LOG_LEVEL = "warn"

if ($Quick) { $Duration = 6; $TeamSizes = @(50, 100); $Engineers = [Math]::Min($Engineers, 100); $BurstDurationSec = 5; $MaxDrainMinutes = 5 }

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

# Lag for ALL consumer groups in ONE call (replaces v1's 4 sequential per-group
# calls). Each kafka-consumer-groups.sh invocation costs ~1.8s of JVM startup alone;
# calling it once per poll instead of 4x cuts every drain-check iteration from ~9s to
# ~2s, so a given iteration budget reflects real drain time instead of CLI overhead.
# Local $ErrorActionPreference="Continue": PS 5.1 turns ANY stderr line from a wrapped
# native command into a terminating NativeCommandError under "Stop", even with 2>$null
# on the call - this shadows that only for this function's scope.
function Get-AllLag {
  $ErrorActionPreference = "Continue"
  $result = @{}
  foreach ($g in $Groups){ $result[$g] = [int64]0 }
  $out = $null
  try { $out = docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --all-groups --describe 2>$null }
  catch { return $result }
  if (-not $out){ return $result }
  $lines = $out -split "`n"
  $hdr = $lines | Where-Object { $_ -match '\bLAG\b' } | Select-Object -First 1
  if (-not $hdr){ return $result }
  $cols = ($hdr.Trim()) -split '\s+'
  $lagIdx = [Array]::IndexOf($cols, 'LAG')
  $grpIdx = [Array]::IndexOf($cols, 'GROUP')
  if ($lagIdx -lt 0 -or $grpIdx -lt 0){ return $result }
  foreach ($l in $lines){
    if ($l -eq $hdr -or -not $l.Trim()){ continue }
    $f = ($l.Trim() -split '\s+')
    if ($f.Count -le [Math]::Max($lagIdx,$grpIdx)){ continue }
    $grp = $f[$grpIdx]
    if ($result.ContainsKey($grp) -and $f[$lagIdx] -match '^\d+$'){ $result[$grp] += [int64]$f[$lagIdx] }
  }
  return $result
}
function Total-Lag { $m = Get-AllLag; $t=[int64]0; foreach($g in $Groups){ $t += $m[$g] }; return $t }

# Wait for consumer lag to settle before a scenario starts. A scenario that falls
# behind leaves Kafka lag that pollutes the NEXT scenario's measurement - Kafka burns
# CPU catching up, which starves Redis/the app of scheduling on this single-laptop
# setup, making the next scenario look worse than it really is on its own.
function Wait-Settle([string]$label){
  $t0 = Get-Date
  for ($i=0; $i -lt 60; $i++){ if ((Total-Lag) -le $Groups.Count){ break }; Start-Sleep -Seconds 2 }
  $sec = [Math]::Round(((Get-Date)-$t0).TotalSeconds,1)
  if ($sec -gt 1){ Note "settled for ${sec}s before $label (draining prior step's backlog)" }
}

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

# ---- clear any leftover Kafka backlog from a prior run -----------------------
Stage "Setup - clearing any leftover consumer-group backlog"
$preLag = Total-Lag
if ($preLag -gt $Groups.Count){
  Note "found $preLag lag left over from a previous run - resetting all 4 groups to latest offset"
  foreach ($g in $Groups){
    docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group $g --topic claude.usage.events --reset-offsets --to-latest --execute 2>$null | Out-Null
  }
  Note ("post-reset lag: {0}" -f (Total-Lag))
} else {
  Note "no leftover backlog (lag=$preLag) - clean start"
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
  Wait-Settle "$n-eng step"
  $rate = $ReqPerEngineer * $n
  $w = [Math]::Max(32, [Math]::Ceiling($rate / 8.0))
  $r = Run-Load ("ingest_team_${n}eng") (@("--workers","$w","--rate","$rate","--engineers","$n","--target",$IngestURL) + $commonIngest)
  $g = Grade-Realistic $r $rate
  $ov = [int]$r.overruns   # JSON omits "overruns" entirely when 0 (omitempty) -> $null -> coerce
  $lagAfter = Total-Lag
  Write-Host ("   {0,4} eng  target={1,6} rps  actual={2,6:N0} rps  {3,7:N0} ev/s  p95={4} p99={5} ms  overruns={6}  err={7}%  lag-after={8}  [{9}]" -f $n,$rate,$r.rps,$r.events_per_sec,$r.latency_ms.p95,$r.latency_ms.p99,$ov,$r.error_rate_pct,$lagAfter,$g) -ForegroundColor Green
  [void]$summary.Add([pscustomobject]@{ scenario="3.1 realistic team load"; scale="$n eng"; rps=$r.rps; events_per_sec=$r.events_per_sec; p95=$r.latency_ms.p95; p99=$r.latency_ms.p99; overruns=$ov; err_pct=$r.error_rate_pct; grade=$g })
}
$bestTeam = ($TeamSizes | Measure-Object -Maximum).Maximum
$bestRate = $ReqPerEngineer * $bestTeam

# ---- 3.2 ingest through the OTel Collector (same load as the largest team) --
Stage "3.2 - ingest through OTel Collector (:4318) - overhead vs direct, $bestTeam-engineer load"
Wait-Settle "3.2 collector"
$wColl = [Math]::Max(32, [Math]::Ceiling($bestRate / 8.0))
$r = Run-Load "ingest_collector" (@("--workers","$wColl","--rate","$bestRate","--engineers","$bestTeam","--target",$ColURL) + $commonIngest)
$g = Grade-Realistic $r $bestRate
$ov = [int]$r.overruns
Write-Host ("   {0,4} eng  target={1,6} rps  actual={2,6:N0} rps  {3,7:N0} ev/s  p99={4} ms  err={5}%  [{6}]" -f $bestTeam,$bestRate,$r.rps,$r.events_per_sec,$r.latency_ms.p99,$r.error_rate_pct,$g) -ForegroundColor Green
[void]$summary.Add([pscustomobject]@{ scenario="3.2 ingest via collector"; scale="$bestTeam eng"; rps=$r.rps; events_per_sec=$r.events_per_sec; p95=$r.latency_ms.p95; p99=$r.latency_ms.p99; overruns=$ov; err_pct=$r.error_rate_pct; grade=$g })

# ---- 3.4 admin / dashboard read path ----------------------------------------
Stage "3.4 - admin read path (leaderboard / engineers / signals)"
Wait-Settle "3.4 admin reads"
$r = Run-Load "admin_reads" @("--mode","admin","--workers","200","--token",$AdminToken,"--target",$ApiBase)
Write-Host ("   {0,5}w  {1,8:N0} rps  p95={2} p99={3} ms  err={4}%  status={5}" -f 200,$r.rps,$r.latency_ms.p95,$r.latency_ms.p99,$r.error_rate_pct,($r.status_counts.PSObject.Properties.Name -join ',')) -ForegroundColor Green
[void]$summary.Add([pscustomobject]@{ scenario="3.4 admin reads"; scale="200w"; rps=$r.rps; events_per_sec=0; p95=$r.latency_ms.p95; p99=$r.latency_ms.p99; overruns=0; err_pct=$r.error_rate_pct; grade="-" })

# ---- 3.3 Kafka burst absorption + drain + zero-loss -------------------------
$burst = $null
if (-not $SkipBurst){
  Stage "3.3 - Kafka burst: absorb a deliberate spike, drain backlog, verify zero message loss"
  Wait-Settle "3.3 burst baseline"
  $rows0 = [int64](Psql "SELECT count(*) FROM usage_events WHERE engineer_id LIKE 'loadtest-%@sentinel.local';")
  Note ("usage_events (loadtest) before: {0} ; firing burst (target {1} rps for {2}s)..." -f $rows0,$BurstRate,$BurstDurationSec)
  $bjson = Join-Path $OutDir "kafka_burst.json"
  $wBurst = [Math]::Max(32, [Math]::Ceiling($BurstRate / 8.0))
  & $LoadTest --mode ingest --rate $BurstRate --workers $wBurst --engineers $Engineers --batch $RealisticBatch --duration "$($BurstDurationSec)s" --target $IngestURL --out $bjson
  $b = Get-Content $bjson -Raw | ConvertFrom-Json
  $produced = [int64]$b.events

  $snap = Get-AllLag
  $peakTotal = [int64](($snap.Values | Measure-Object -Sum).Sum)
  $peakPerGroup = [int64](($snap.Values | Measure-Object -Maximum).Maximum)
  Note ("burst done: produced {0:N0} events; backlog now {1:N0} per group (max), {2:N0} total across 4 groups; draining..." -f $produced,$peakPerGroup,$peakTotal)
  $t0 = Get-Date; $drainSec = -1
  $maxIters = [Math]::Ceiling(($MaxDrainMinutes * 60) / 2.0)
  for ($i=0; $i -lt $maxIters; $i++){
    $snap = Get-AllLag
    $total = [int64](($snap.Values | Measure-Object -Sum).Sum)
    $maxGrp = [int64](($snap.Values | Measure-Object -Maximum).Maximum)
    if ($total -gt $peakTotal){ $peakTotal = $total }
    if ($maxGrp -gt $peakPerGroup){ $peakPerGroup = $maxGrp }
    if ($total -le $Groups.Count){ $drainSec = [Math]::Round(((Get-Date)-$t0).TotalSeconds,1); break }
    Start-Sleep -Seconds 2
  }
  $rows1 = [int64](Psql "SELECT count(*) FROM usage_events WHERE engineer_id LIKE 'loadtest-%@sentinel.local';")
  $applied = $rows1 - $rows0
  $loss = $produced - $applied
  $thru = if ($drainSec -gt 0){ [Math]::Round($produced / $drainSec) } else { 0 }
  $burst = [pscustomobject]@{
    produced=$produced; peak_backlog_per_group=$peakPerGroup; peak_backlog_total=$peakTotal; drain_sec=$drainSec
    consumer_msgs_per_sec=$thru; rows_applied=$applied; message_loss=$loss
  }
  ($burst | ConvertTo-Json) | Set-Content -Encoding UTF8 (Join-Path $OutDir "kafka_burst_summary.json")
  $drainedOK = ($drainSec -gt 0)
  $lossColor = if ($drainedOK -and $loss -le 0){ "Green" } else { "Red" }
  $drainLabel = if ($drainedOK){ "${drainSec}s" } else { "DID NOT FINISH within $MaxDrainMinutes min" }
  Write-Host ("   produced={0:N0}  peak backlog={1:N0}/group ({2:N0} total)  drained in {3}  ({4:N0} msg/s)" -f $produced,$peakPerGroup,$peakTotal,$drainLabel,$thru) -ForegroundColor Green
  Write-Host ("   postgres rows applied={0:N0}  MESSAGE LOSS={1}" -f $applied,$loss) -ForegroundColor $lossColor
}

# ---- results file -----------------------------------------------------------
Stage "Results"
$md = New-Object System.Collections.ArrayList
[void]$md.Add("# Stress results - $(Get-Date -Format 'yyyy-MM-dd HH:mm')")
[void]$md.Add("")
[void]$md.Add("Config: $Config | engineers seeded: $Engineers | team sizes: $($TeamSizes -join ',') | req/engineer: $ReqPerEngineer | realistic batch: $RealisticBatch | burst: $BurstRate rps x ${BurstDurationSec}s | generator capped: GOMAXPROCS=$GenCPU | host: single dev laptop (docker-compose)")
[void]$md.Add("")
[void]$md.Add("| scenario | scale | req/sec | events/sec | p95 ms | p99 ms | overruns | err % | grade |")
[void]$md.Add("|---|---|---:|---:|---:|---:|---:|---:|---|")
foreach ($s in $summary){
  [void]$md.Add(("| {0} | {1} | {2:N0} | {3:N0} | {4} | {5} | {6} | {7} | {8} |" -f $s.scenario,$s.scale,$s.rps,$s.events_per_sec,$s.p95,$s.p99,$s.overruns,$s.err_pct,$s.grade))
}
if ($burst){
  [void]$md.Add("")
  [void]$md.Add("## 3.3 Kafka burst")
  [void]$md.Add(("- produced: **{0:N0}** events ({1} rps target x {2}s)" -f $burst.produced,$BurstRate,$BurstDurationSec))
  [void]$md.Add(("- peak backlog: **{0:N0}** messages in the topic (per-group depth) - {1:N0} total catch-up work summed across the 4 independent consumer groups" -f $burst.peak_backlog_per_group,$burst.peak_backlog_total))
  if ($burst.drain_sec -gt 0){
    [void]$md.Add(("- drain time after burst: **{0}s** (~{1:N0} msg/sec sustained consumer throughput)" -f $burst.drain_sec,$burst.consumer_msgs_per_sec))
  } else {
    [void]$md.Add(("- drain DID NOT finish within the {0}-minute budget - re-run with a higher -MaxDrainMinutes, or this is a real finding (the slowest consumer can't keep up)" -f $MaxDrainMinutes))
  }
  [void]$md.Add(("- postgres rows applied: {0:N0} - **message loss: {1}**" -f $burst.rows_applied,$burst.message_loss))
}
$bestOK = ($summary | Where-Object { $_.scenario -eq "3.1 realistic team load" -and $_.grade -eq "OK" } | Select-Object -Last 1)
if ($bestOK){
  [void]$md.Add("")
  [void]$md.Add("## Headline")
  [void]$md.Add(("Sustained realistic load for a **{0}** team ({1:N0} req/sec, {2:N0} events/sec) at **p99 {3} ms** with zero errors and zero pacer overruns." -f $bestOK.scale,$bestOK.rps,$bestOK.events_per_sec,$bestOK.p99))
  if ($burst -and $burst.drain_sec -gt 0 -and $burst.message_loss -le 0){
    [void]$md.Add(("Kafka absorbed a deliberate burst of {0:N0} events ({1} rps for {2}s) and drained the backlog in {3}s with **zero message loss**." -f $burst.produced,$BurstRate,$BurstDurationSec,$burst.drain_sec))
  }
}
$resultsPath = Join-Path $OutDir "RESULTS.md"
$md -join "`n" | Set-Content -Encoding UTF8 $resultsPath

$summary | Format-Table -AutoSize | Out-Host
Write-Host "`nWrote $resultsPath" -ForegroundColor Cyan
Write-Host "Raw per-scenario JSON + binary logs are in $OutDir" -ForegroundColor DarkGray
Write-Host "Tip: stop the app binaries with:  Get-Process sentinel-api,sentinel-workers | Stop-Process -Force" -ForegroundColor DarkGray
