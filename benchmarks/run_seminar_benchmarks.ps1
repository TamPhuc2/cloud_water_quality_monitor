param(
    [ValidateSet("all", "burst", "single", "batch", "crash", "charts")]
    [string]$Phase = "all",
    [string]$Bootstrap = "127.0.0.1:19092",
    [string]$Topic = "water-quality-events",
    [string]$ConsumerGroup = "water-quality-consumer",
    [string]$PythonExe = ""
)

$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $PSScriptRoot
$ResultsDir = Join-Path $PSScriptRoot "results"

if (-not $PythonExe) {
    $VenvPython = Join-Path $ProjectRoot ".venv\Scripts\python.exe"
    if (Test-Path $VenvPython) {
        $PythonExe = $VenvPython
    } else {
        $PythonExe = "python"
    }
}

function Invoke-BenchmarkRun {
    param(
        [string]$PhaseTag,
        [double]$Rate,
        [int]$MaxEvents,
        [int]$WaitTimeout = 90
    )
    $Stamp = (Get-Date).ToUniversalTime().ToString("yyyyMMdd-HHmmss")
    $RunId = "$PhaseTag-$Stamp"
    $ProdReport = Join-Path $ResultsDir "$RunId-producer.json"
    $EvalOutput = Join-Path $ResultsDir "$RunId.json"

    Write-Host "`n=== Running [$PhaseTag] run_id=$RunId rate=$Rate max=$MaxEvents ===" -ForegroundColor Cyan
    & $PythonExe (Join-Path $ProjectRoot "producer\replay_producer.py") `
        --bootstrap $Bootstrap `
        --topic $Topic `
        --run-id $RunId `
        --rate $Rate `
        --max-events $MaxEvents `
        --report-json $ProdReport

    & $PythonExe (Join-Path $PSScriptRoot "evaluate_run.py") `
        --run-id $RunId `
        --phase $PhaseTag `
        --producer-report $ProdReport `
        --wait-timeout $WaitTimeout `
        --bootstrap $Bootstrap `
        --topic $Topic `
        --group $ConsumerGroup `
        --output-json $EvalOutput
}

if ($Phase -in @("all", "burst")) {
    Write-Host "`n[Step 1/4] Burst benchmark (ensure EC2 consumer is in CONSUMER_MODE=batch)." -ForegroundColor Green
    Invoke-BenchmarkRun -PhaseTag "burst-10" -Rate 10 -MaxEvents 300
    Invoke-BenchmarkRun -PhaseTag "burst-50" -Rate 50 -MaxEvents 300
    Invoke-BenchmarkRun -PhaseTag "burst-100" -Rate 100 -MaxEvents 300
}

if ($Phase -in @("all", "single")) {
    Write-Host "`n[Step 2/4] Sustained 1000 @ 100/s SINGLE-COMMIT baseline." -ForegroundColor Yellow
    Write-Host "On EC2, set CONSUMER_MODE=single in .env and run: docker compose up -d consumer" -ForegroundColor Yellow
    Read-Host "Press ENTER once EC2 consumer is running in SINGLE mode"
    Invoke-BenchmarkRun -PhaseTag "sustained-single" -Rate 100 -MaxEvents 1000 -WaitTimeout 300
}

if ($Phase -in @("all", "batch")) {
    Write-Host "`n[Step 3/4] Sustained 1000 @ 100/s BATCH-COMMIT (batch=100)." -ForegroundColor Yellow
    Write-Host "On EC2, set CONSUMER_MODE=batch in .env and run: docker compose up -d consumer" -ForegroundColor Yellow
    Read-Host "Press ENTER once EC2 consumer is running in BATCH mode"
    Invoke-BenchmarkRun -PhaseTag "sustained-batch" -Rate 100 -MaxEvents 1000 -WaitTimeout 120
}

if ($Phase -in @("all", "crash")) {
    Write-Host "`n[Step 4/4] Crash-Recovery Test (exit code 77 after DB commit, before Kafka offset commit)." -ForegroundColor Yellow
    Write-Host "On EC2, stop normal consumer and start a one-shot crash-armed container:" -ForegroundColor Yellow
    Write-Host "  docker compose stop consumer" -ForegroundColor Cyan
    Write-Host "  docker compose run --rm -e CONSUMER_MODE=single -e CONSUMER_CRASH_AFTER_DB_COMMIT=true -e CONSUMER_CRASH_TARGET_INDEX=0 consumer" -ForegroundColor Cyan
    Read-Host "Press ENTER once the one-shot crash-armed container is running and subscribed"

    $Stamp = (Get-Date).ToUniversalTime().ToString("yyyyMMdd-HHmmss")
    $CrashRunId = "crash-recovery-$Stamp"
    $ProdReport = Join-Path $ResultsDir "$CrashRunId-producer.json"
    $EvalOutput = Join-Path $ResultsDir "$CrashRunId.json"

    & $PythonExe (Join-Path $ProjectRoot "producer\replay_producer.py") `
        --bootstrap $Bootstrap `
        --topic $Topic `
        --run-id $CrashRunId `
        --rate 20 `
        --max-events 100 `
        --report-json $ProdReport

    Write-Host "`nVerify the one-shot container exited with code 77 (INJECTED_CRASH_AFTER_DB_COMMIT)." -ForegroundColor Yellow
    Write-Host "Now start normal BATCH consumer on EC2 to replay uncommitted offsets:" -ForegroundColor Yellow
    Write-Host "  docker compose up -d consumer" -ForegroundColor Cyan
    Read-Host "Press ENTER once normal EC2 consumer has started and logged DEDUP_REPLAY"

    & $PythonExe (Join-Path $PSScriptRoot "evaluate_run.py") `
        --run-id $CrashRunId `
        --phase "crash-recovery" `
        --producer-report $ProdReport `
        --bootstrap $Bootstrap `
        --topic $Topic `
        --group $ConsumerGroup `
        --output-json $EvalOutput
}

if ($Phase -in @("all", "charts")) {
    Write-Host "`nGenerating Slide 9 & Slide 10 Cloud charts..." -ForegroundColor Cyan
    & $PythonExe (Join-Path $PSScriptRoot "generate_charts.py")
}
