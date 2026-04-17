$ErrorActionPreference = 'Stop'

$projectRoot = Split-Path $PSScriptRoot -Parent
$commonScript = Join-Path $PSScriptRoot 'monitor_service_common.ps1'
$stopScript = Join-Path $PSScriptRoot 'stop_monitor_service.ps1'
$startScript = Join-Path $PSScriptRoot 'start_monitor_console.ps1'

if (-not (Test-Path $commonScript)) {
    throw "Missing monitor service helper script: $commonScript"
}
. $commonScript

if (-not (Ensure-MonitorAdmin -ScriptPath $PSCommandPath)) {
    return
}

if (-not (Test-Path $stopScript)) {
    throw "Missing stop script: $stopScript"
}

if (-not (Test-Path $startScript)) {
    throw "Missing start script: $startScript"
}

Write-Host '========================================' -ForegroundColor Cyan
Write-Host '  Restarting monitor service...' -ForegroundColor Cyan
Write-Host '========================================' -ForegroundColor Cyan
Write-Host ''

Write-Host '[monitor] Step 1/2: stopping current service...' -ForegroundColor Yellow
& $stopScript

Start-Sleep -Seconds 1

Write-Host ''
Write-Host '[monitor] Step 2/2: starting service again...' -ForegroundColor Yellow
& $startScript

Write-Host ''
Write-Host '[monitor] Restart completed.' -ForegroundColor Green
