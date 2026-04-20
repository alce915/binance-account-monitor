$ErrorActionPreference = 'Stop'

$projectRoot = Split-Path $PSScriptRoot -Parent
$commonScript = Join-Path $PSScriptRoot 'monitor_service_common.ps1'
$envFile = Join-Path $projectRoot '.env'

if (-not (Test-Path $commonScript)) {
    throw "Missing monitor service helper script: $commonScript"
}
. $commonScript

if (-not (Ensure-MonitorAdmin -ScriptPath $PSCommandPath -KeepWindowOpen)) {
    return
}

$envConfig = Read-EnvConfig -Path $envFile
$hostAddress = Get-MonitorHostAddress -EnvConfig $envConfig
$healthHostAddress = Get-MonitorHealthHostAddress -HostAddress $hostAddress
$port = Get-MonitorPort -EnvConfig $envConfig

$stopResult = Stop-MonitorServiceInstance -ProjectRoot $projectRoot -HostAddress $hostAddress -Port $port -TimeoutSeconds 10
if (-not $stopResult.state_found -and $stopResult.attempted_pids.Count -eq 0) {
    Write-Output "[monitor] No tracked or listening process found on port $port."
    return
}

Write-Output "[monitor] Attempted process count: $($stopResult.attempted_pids.Count)"
Write-Output "[monitor] Stopped process count: $($stopResult.stopped_pids.Count)"
if ($stopResult.success) {
    Write-Output "[monitor] Service is no longer reachable at http://$healthHostAddress`:$port/"
    return
}

$pending = if ($stopResult.pending_listener_pids.Count -gt 0) {
    $stopResult.pending_listener_pids -join ', '
} else {
    'unknown'
}
throw "[monitor] Service still appears reachable on port $port. Pending listener PID(s): $pending"
