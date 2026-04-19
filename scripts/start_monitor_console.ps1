$ErrorActionPreference = 'Stop'

$projectRoot = Split-Path $PSScriptRoot -Parent
$commonScript = Join-Path $PSScriptRoot 'monitor_service_common.ps1'
$localPyVenvConfig = Join-Path $projectRoot '.venv\pyvenv.cfg'
$localVenvPython = Join-Path $projectRoot '.venv\Scripts\python.exe'
$parentVenvPython = Join-Path (Split-Path $projectRoot -Parent) '.venv\Scripts\python.exe'
$envFile = Join-Path $projectRoot '.env'
$envExampleFile = Join-Path $projectRoot '.env.example'
$accountsFile = Join-Path $projectRoot 'config\binance_monitor_accounts.json'
$accountsExampleFile = Join-Path $projectRoot 'config\binance_monitor_accounts.example.json'

if (-not (Test-Path $commonScript)) {
    throw "Missing monitor service helper script: $commonScript"
}
. $commonScript

if (-not (Ensure-MonitorAdmin -ScriptPath $PSCommandPath)) {
    return
}

function Normalize-MonitorProcessEnvironment {
    $processVariables = [System.Environment]::GetEnvironmentVariables('Process')
    $pathValue = $null

    foreach ($candidate in @('Path', 'PATH')) {
        if ($processVariables.ContainsKey($candidate) -and $processVariables[$candidate]) {
            $pathValue = [string]$processVariables[$candidate]
            break
        }
    }

    if ($pathValue) {
        [System.Environment]::SetEnvironmentVariable('Path', $pathValue, 'Process')
    }

    [System.Environment]::SetEnvironmentVariable('PATH', $null, 'Process')
}

function Export-MonitorEnvConfigToProcess {
    param([hashtable]$EnvConfig)

    foreach ($entry in $EnvConfig.GetEnumerator()) {
        $name = [string]$entry.Key
        $value = [string]$entry.Value
        if (-not $name) {
            continue
        }
        [System.Environment]::SetEnvironmentVariable($name, $value, 'Process')
    }
}

function Test-PlaceholderCredentials {
    param([string]$Path)

    if (-not (Test-Path $Path)) {
        return $false
    }

    $content = Get-Content -Raw -Path $Path -ErrorAction SilentlyContinue
    if (-not $content) {
        return $false
    }

    return $content -match 'replace-with-api-(key|secret)-' -or $content -match '"api_(key|secret)"\s*:\s*""'
}

function Resolve-MonitorPython {
    param(
        [string]$ProjectRoot,
        [hashtable]$EnvConfig
    )

    $pythonExecutable = $null
    if (Test-Path $localPyVenvConfig) {
        $homeLine = Get-Content $localPyVenvConfig | Where-Object { $_ -like 'home = *' } | Select-Object -First 1
        if ($homeLine) {
            $homePath = $homeLine.Substring(7).Trim()
            if ($homePath) {
                $candidate = Join-Path $homePath 'python.exe'
                if (Test-Path $candidate) {
                    $pythonExecutable = $candidate
                }
            }
        }
    }

    if (-not $pythonExecutable) {
        if (Test-Path $localVenvPython) {
            $pythonExecutable = $localVenvPython
        } elseif (Test-Path $parentVenvPython) {
            $pythonExecutable = $parentVenvPython
        }
    }

    if ($EnvConfig.ContainsKey('MONITOR_PYTHON') -and $EnvConfig['MONITOR_PYTHON']) {
        return @{
            exe = $EnvConfig['MONITOR_PYTHON']
            args = @()
        }
    }

    if ($pythonExecutable -and (Test-Path $pythonExecutable)) {
        return @{
            exe = $pythonExecutable
            args = @()
        }
    }

    $pyCommand = Get-Command py -ErrorAction SilentlyContinue
    if ($pyCommand) {
        return @{
            exe = 'py'
            args = @('-3')
        }
    }

    $pythonCommand = Get-Command python -ErrorAction SilentlyContinue
    if ($pythonCommand) {
        return @{
            exe = 'python'
            args = @()
        }
    }

    throw 'No runnable Python launcher was found. Configure MONITOR_PYTHON in .env first.'
}

if (-not (Test-Path $envFile) -and (Test-Path $envExampleFile)) {
    Copy-Item -Path $envExampleFile -Destination $envFile
}

if (-not (Test-Path $accountsFile) -and (Test-Path $accountsExampleFile)) {
    Copy-Item -Path $accountsExampleFile -Destination $accountsFile
}

if (Test-PlaceholderCredentials -Path $accountsFile) {
    throw "Placeholder Binance API credentials detected in $accountsFile"
}

$envConfig = Read-EnvConfig -Path $envFile
$pythonInfo = Resolve-MonitorPython -ProjectRoot $projectRoot -EnvConfig $envConfig

$hostAddress = Get-MonitorHostAddress -EnvConfig $envConfig
$port = Get-MonitorPort -EnvConfig $envConfig
$runtimeDir = Get-MonitorRuntimeDir -ProjectRoot $projectRoot
$statePath = Get-MonitorStatePath -ProjectRoot $projectRoot
$stdoutLogPath = Join-Path $runtimeDir 'monitor.runtime.log'
$stderrLogPath = Join-Path $runtimeDir 'monitor.runtime.err.log'

$sitePackages = if (Test-Path (Join-Path $projectRoot '.venv\Lib\site-packages')) {
    Join-Path $projectRoot '.venv\Lib\site-packages'
} else {
    Join-Path (Split-Path $projectRoot -Parent) '.venv\Lib\site-packages'
}

Set-Location $projectRoot
Normalize-MonitorProcessEnvironment
Export-MonitorEnvConfigToProcess -EnvConfig $envConfig
$env:PYTHONPATH = "$projectRoot;$sitePackages"

$cleanupResult = Stop-MonitorServiceInstance -ProjectRoot $projectRoot -HostAddress $hostAddress -Port $port -TimeoutSeconds 6
if (-not $cleanupResult.success -and $cleanupResult.pending_listener_pids.Count -gt 0) {
    throw "Monitor service port $port is still occupied by PID(s): $($cleanupResult.pending_listener_pids -join ', ')"
}

Write-Host "Starting monitor service at http://$hostAddress`:$port/" -ForegroundColor Green
Write-Host "Launching monitor service in background..." -ForegroundColor Yellow
Write-Host ''

$launchArguments = @()
if ($pythonInfo.args.Count -gt 0) {
    $launchArguments += $pythonInfo.args
}
$launchArguments += @('-m', 'uvicorn', 'monitor_app.api:app', '--host', $hostAddress, '--port', [string]$port)

$process = $null
try {
    $process = Start-Process `
        -FilePath $pythonInfo.exe `
        -ArgumentList $launchArguments `
        -WorkingDirectory $projectRoot `
        -RedirectStandardOutput $stdoutLogPath `
        -RedirectStandardError $stderrLogPath `
        -WindowStyle Hidden `
        -PassThru

    Write-Host "Monitor service process started in background (PID=$($process.Id))." -ForegroundColor DarkGray

    $deadline = (Get-Date).AddSeconds(60)
    $started = $false
    $consecutiveHealthyChecks = 0
    while ((Get-Date) -lt $deadline) {
        Start-Sleep -Milliseconds 500
        if ($process.HasExited) {
            throw "Monitor service exited before startup completed (PID=$($process.Id), exit_code=$($process.ExitCode)). See logs: $stdoutLogPath ; $stderrLogPath"
        }
        try {
            if (Test-ServiceHealth -HostAddress $hostAddress -Port $port) {
                $consecutiveHealthyChecks += 1
                if ($consecutiveHealthyChecks -ge 2) {
                    $started = $true
                    break
                }
            } else {
                $consecutiveHealthyChecks = 0
            }
        } catch {
            $consecutiveHealthyChecks = 0
        }
    }

    if (-not $started) {
        throw "Monitor service failed to start: health check did not pass within timeout. See logs: $stdoutLogPath ; $stderrLogPath"
    }

    Start-Sleep -Seconds 1
    if ($process.HasExited) {
        throw "Monitor service exited immediately after startup (PID=$($process.Id), exit_code=$($process.ExitCode)). See logs: $stdoutLogPath ; $stderrLogPath"
    }
    if (-not (Test-ServiceHealth -HostAddress $hostAddress -Port $port)) {
        throw "Monitor service health check did not stay healthy. See logs: $stdoutLogPath ; $stderrLogPath"
    }

    $listenerPids = Get-ListenerPids -Port $port
    if (-not $listenerPids -or $listenerPids.Count -eq 0) {
        throw "Monitor service started but no listening PID was detected on port $port"
    }

    Write-MonitorState -StatePath $statePath -State @{
        host = $hostAddress
        port = $port
        launcher_pid = $process.Id
        service_pid = [int]$listenerPids[0]
        listener_pids = @($listenerPids)
        started_at = (Get-Date).ToString('o')
        stdout_log = $stdoutLogPath
        stderr_log = $stderrLogPath
    }
} catch {
    if ($process) {
        Stop-MonitorProcessSet -ProcessIds @($process.Id) | Out-Null
    }
    Clear-MonitorState -StatePath $statePath
    throw
}

Write-Host "Monitor service is healthy at http://$hostAddress`:$port/" -ForegroundColor Green
