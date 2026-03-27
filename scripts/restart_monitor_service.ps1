$ErrorActionPreference = 'Stop'

$projectRoot = Split-Path $PSScriptRoot -Parent
$localPyVenvConfig = Join-Path $projectRoot '.venv\pyvenv.cfg'
$localVenvPython = Join-Path $projectRoot '.venv\Scripts\python.exe'
$parentVenvPython = Join-Path (Split-Path $projectRoot -Parent) '.venv\Scripts\python.exe'
$envFile = Join-Path $projectRoot '.env'
$envExampleFile = Join-Path $projectRoot '.env.example'
$accountsFile = Join-Path $projectRoot 'config\binance_monitor_accounts.json'
$accountsExampleFile = Join-Path $projectRoot 'config\binance_monitor_accounts.example.json'
$pythonExecutable = $null
$bootstrapMessages = [System.Collections.Generic.List[string]]::new()

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

if (-not (Test-Path $envFile) -and (Test-Path $envExampleFile)) {
    Copy-Item -Path $envExampleFile -Destination $envFile
    $bootstrapMessages.Add("Created .env from .env.example")
}

if (-not (Test-Path $accountsFile) -and (Test-Path $accountsExampleFile)) {
    Copy-Item -Path $accountsExampleFile -Destination $accountsFile
    $bootstrapMessages.Add("Created config\\binance_monitor_accounts.json from example template")
}

if ($bootstrapMessages.Count -gt 0) {
    $bootstrapMessages.Add('Fill in your real Binance API credentials in config\binance_monitor_accounts.json, then run the restart script again.')
    throw ($bootstrapMessages -join [Environment]::NewLine)
}

if (Test-PlaceholderCredentials -Path $accountsFile) {
    throw "Placeholder Binance API credentials detected in $accountsFile. Fill in your real credentials, then run the restart script again."
}

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

$sitePackages = if (Test-Path (Join-Path $projectRoot '.venv\Lib\site-packages')) {
    Join-Path $projectRoot '.venv\Lib\site-packages'
} else {
    Join-Path (Split-Path $projectRoot -Parent) '.venv\Lib\site-packages'
}
$envConfig = @{}
if (Test-Path $envFile) {
    foreach ($line in Get-Content $envFile) {
        $trimmed = $line.Trim()
        if (-not $trimmed -or $trimmed.StartsWith('#')) {
            continue
        }
        $parts = $trimmed -split '=', 2
        if ($parts.Count -eq 2) {
            $envConfig[$parts[0].Trim()] = $parts[1].Trim()
        }
    }
}

$hostAddress = if ($envConfig.ContainsKey('MONITOR_API_HOST') -and $envConfig['MONITOR_API_HOST']) {
    $envConfig['MONITOR_API_HOST']
} else {
    '127.0.0.1'
}

$port = 8010
if ($envConfig.ContainsKey('MONITOR_API_PORT') -and $envConfig['MONITOR_API_PORT']) {
    try {
        $port = [int]$envConfig['MONITOR_API_PORT']
    } catch {
        throw "MONITOR_API_PORT must be an integer in $envFile"
    }
}

if ($port -eq 8000) {
    throw "MONITOR_API_PORT=8000 is not allowed for this local deployment. Please use another local port such as 8010."
}

$runtimeLog = Join-Path $projectRoot 'monitor.runtime.log'
$healthUrl = "http://{0}:{1}/healthz" -f $hostAddress, $port
$pythonPathValue = "$sitePackages;$projectRoot"

if (-not (Test-Path $pythonExecutable)) {
    throw "Python executable not found. Create a .venv in the monitor project or ensure the parent workspace .venv exists."
}

Set-Location $projectRoot

try {
    $connections = Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction Stop
} catch {
    $connections = @()
}

foreach ($connection in $connections) {
    try {
        Stop-Process -Id $connection.OwningProcess -Force -ErrorAction Stop
        Start-Sleep -Milliseconds 300
    } catch {
    }
}

if (Test-Path $runtimeLog) {
    Remove-Item $runtimeLog -Force -ErrorAction SilentlyContinue
}

$childCommand = "Set-Location '$projectRoot'; `$env:PYTHONPATH = '$pythonPathValue'; & '$pythonExecutable' -m uvicorn monitor_app.api:app --host $hostAddress --port $port *>> '$runtimeLog'"
Start-Process -FilePath 'powershell.exe' -ArgumentList '-NoProfile','-ExecutionPolicy','Bypass','-Command',$childCommand -WindowStyle Hidden | Out-Null

$deadline = (Get-Date).AddSeconds(20)
$started = $false
while ((Get-Date) -lt $deadline) {
    Start-Sleep -Milliseconds 500
    try {
        $response = Invoke-WebRequest -UseBasicParsing $healthUrl -TimeoutSec 2
        if ($response.StatusCode -eq 200) {
            $started = $true
            break
        }
    } catch {
    }
}

if (-not $started) {
    $runtime = if (Test-Path $runtimeLog) { Get-Content $runtimeLog -Raw -ErrorAction SilentlyContinue } else { '' }
    if (-not $runtime) {
        $runtime = 'monitor service did not become healthy within timeout'
    }
    throw "Monitor service restart failed: $runtime"
}

$listener = Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction Stop | Select-Object -First 1
$statusLines = [System.Collections.Generic.List[string]]::new()
$statusLines.Add("Monitor service restarted successfully on http://$hostAddress`:$port/ (PID=$($listener.OwningProcess))")
foreach ($message in $bootstrapMessages) {
    $statusLines.Add($message)
}
if ($bootstrapMessages.Count -gt 0) {
    $statusLines.Add('Fill in your real Binance API credentials before relying on live monitor data.')
}

$statusLines -join [Environment]::NewLine
