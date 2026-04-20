param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$PytestArgs
)

$ErrorActionPreference = 'Stop'

$projectRoot = Split-Path $PSScriptRoot -Parent
$commonScript = Join-Path $PSScriptRoot 'monitor_service_common.ps1'
$envFile = Join-Path $projectRoot '.env'
$localPyVenvConfig = Join-Path $projectRoot '.venv\pyvenv.cfg'
$localVenvSitePackages = Join-Path $projectRoot '.venv\Lib\site-packages'
$runtimeDir = Join-Path $projectRoot 'data\runtime'
$stdoutLogPath = Join-Path $runtimeDir 'pytest.stdout.log'
$stderrLogPath = Join-Path $runtimeDir 'pytest.stderr.log'

if (-not (Test-Path $commonScript)) {
    throw "Missing monitor service helper script: $commonScript"
}
. $commonScript

if (-not (Ensure-MonitorAdmin -ScriptPath $PSCommandPath -KeepWindowOpen)) {
    return
}

function Resolve-TestPython {
    param([string]$ProjectRoot)

    $envConfig = Read-EnvConfig -Path $envFile
    if ($envConfig.ContainsKey('MONITOR_PYTHON') -and $envConfig['MONITOR_PYTHON']) {
        return [string]$envConfig['MONITOR_PYTHON']
    }

    if (Test-Path $localPyVenvConfig) {
        $homeLine = Get-Content $localPyVenvConfig | Where-Object { $_ -like 'home = *' } | Select-Object -First 1
        if ($homeLine) {
            $homePath = $homeLine.Substring(7).Trim()
            if ($homePath) {
                $candidate = Join-Path $homePath 'python.exe'
                if (Test-Path $candidate) {
                    return $candidate
                }
            }
        }
    }

    $pythonCommand = Get-Command python -ErrorAction SilentlyContinue
    if ($pythonCommand) {
        return $pythonCommand.Source
    }

    throw 'No runnable Python executable was found. Configure MONITOR_PYTHON in .env first.'
}

function Get-TestSitePackages {
    param([string]$ProjectRoot)

    if (Test-Path $localVenvSitePackages) {
        return $localVenvSitePackages
    }

    $parentSitePackages = Join-Path (Split-Path $ProjectRoot -Parent) '.venv\Lib\site-packages'
    if (Test-Path $parentSitePackages) {
        return $parentSitePackages
    }

    throw 'Virtual environment site-packages directory was not found.'
}

function Normalize-TestProcessEnvironment {
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

function Try-RewriteUtf8Log {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path,
        [Parameter(Mandatory = $true)]
        [string]$Content
    )

    try {
        Set-Content -Path $Path -Value $Content -Encoding UTF8
    } catch {
        Write-Warning "Failed to rewrite log as UTF-8: $Path ($($_.Exception.Message))"
    }
}

if (-not (Test-Path $runtimeDir)) {
    New-Item -ItemType Directory -Path $runtimeDir -Force | Out-Null
}

$pythonExe = Resolve-TestPython -ProjectRoot $projectRoot
$sitePackages = Get-TestSitePackages -ProjectRoot $projectRoot
$pytestArguments = @('-m', 'pytest')
if ($PytestArgs -and $PytestArgs.Count -gt 0) {
    $pytestArguments += $PytestArgs
}

Write-Host "Using Python: $pythonExe" -ForegroundColor Cyan
Write-Host "Using site-packages: $sitePackages" -ForegroundColor Cyan
Write-Host "Running: python -m pytest $($PytestArgs -join ' ')" -ForegroundColor Yellow

Normalize-TestProcessEnvironment
$previousPythonPath = [System.Environment]::GetEnvironmentVariable('PYTHONPATH', 'Process')
$env:PYTHONPATH = "$projectRoot;$sitePackages"

try {
    $process = Start-Process `
        -FilePath $pythonExe `
        -ArgumentList $pytestArguments `
        -WorkingDirectory $projectRoot `
        -RedirectStandardOutput $stdoutLogPath `
        -RedirectStandardError $stderrLogPath `
        -WindowStyle Hidden `
        -PassThru `
        -Wait
} finally {
    [System.Environment]::SetEnvironmentVariable('PYTHONPATH', $previousPythonPath, 'Process')
}

if (-not $process) {
    throw 'Failed to start backend test process.'
}

$stdout = if (Test-Path $stdoutLogPath) { Get-Content -Raw -Path $stdoutLogPath } else { '' }
$stderr = if (Test-Path $stderrLogPath) { Get-Content -Raw -Path $stderrLogPath } else { '' }

Try-RewriteUtf8Log -Path $stdoutLogPath -Content $stdout
Try-RewriteUtf8Log -Path $stderrLogPath -Content $stderr

if ($stdout) {
    Write-Host $stdout
}
if ($stderr) {
    Write-Error $stderr
}

if ($process.ExitCode -ne 0) {
    throw "Backend tests failed with exit code $($process.ExitCode). Logs: $stdoutLogPath ; $stderrLogPath"
}

Write-Host 'Backend tests completed successfully.' -ForegroundColor Green
