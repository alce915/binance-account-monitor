$ErrorActionPreference = 'Stop'

$projectRoot = Split-Path $PSScriptRoot -Parent
$commonScript = Join-Path $PSScriptRoot 'monitor_service_common.ps1'
$envFile = Join-Path $projectRoot '.env'
$localPyVenvConfig = Join-Path $projectRoot '.venv\pyvenv.cfg'
$localVenvSitePackages = Join-Path $projectRoot '.venv\Lib\site-packages'
$runtimeDir = Join-Path $projectRoot 'data\runtime'
$stdoutLogPath = Join-Path $runtimeDir 'pytest.stdout.log'
$stderrLogPath = Join-Path $runtimeDir 'pytest.stderr.log'

param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$PytestArgs
)

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

$psi = New-Object System.Diagnostics.ProcessStartInfo
$psi.FileName = $pythonExe
$psi.ArgumentList.Add('-m') | Out-Null
$psi.ArgumentList.Add('pytest') | Out-Null
foreach ($arg in $PytestArgs) {
    $psi.ArgumentList.Add($arg) | Out-Null
}
$psi.WorkingDirectory = $projectRoot
$psi.UseShellExecute = $false
$psi.RedirectStandardOutput = $true
$psi.RedirectStandardError = $true
$null = $psi.Environment
$psi.Environment['PYTHONPATH'] = "$projectRoot;$sitePackages"

$process = New-Object System.Diagnostics.Process
$process.StartInfo = $psi

if ($process.Start() -ne $true) {
    throw 'Failed to start backend test process.'
}

$stdout = $process.StandardOutput.ReadToEnd()
$stderr = $process.StandardError.ReadToEnd()
$process.WaitForExit()

Set-Content -Path $stdoutLogPath -Value $stdout -Encoding UTF8
Set-Content -Path $stderrLogPath -Value $stderr -Encoding UTF8

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
