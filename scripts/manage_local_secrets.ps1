param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$CliArgs
)

$ErrorActionPreference = 'Stop'

$projectRoot = Split-Path $PSScriptRoot -Parent
$commonScript = Join-Path $PSScriptRoot 'monitor_service_common.ps1'
$envFile = Join-Path $projectRoot '.env'
$localPyVenvConfig = Join-Path $projectRoot '.venv\pyvenv.cfg'
$localVenvSitePackages = Join-Path $projectRoot '.venv\Lib\site-packages'
$runtimeDir = Join-Path $projectRoot 'data\runtime'
$stdoutLogPath = Join-Path $runtimeDir 'manage_secrets.stdout.log'
$stderrLogPath = Join-Path $runtimeDir 'manage_secrets.stderr.log'

if (-not (Test-Path $commonScript)) {
    throw "Missing monitor service helper script: $commonScript"
}
. $commonScript

$normalizedCliArgs = @($CliArgs | Where-Object { $_ -ne $null })

if (-not (Ensure-MonitorAdmin -ScriptPath $PSCommandPath -KeepWindowOpen -ScriptArguments $normalizedCliArgs)) {
    return
}

function Resolve-SecretsPython {
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

function Get-SecretsSitePackages {
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

function Normalize-SecretsProcessEnvironment {
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

function Invoke-SecretsCommand {
    param(
        [string]$PythonExe,
        [string[]]$ArgumentList
    )

    Normalize-SecretsProcessEnvironment
    $previousPythonPath = [System.Environment]::GetEnvironmentVariable('PYTHONPATH', 'Process')
    $env:PYTHONPATH = "$projectRoot;$sitePackages"
    $normalizedArgumentList = @($ArgumentList | Where-Object { -not [string]::IsNullOrWhiteSpace([string]$_) })

    try {
        $startProcessParams = @{
            FilePath = $PythonExe
            WorkingDirectory = $projectRoot
            RedirectStandardOutput = $stdoutLogPath
            RedirectStandardError = $stderrLogPath
            WindowStyle = 'Hidden'
            PassThru = $true
            Wait = $true
        }
        if ($normalizedArgumentList.Count -gt 0) {
            $startProcessParams.ArgumentList = $normalizedArgumentList
        }
        $process = Start-Process @startProcessParams
    } finally {
        [System.Environment]::SetEnvironmentVariable('PYTHONPATH', $previousPythonPath, 'Process')
    }

    if (-not $process) {
        throw 'Failed to start local secrets command.'
    }

    $stdout = if (Test-Path $stdoutLogPath) { Get-Content -Raw -Path $stdoutLogPath } else { '' }
    $stderr = if (Test-Path $stderrLogPath) { Get-Content -Raw -Path $stderrLogPath } else { '' }

    Set-Content -Path $stdoutLogPath -Value $stdout -Encoding UTF8
    Set-Content -Path $stderrLogPath -Value $stderr -Encoding UTF8

    if ($stdout) {
        Write-Host $stdout
    }
    if ($stderr) {
        Write-Error $stderr
    }

    if ($process.ExitCode -ne 0) {
        throw "Local secrets command failed with exit code $($process.ExitCode). Logs: $stdoutLogPath ; $stderrLogPath"
    }
}

function Ensure-SecretsDependency {
    param(
        [string]$PythonExe,
        [string]$SitePackages
    )

    $cryptographyPackagePath = Join-Path $SitePackages 'cryptography'
    if (Test-Path $cryptographyPackagePath) {
        return
    }

    Write-Host 'Installing cryptography into local site-packages...' -ForegroundColor Yellow
    Invoke-SecretsCommand -PythonExe $PythonExe -ArgumentList @(
        '-m', 'pip', 'install',
        '--disable-pip-version-check',
        '--upgrade',
        '--target', $SitePackages,
        'cryptography>=43,<46'
    )
}

if (-not (Test-Path $runtimeDir)) {
    New-Item -ItemType Directory -Path $runtimeDir -Force | Out-Null
}

$pythonExe = Resolve-SecretsPython -ProjectRoot $projectRoot
$sitePackages = Get-SecretsSitePackages -ProjectRoot $projectRoot

Write-Host "Using Python: $pythonExe" -ForegroundColor Cyan
Write-Host "Using site-packages: $sitePackages" -ForegroundColor Cyan

Ensure-SecretsDependency -PythonExe $pythonExe -SitePackages $sitePackages

$argumentList = @('-m', 'monitor_app.secrets_cli')
if ($normalizedCliArgs.Count -gt 0) {
    $argumentList += $normalizedCliArgs
} else {
    $argumentList += '--help'
}
Invoke-SecretsCommand -PythonExe $pythonExe -ArgumentList $argumentList

Write-Host 'Local secret command completed successfully.' -ForegroundColor Green
