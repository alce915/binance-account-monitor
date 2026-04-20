param(
    [string]$MasterKeyFile = '',
    [string]$EnvFile = ''
)

$ErrorActionPreference = 'Stop'

$projectRoot = Split-Path $PSScriptRoot -Parent
$commonScript = Join-Path $PSScriptRoot 'monitor_service_common.ps1'
$defaultEnvFile = Join-Path $projectRoot '.env'
$localPyVenvConfig = Join-Path $projectRoot '.venv\pyvenv.cfg'
$localVenvSitePackages = Join-Path $projectRoot '.venv\Lib\site-packages'
$runtimeDir = Join-Path $projectRoot 'data\runtime'
$stdoutLogPath = Join-Path $runtimeDir 'secret_migration.stdout.log'
$stderrLogPath = Join-Path $runtimeDir 'secret_migration.stderr.log'

if (-not (Test-Path $commonScript)) {
    throw "Missing monitor service helper script: $commonScript"
}
. $commonScript

$elevationScriptArgs = @()
if ($MasterKeyFile) {
    $elevationScriptArgs += '-MasterKeyFile'
    $elevationScriptArgs += $MasterKeyFile
}
if ($EnvFile) {
    $elevationScriptArgs += '-EnvFile'
    $elevationScriptArgs += $EnvFile
}

if (-not (Ensure-MonitorAdmin -ScriptPath $PSCommandPath -KeepWindowOpen -ScriptArguments $elevationScriptArgs)) {
    return
}

function Resolve-MigrationPython {
    param([string]$ProjectRoot)

    $envConfig = Read-EnvConfig -Path $defaultEnvFile
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

function Get-MigrationSitePackages {
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

function Normalize-MigrationProcessEnvironment {
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

function Invoke-MigrationCommand {
    param(
        [string]$PythonExe,
        [string[]]$ArgumentList
    )

    Normalize-MigrationProcessEnvironment
    $previousPythonPath = [System.Environment]::GetEnvironmentVariable('PYTHONPATH', 'Process')
    $env:PYTHONPATH = "$projectRoot;$sitePackages"

    try {
        $process = Start-Process `
            -FilePath $PythonExe `
            -ArgumentList $ArgumentList `
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
        throw 'Failed to start secret migration process.'
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
        throw "Secret migration failed with exit code $($process.ExitCode). Logs: $stdoutLogPath ; $stderrLogPath"
    }
}

function Ensure-MigrationDependency {
    param(
        [string]$PythonExe,
        [string]$SitePackages
    )

    $cryptographyPackagePath = Join-Path $SitePackages 'cryptography'
    if (Test-Path $cryptographyPackagePath) {
        return
    }

    Write-Host 'Installing cryptography into local site-packages...' -ForegroundColor Yellow
    Invoke-MigrationCommand -PythonExe $PythonExe -ArgumentList @(
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

$pythonExe = Resolve-MigrationPython -ProjectRoot $projectRoot
$sitePackages = Get-MigrationSitePackages -ProjectRoot $projectRoot
$resolvedEnvFile = if ($EnvFile) { $EnvFile } else { '.env' }
$resolvedMasterKeyFile = if ($MasterKeyFile) { $MasterKeyFile } else { '.local-secrets/monitor-master-key' }

Write-Host "Using Python: $pythonExe" -ForegroundColor Cyan
Write-Host "Using site-packages: $sitePackages" -ForegroundColor Cyan
Write-Host "Master key file: $resolvedMasterKeyFile" -ForegroundColor Yellow
Write-Host "Environment file: $resolvedEnvFile" -ForegroundColor Yellow

Ensure-MigrationDependency -PythonExe $pythonExe -SitePackages $sitePackages

Invoke-MigrationCommand -PythonExe $pythonExe -ArgumentList @(
    '-m', 'monitor_app.secrets_cli', 'init',
    '--master-key-file', $resolvedMasterKeyFile
)

Invoke-MigrationCommand -PythonExe $pythonExe -ArgumentList @(
    '-m', 'monitor_app.secrets_cli', 'migrate',
    '--write-config',
    '--master-key-file', $resolvedMasterKeyFile,
    '--env-file', $resolvedEnvFile
)

Write-Host 'Local secret migration completed successfully.' -ForegroundColor Green
