$ErrorActionPreference = 'Stop'

function Get-MonitorCmdPath {
    $candidate = Join-Path $env:SystemRoot 'System32\cmd.exe'
    if (Test-Path $candidate) {
        return $candidate
    }
    return 'cmd.exe'
}

function Get-MonitorNetstatPath {
    $candidate = Join-Path $env:SystemRoot 'System32\netstat.exe'
    if (Test-Path $candidate) {
        return $candidate
    }
    return 'netstat.exe'
}

function Get-MonitorTaskkillPath {
    $candidate = Join-Path $env:SystemRoot 'System32\taskkill.exe'
    if (Test-Path $candidate) {
        return $candidate
    }
    return 'taskkill.exe'
}

function Get-MonitorPowerShellPath {
    $candidate = Join-Path $env:SystemRoot 'System32\WindowsPowerShell\v1.0\powershell.exe'
    if (Test-Path $candidate) {
        return $candidate
    }
    return 'powershell.exe'
}

function Test-MonitorElevationBypass {
    $value = [string]$env:MONITOR_SCRIPT_SKIP_ELEVATION
    if (-not $value) {
        return $false
    }

    switch ($value.Trim().ToLowerInvariant()) {
        '1' { return $true }
        'true' { return $true }
        'yes' { return $true }
        'on' { return $true }
        default { return $false }
    }
}

function Ensure-MonitorAdmin {
    param(
        [Parameter(Mandatory = $true)]
        [string]$ScriptPath,
        [switch]$KeepWindowOpen,
        [string[]]$ScriptArguments = @()
    )

    if (Test-MonitorElevationBypass) {
        return $true
    }

    $currentIdentity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = New-Object Security.Principal.WindowsPrincipal($currentIdentity)
    $isAdministrator = $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
    if ($isAdministrator) {
        return $true
    }

    $argumentList = @()
    if ($KeepWindowOpen) {
        $argumentList += '-NoExit'
    }
    $argumentList += '-ExecutionPolicy', 'Bypass', '-File', $ScriptPath
    if ($ScriptArguments) {
        $argumentList += @($ScriptArguments | Where-Object { $_ -ne $null })
    }
    Start-Process -FilePath (Get-MonitorPowerShellPath) -ArgumentList $argumentList -WindowStyle Normal -Verb RunAs | Out-Null
    return $false
}

function Read-EnvConfig {
    param([string]$Path)

    $config = @{}
    if (-not (Test-Path $Path)) {
        return $config
    }

    foreach ($line in Get-Content $Path) {
        $trimmed = $line.Trim()
        if (-not $trimmed -or $trimmed.StartsWith('#')) {
            continue
        }

        $parts = $trimmed -split '=', 2
        if ($parts.Count -eq 2) {
            $config[$parts[0].Trim()] = $parts[1].Trim()
        }
    }

    return $config
}

function Get-MonitorHostAddress {
    param([hashtable]$EnvConfig)

    if ($env:MONITOR_API_HOST_OVERRIDE) {
        return [string]$env:MONITOR_API_HOST_OVERRIDE
    }

    if ($EnvConfig.ContainsKey('MONITOR_API_HOST') -and $EnvConfig['MONITOR_API_HOST']) {
        return [string]$EnvConfig['MONITOR_API_HOST']
    }

    return '127.0.0.1'
}

function Get-MonitorHealthHostAddress {
    param([string]$HostAddress)

    $normalized = ([string]$HostAddress).Trim()
    if (-not $normalized) {
        return '127.0.0.1'
    }

    switch ($normalized.ToLowerInvariant()) {
        '0.0.0.0' { return '127.0.0.1' }
        '::' { return '::1' }
        '[::]' { return '::1' }
        default { return $normalized }
    }
}

function Get-MonitorPort {
    param([hashtable]$EnvConfig)

    if ($env:MONITOR_API_PORT_OVERRIDE) {
        return [int]$env:MONITOR_API_PORT_OVERRIDE
    }

    if ($EnvConfig.ContainsKey('MONITOR_API_PORT') -and $EnvConfig['MONITOR_API_PORT']) {
        return [int]$EnvConfig['MONITOR_API_PORT']
    }

    return 8010
}

function Get-MonitorRuntimeDir {
    param([string]$ProjectRoot)

    $runtimeDir = if ($env:MONITOR_RUNTIME_DIR) {
        [string]$env:MONITOR_RUNTIME_DIR
    } else {
        Join-Path $ProjectRoot 'data\runtime'
    }

    if (-not (Test-Path $runtimeDir)) {
        New-Item -ItemType Directory -Force -Path $runtimeDir | Out-Null
    }

    return $runtimeDir
}

function Get-MonitorStatePath {
    param([string]$ProjectRoot)

    return (Join-Path (Get-MonitorRuntimeDir -ProjectRoot $ProjectRoot) 'monitor-service.state.json')
}

function Read-MonitorState {
    param([string]$StatePath)

    if (-not (Test-Path $StatePath)) {
        return $null
    }

    try {
        return (Get-Content -Raw -Path $StatePath | ConvertFrom-Json)
    } catch {
        return $null
    }
}

function Write-MonitorState {
    param(
        [string]$StatePath,
        [hashtable]$State
    )

    $directory = Split-Path $StatePath -Parent
    if ($directory -and -not (Test-Path $directory)) {
        New-Item -ItemType Directory -Force -Path $directory | Out-Null
    }

    $State | ConvertTo-Json -Depth 5 | Set-Content -Path $StatePath -Encoding UTF8
}

function Clear-MonitorState {
    param([string]$StatePath)

    if (Test-Path $StatePath) {
        Remove-Item -Path $StatePath -Force -ErrorAction SilentlyContinue
    }
}

function Get-ListenerPids {
    param([int]$Port)

    $pids = New-Object System.Collections.Generic.HashSet[int]

    try {
        $connections = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction Stop
        foreach ($connection in $connections) {
            [void]$pids.Add([int]$connection.OwningProcess)
        }
    } catch {
    }

    $netstatLines = & (Get-MonitorNetstatPath) -ano -p tcp
    foreach ($line in $netstatLines) {
        if ($line -match "^\s*TCP\s+\S+:$Port\s+\S+\s+LISTENING\s+(\d+)\s*$") {
            [void]$pids.Add([int]$matches[1])
        }
    }

    return @($pids)
}

function Test-ServiceHealth {
    param(
        [string]$HostAddress,
        [int]$Port,
        [int]$TimeoutMilliseconds = 3000
    )

    $probeHostAddress = Get-MonitorHealthHostAddress -HostAddress $HostAddress
    $uri = "http://$probeHostAddress`:$Port/healthz"
    $request = [System.Net.HttpWebRequest]::Create($uri)
    $request.Method = 'GET'
    $request.Timeout = $TimeoutMilliseconds
    $request.ReadWriteTimeout = $TimeoutMilliseconds
    $request.KeepAlive = $false
    $request.Proxy = $null

    try {
        $response = [System.Net.HttpWebResponse]$request.GetResponse()
        try {
            return ($response.StatusCode -eq [System.Net.HttpStatusCode]::OK)
        } finally {
            $response.Close()
        }
    } catch [System.Net.WebException] {
        return $false
    } catch {
        return $false
    }
}

function Invoke-MonitorProcessTermination {
    param([int]$ProcessId)

    if ($ProcessId -le 0) {
        return $false
    }

    try {
        Stop-Process -Id $ProcessId -Force -ErrorAction Stop
        return $true
    } catch {
    }

    try {
        $proc = Get-CimInstance Win32_Process -Filter "ProcessId = $ProcessId" -ErrorAction Stop
        if ($proc) {
            $result = Invoke-CimMethod -InputObject $proc -MethodName Terminate -Arguments @{ Reason = 0 }
            return ($result.ReturnValue -eq 0)
        }
    } catch {
    }

    try {
        & (Get-MonitorTaskkillPath) /PID $ProcessId /F /T *> $null
        if ($LASTEXITCODE -eq 0) {
            return $true
        }
    } catch {
    }

    return $false
}

function Stop-MonitorProcessSet {
    param([int[]]$ProcessIds)

    $unique = New-Object System.Collections.Generic.HashSet[int]
    $stopped = New-Object System.Collections.Generic.List[int]

    foreach ($processId in $ProcessIds) {
        if ($processId -gt 0) {
            [void]$unique.Add([int]$processId)
        }
    }

    foreach ($processId in $unique) {
        if (Invoke-MonitorProcessTermination -ProcessId $processId) {
            [void]$stopped.Add([int]$processId)
        }
    }

    return @($stopped)
}

function Get-OrderedStopTargets {
    param(
        [object]$State,
        [int[]]$ListenerPids
    )

    $ordered = New-Object System.Collections.Generic.List[int]
    $seen = New-Object System.Collections.Generic.HashSet[int]

    foreach ($listenerPid in $ListenerPids) {
        if ($listenerPid -gt 0 -and $seen.Add([int]$listenerPid)) {
            [void]$ordered.Add([int]$listenerPid)
        }
    }

    if ($State) {
        foreach ($property in @('service_pid', 'launcher_pid')) {
            $value = $State.$property
            if ($null -ne $value) {
                $processId = [int]$value
                if ($processId -gt 0 -and $seen.Add($processId)) {
                    [void]$ordered.Add($processId)
                }
            }
        }

        if ($State.listener_pids) {
            foreach ($listenerPid in $State.listener_pids) {
                $processId = [int]$listenerPid
                if ($processId -gt 0 -and $seen.Add($processId)) {
                    [void]$ordered.Add($processId)
                }
            }
        }
    }

    return @($ordered)
}

function Stop-MonitorServiceInstance {
    param(
        [Parameter(Mandatory = $true)]
        [string]$ProjectRoot,
        [Parameter(Mandatory = $true)]
        [string]$HostAddress,
        [Parameter(Mandatory = $true)]
        [int]$Port,
        [int]$TimeoutSeconds = 10
    )

    $statePath = Get-MonitorStatePath -ProjectRoot $ProjectRoot
    $state = Read-MonitorState -StatePath $statePath

    $listenerPids = Get-ListenerPids -Port $Port
    $attemptedPids = Get-OrderedStopTargets -State $state -ListenerPids $listenerPids
    $stoppedPids = Stop-MonitorProcessSet -ProcessIds $attemptedPids

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    $portFree = $false
    while ((Get-Date) -lt $deadline) {
        Start-Sleep -Milliseconds 400

        $remainingListeners = Get-ListenerPids -Port $Port
        if (-not $remainingListeners -or $remainingListeners.Count -eq 0) {
            try {
                if (-not (Test-ServiceHealth -HostAddress $HostAddress -Port $Port)) {
                    $portFree = $true
                    break
                }
            } catch {
                $portFree = $true
                break
            }
        }
    }

    $pendingListeners = Get-ListenerPids -Port $Port
    if (-not $pendingListeners -or $pendingListeners.Count -eq 0) {
        try {
            if (-not (Test-ServiceHealth -HostAddress $HostAddress -Port $Port)) {
                $portFree = $true
            }
        } catch {
            $portFree = $true
        }
    }

    if ($portFree) {
        Clear-MonitorState -StatePath $statePath
    }

    return [pscustomobject]@{
        state_path = $statePath
        state_found = ($null -ne $state)
        attempted_pids = @($attemptedPids)
        stopped_pids = @($stoppedPids)
        pending_listener_pids = @($pendingListeners)
        success = $portFree
    }
}
