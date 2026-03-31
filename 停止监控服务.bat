@echo off
setlocal

cd /d "%~dp0"
title Monitor Service Stop

echo ========================================
echo   Stopping monitor service...
echo ========================================
echo.

powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "$projectRoot = '%~dp0'.TrimEnd('\');" ^
  "$envFile = Join-Path $projectRoot '.env';" ^
  "$envConfig = @{};" ^
  "if (Test-Path $envFile) { foreach ($line in Get-Content $envFile) { $trimmed = $line.Trim(); if (-not $trimmed -or $trimmed.StartsWith('#')) { continue }; $parts = $trimmed -split '=', 2; if ($parts.Count -eq 2) { $envConfig[$parts[0].Trim()] = $parts[1].Trim() } } };" ^
  "$port = 8010;" ^
  "if ($envConfig.ContainsKey('MONITOR_API_PORT') -and $envConfig['MONITOR_API_PORT']) { try { $port = [int]$envConfig['MONITOR_API_PORT'] } catch {} };" ^
  "try { $connections = Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction Stop } catch { $connections = @() };" ^
  "if (-not $connections -or $connections.Count -eq 0) { Write-Output ('[monitor] No listening process found on port ' + $port + '.'); exit 0 };" ^
  "$stopped = 0;" ^
  "foreach ($connection in $connections) { try { Stop-Process -Id $connection.OwningProcess -Force -ErrorAction Stop; $stopped++; Start-Sleep -Milliseconds 300 } catch {} };" ^
  "Write-Output ('[monitor] Stopped process count: ' + $stopped)"

set "EXIT_CODE=%ERRORLEVEL%"

echo.
if "%EXIT_CODE%"=="0" (
    echo [monitor] Stop command finished.
) else (
    echo [monitor] Stop failed. Exit code: %EXIT_CODE%
)

echo.
pause
exit /b %EXIT_CODE%
