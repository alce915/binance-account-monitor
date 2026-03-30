@echo off
setlocal

cd /d "%~dp0"
title 亢龙监控 - 停止服务

echo ========================================
echo  正在停止亢龙监控服务...
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
  "if (-not $connections -or $connections.Count -eq 0) { Write-Output ('未发现端口 ' + $port + ' 上的监控服务进程。'); exit 0 };" ^
  "$stopped = 0;" ^
  "foreach ($connection in $connections) { try { Stop-Process -Id $connection.OwningProcess -Force -ErrorAction Stop; $stopped++; Start-Sleep -Milliseconds 300 } catch {} };" ^
  "Write-Output ('已停止监控服务进程数: ' + $stopped)"

set "EXIT_CODE=%ERRORLEVEL%"

echo.
if "%EXIT_CODE%"=="0" (
    echo 停止命令执行完成。
) else (
    echo 停止服务失败，错误码: %EXIT_CODE%
)

echo.
pause
exit /b %EXIT_CODE%
