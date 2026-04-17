@echo off
setlocal

cd /d "%~dp0"
title Monitor Service Restart
set "POWERSHELL=%SystemRoot%\System32\WindowsPowerShell\v1.0\powershell.exe"
set "RESTART_SCRIPT=%~dp0scripts\restart_monitor_service.ps1"

echo ========================================
echo   Restarting monitor service...
echo ========================================
echo.

"%POWERSHELL%" -NoProfile -ExecutionPolicy Bypass -File "%RESTART_SCRIPT%"
set "EXIT_CODE=%ERRORLEVEL%"

echo.
if "%EXIT_CODE%"=="0" (
    echo [monitor] Service restarted successfully.
    echo [monitor] URL: http://127.0.0.1:8010/
) else (
    echo [monitor] Service restart failed. Exit code: %EXIT_CODE%
)

echo.
pause
exit /b %EXIT_CODE%
