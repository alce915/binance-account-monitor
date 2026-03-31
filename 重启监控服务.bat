@echo off
setlocal

cd /d "%~dp0"
title Monitor Service Restart

echo ========================================
echo   Restarting monitor service...
echo ========================================
echo.

powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\restart_monitor_service.ps1"
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
