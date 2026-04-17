@echo off
setlocal

cd /d "%~dp0"
title Monitor Service Stop
set "POWERSHELL=%SystemRoot%\System32\WindowsPowerShell\v1.0\powershell.exe"
set "STOP_SCRIPT=%~dp0scripts\stop_monitor_service.ps1"

echo ========================================
echo   Stopping monitor service...
echo ========================================
echo.

"%POWERSHELL%" -NoProfile -ExecutionPolicy Bypass -File "%STOP_SCRIPT%"

set "EXIT_CODE=%ERRORLEVEL%"

echo.
if "%EXIT_CODE%"=="0" (
    echo [monitor] Stop command finished.
    exit /b 0
) else (
    echo [monitor] Stop failed. Exit code: %EXIT_CODE%
)

echo.
pause
exit /b %EXIT_CODE%
