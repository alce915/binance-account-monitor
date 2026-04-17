@echo off
setlocal

cd /d "%~dp0"
title Monitor Console Launcher
set "POWERSHELL=%SystemRoot%\System32\WindowsPowerShell\v1.0\powershell.exe"
set "START_SCRIPT=%~dp0scripts\start_monitor_console.ps1"

echo ========================================
echo   Opening monitor console...
echo ========================================
echo.

"%POWERSHELL%" -NoProfile -ExecutionPolicy Bypass -File "%START_SCRIPT%"
set "EXIT_CODE=%ERRORLEVEL%"

echo.
if "%EXIT_CODE%"=="0" (
    echo [monitor] Service launcher executed.
    echo [monitor] URL: http://127.0.0.1:8010/
    exit /b 0
)

echo [monitor] Failed to start the monitor service launcher. Exit code: %EXIT_CODE%
echo.
pause
exit /b %EXIT_CODE%
