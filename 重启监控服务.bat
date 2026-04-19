@echo off
setlocal EnableExtensions

cd /d "%~dp0"
title Monitor Service Restart
set "POWERSHELL=%SystemRoot%\System32\WindowsPowerShell\v1.0\powershell.exe"
set "RESTART_SCRIPT=%~dp0scripts\restart_monitor_service.ps1"
set "FLTMC=%SystemRoot%\System32\fltmc.exe"

"%FLTMC%" >nul 2>&1
if not "%~1"=="__elevated__" if errorlevel 1 (
    echo ========================================
    echo   Requesting administrator access...
    echo ========================================
    echo.
    "%POWERSHELL%" -NoProfile -ExecutionPolicy Bypass -Command "Start-Process -FilePath '%~f0' -ArgumentList '__elevated__' -Verb RunAs"
    if errorlevel 1 (
        echo [monitor] Failed to request administrator access.
        echo.
        pause
        exit /b 1
    )
    exit /b 0
)

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
