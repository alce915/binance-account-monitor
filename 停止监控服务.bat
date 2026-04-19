@echo off
setlocal EnableExtensions

cd /d "%~dp0"
title Monitor Service Stop
set "POWERSHELL=%SystemRoot%\System32\WindowsPowerShell\v1.0\powershell.exe"
set "STOP_SCRIPT=%~dp0scripts\stop_monitor_service.ps1"
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
