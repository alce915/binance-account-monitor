@echo off
setlocal EnableExtensions

cd /d "%~dp0"
title Monitor Console Launcher
set "POWERSHELL=%SystemRoot%\System32\WindowsPowerShell\v1.0\powershell.exe"
set "START_SCRIPT=%~dp0scripts\start_monitor_console.ps1"
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
