@echo off
setlocal EnableExtensions

cd /d "%~dp0"
title Backend Test Runner
set "POWERSHELL=%SystemRoot%\System32\WindowsPowerShell\v1.0\powershell.exe"
set "TEST_SCRIPT=%~dp0scripts\run_backend_tests.ps1"
set "FLTMC=%SystemRoot%\System32\fltmc.exe"
set "TEST_STDOUT_LOG=%~dp0data\runtime\pytest.stdout.log"
set "TEST_STDERR_LOG=%~dp0data\runtime\pytest.stderr.log"

"%FLTMC%" >nul 2>&1
if not "%~1"=="__elevated__" if errorlevel 1 (
    echo ========================================
    echo   Requesting administrator access...
    echo ========================================
    echo.
    "%POWERSHELL%" -NoProfile -ExecutionPolicy Bypass -Command "Start-Process -FilePath '%~f0' -ArgumentList '__elevated__' -Verb RunAs"
    if errorlevel 1 (
        echo [tests] Failed to request administrator access.
        echo.
        pause
        exit /b 1
    )
    exit /b 0
)

echo ========================================
echo   Running backend tests...
echo ========================================
echo.

"%POWERSHELL%" -NoProfile -ExecutionPolicy Bypass -File "%TEST_SCRIPT%"
set "EXIT_CODE=%ERRORLEVEL%"

echo.
if "%EXIT_CODE%"=="0" (
    echo [tests] Backend tests completed successfully.
) else (
    echo [tests] Backend tests failed. Exit code: %EXIT_CODE%
    echo [tests] Logs:
    echo [tests]   %TEST_STDOUT_LOG%
    echo [tests]   %TEST_STDERR_LOG%
)

echo.
pause
exit /b %EXIT_CODE%
