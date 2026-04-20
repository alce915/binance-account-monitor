@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
set "PS_SCRIPT=%SCRIPT_DIR%scripts\run_secret_migration.ps1"

if not exist "%PS_SCRIPT%" (
  echo [secrets] Missing script: %PS_SCRIPT%
  exit /b 1
)

echo [secrets] Requesting administrator access...
powershell -NoProfile -ExecutionPolicy Bypass -File "%PS_SCRIPT%"
set "EXIT_CODE=%ERRORLEVEL%"

if "%EXIT_CODE%"=="0" (
  echo [secrets] Local secret migration completed.
  exit /b 0
)

echo [secrets] Local secret migration failed. Exit code: %EXIT_CODE%
echo [secrets] Logs:
echo [secrets]   %SCRIPT_DIR%data\runtime\secret_migration.stdout.log
echo [secrets]   %SCRIPT_DIR%data\runtime\secret_migration.stderr.log
exit /b %EXIT_CODE%
