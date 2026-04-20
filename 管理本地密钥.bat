@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
set "PS_SCRIPT=%SCRIPT_DIR%scripts\manage_local_secrets.ps1"

if not exist "%PS_SCRIPT%" (
  echo [secrets] Missing script: %PS_SCRIPT%
  exit /b 1
)

echo [secrets] Requesting administrator access...
powershell -NoProfile -ExecutionPolicy Bypass -File "%PS_SCRIPT%" %*
set "EXIT_CODE=%ERRORLEVEL%"

if "%EXIT_CODE%"=="0" (
  echo [secrets] Local secret command completed.
  exit /b 0
)

echo [secrets] Local secret command failed. Exit code: %EXIT_CODE%
echo [secrets] Logs:
echo [secrets]   %SCRIPT_DIR%data\runtime\manage_secrets.stdout.log
echo [secrets]   %SCRIPT_DIR%data\runtime\manage_secrets.stderr.log
exit /b %EXIT_CODE%
