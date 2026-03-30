@echo off
setlocal

cd /d "%~dp0"
title 亢龙监控 - 重启服务

echo ========================================
echo  正在重启亢龙监控服务...
echo ========================================
echo.

call "%~dp0scripts\restart_monitor_service.bat"
set "EXIT_CODE=%ERRORLEVEL%"

echo.
if "%EXIT_CODE%"=="0" (
    echo 服务重启完成。
    echo 访问地址: http://127.0.0.1:8010/
) else (
    echo 服务重启失败，错误码: %EXIT_CODE%
)

echo.
pause
exit /b %EXIT_CODE%
