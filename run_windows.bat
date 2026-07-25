@echo off
setlocal
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0run_app.ps1" %*
exit /b %ERRORLEVEL%
