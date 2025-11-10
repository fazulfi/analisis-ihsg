@echo off
SET REPO_DIR=C:\Users\igors\analisis-ihsg
SET VENV_PY=%REPO_DIR%\.venv\Scripts\python.exe
SET LOG_DIR=%REPO_DIR%\logs
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
cd /d "%REPO_DIR%"
set PYTHONPATH=%REPO_DIR%
for /f "tokens=1-3 delims=/: " %%a in ("%date%") do set D=%%c-%%b-%%a
for /f "tokens=1-2 delims=:." %%a in ("%time%") do set T=%%a%%b
"%VENV_PY%" -m runner.check_and_dispatch >> "%LOG_DIR%\dispatch_%D%_%T%.log" 2>&1
EXIT /B %ERRORLEVEL%
