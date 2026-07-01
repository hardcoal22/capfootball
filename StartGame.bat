@echo off

:: Kill any old server on port 2567
for /f "tokens=5" %%a in ('netstat -aon 2^>nul ^| find ":2567 " ^| find "LISTENING"') do (
    taskkill /f /pid %%a >nul 2>&1
)
timeout /t 1 /nobreak >nul

:: cd into server dir so node inherits it, then return
cd /d "%~dp0server"
start "Slime Soccer Server" /min node index.js
cd /d "%~dp0"

:: Wait for server to be ready
timeout /t 3 /nobreak >nul

:: Open game in browser (no quotes around URL so Windows treats it as a URL)
start http://localhost:2567

exit
