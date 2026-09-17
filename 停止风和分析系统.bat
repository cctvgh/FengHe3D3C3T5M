@echo off
for /f "tokens=5" %%a in ('netstat -ano ^| findstr ":8021" ^| findstr "LISTENING"') do taskkill /PID %%a /T /F
echo ·þÎñÒÑÍ£Ö¹
pause