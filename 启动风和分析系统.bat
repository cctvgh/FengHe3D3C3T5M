@echo off
chcp 65001 >nul
cd /d D:\FengHe3D3C3T5M
start "FengHe3D3C3T5M" /min "D:\StockMasters\venv\Scripts\python.exe" server.py
echo 风和投资3D3C3T5M分析系统已启动，浏览器即将打开...
timeout /t 3 >nul
start http://localhost:8021
exit