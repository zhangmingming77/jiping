@echo off
chcp 65001 >nul
title 迹评 - 本地实训平台（作者：张昊 信安252）
cd /d "C:\Users\zhang\Doubao\chats\2026-10-28\new-chat-1"
echo ============================================
echo   迹评 EVERY STEP COUNTS
echo   正在启动，请稍候... 浏览器会自动打开
echo   关闭此窗口即停止服务
echo ============================================
"C:\Users\zhang\AppData\Local\Doubao\User Data\sandbox_runtime\bases\772984f6fd3a8ae0ae7d2422cd7c2bfa\python\python.exe" -m streamlit run 迹评_app.py --server.port 8502
pause