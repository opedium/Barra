@echo off
chcp 65001 >nul
title 抖音直播采集启动器
color 0A
echo ========================================
echo.
echo     正在启动数据采集，请勿关闭此窗口...
echo.
echo ========================================
python main.py
pause