@echo off
chcp 65001 >nul
title 音浪精准去重工具
color 0B
echo ========================================
echo.
echo     正在启动精准去重，请按照提示拖入文件...
echo.
echo ========================================
python deduplicate_gifts.py
pause