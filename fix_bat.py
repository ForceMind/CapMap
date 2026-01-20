# fix_bat.py
content = r"""@echo off
chcp 65001 >nul
title A股历史盘面回放系统启动脚本
echo ========================================================
echo        🚀 正在启动 A股历史盘面回放系统 (沪深300版)
echo ========================================================
echo.
echo [提示]
echo 1. 请确保您已经安装了 Python 和必要的库。
echo    如果没有安装，请运行 pip install -r requirements.txt
echo 2. 初次运行时，程序需要下载 300 只股票的历史数据。
echo    这一过程可能需要数分钟，请耐心等待界面进度条跑完。
echo.
echo 正在唤醒 Streamlit...
echo.

cd /d "%~dp0"
streamlit run app.py

if %errorlevel% neq 0 (
    echo.
    echo [错误] 启动失败。请检查是否安装了 Streamlit。
    echo 您可以尝试运行: pip install streamlit pandas plotly akshare
    echo.
    pause
)
"""

# 使用 UTF-8 编码写入，不带 BOM，确保换行符统一为 Windows 格式
with open("start_fixed.bat", "w", encoding="utf-8") as f:
    f.write(content)

print("✅ 已生成纯净版启动脚本：start_fixed.bat")