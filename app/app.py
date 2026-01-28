import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import akshare as ak
from datetime import datetime, timedelta
import time
import random
import os
import concurrent.futures
import threading
import sys
import io
import zipfile
import shutil
import json
import logging
import html

# 尝试导入 Streamlit 上下文管理器，用于解决多线程 "missing ScriptRunContext" 警告
try:
    from streamlit.runtime.scriptrunner import add_script_run_ctx, get_script_run_ctx
except ImportError:
    # 兼容旧版本 Streamlit
    from streamlit.scriptrunner import add_script_run_ctx, get_script_run_ctx

# 配置页面信息
st.set_page_config(
    page_title="A股历史盘面回放系统",
    page_icon="⏪",
    layout="wide"
)

# -----------------------------------------------------------------------------
from core.data_access import *
from core.data_access import (
    _get_cached_codes_for_date,
    _get_daily_codes,
    _refresh_name_map_for_codes,
    _scan_cached_dates,
    _start_auto_prefetch_if_needed,
    _start_manual_prefetch,
)
from ui.history_view import render_history_view
from ui.data_manager_view import render_data_manager
from ui.divergence_view import render_divergence_view
from ui.stock_analysis_view import render_stock_analysis_view
from ui.market_overview import render_market_overview  # 新增

NAV_OVERVIEW = "🔍 市场概览 (Market Overview)"
NAV_HISTORY = "⏪ 历史盘面回放"
NAV_STOCK = "📈 个股多日分析"
NAV_DIVERGENCE = "🌊 资金偏离分析"
NAV_MANAGER = "📂 数据管理"

# 2. UI 布局
# -----------------------------------------------------------------------------

st.title("A股历史盘面回放系统 (多指数分层版)")

st.markdown("""
> 🕹️ **操作指南**：
> 1. 等待数据初始化完成（初次运行可能需要 2-3 分钟）。
> 2. 拖动下方滑块选择历史日期。
> 3. 观察当日盘面的资金流向与热度。
""")

# 侧边栏
with st.sidebar:
    st.header("⚙️ 数据管理")

    # 指数池选择 (优先显示，以便加载数据)
    target_pool = st.selectbox(
        "🎯 分析对象池", 
        ["000300 | 沪深300 (大盘)", "000905 | 中证500 (中盘, 剔除300)", "000852 | 中证1000 (小盘, 剔除300+500)"],
        index=0,
        help="切换不同的股票池进行分层分析。注意：切换后需要重新拉取数据，可能需要一点时间。"
    )
    # Extract code
    pool_code = target_pool.split(" | ")[0]
    st.session_state["index_pool_code"] = pool_code
    
    with st.expander("数据刷新与维护", expanded=True):
        st.write("如果数据显示不正确，请尝试以下操作：")
        
        # 1. ????
        if st.button("🟢 刷新今日行情 (盘中)"):
            log_action("刷新今日行情(盘中)")
            try:
                if os.path.exists(CACHE_FILE):
                    _df = pd.read_parquet(CACHE_FILE)
                    _today = datetime.now().date()
                    _df_new = _df[_df["日期"].dt.date < _today]
                    _df_new.to_parquet(CACHE_FILE)
                    st.toast("已清除今日缓存，正在重新拉取实时数据...")
                st.cache_data.clear()
                st.rerun()
            except Exception as e:
                st.error(f"操作失败: {e}")

        # 2. ??????????
        if st.button("🧹 清空分时图内存缓存"):
            log_action("清空分时图内存缓存")
            st.cache_data.clear()
            st.toast("✅ 内存缓存已清空，磁盘缓存保留。")

        # 3. ????????
        if st.button("🔄 手动更新股票名称"):
            codes_hint = st.session_state.get("last_top_codes", [])
            log_action("手动更新股票名称", codes=len(codes_hint))
            name_map = _refresh_name_map_for_codes(codes_hint, force=True)
            if name_map:
                st.toast(f"✅ 已更新名称映射：{len(name_map)} 条")
            else:
                st.warning("未获取到最新名称映射。")

        # 4. ????????
        if st.button("🗑️ 删除本地分时缓存"):
            log_action("删除本地分时缓存")
            clear_min_cache()
            st.cache_data.clear()
            st.toast("✅ 本地分时缓存已删除。")

        # 5. ????
        if st.button("🚨 彻底重置 (删除所有)"):
            log_action("彻底重置")
            if os.path.exists(CACHE_FILE):
                os.remove(CACHE_FILE)
                st.toast("已删除历史日线缓存。")
            clear_min_cache()
            st.cache_data.clear()
            st.rerun()

    with st.expander("💾 数据备份与恢复", expanded=False):
        st.caption("备份 data 目录（历史日线 + 分时缓存）")
        if st.button("📦 生成备份", key="backup_build"):
            log_action("生成备份")
            data_bytes = build_data_backup_zip()
            if data_bytes:
                st.session_state["backup_zip"] = data_bytes
                st.session_state["backup_name"] = f"capmap_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"
                st.toast("✅ 备份已生成")
            else:
                st.warning("没有可备份的数据。")
        if "backup_zip" in st.session_state:
            download_clicked = st.download_button(
                "⬇️ 下载备份",
                data=st.session_state["backup_zip"],
                file_name=st.session_state.get("backup_name", "capmap_data_backup.zip"),
                mime="application/zip",
                key="backup_download",
            )
            if download_clicked:
                log_action("下载备份")
        uploaded = st.file_uploader("恢复备份（.zip）", type=["zip"], key="backup_upload")
        if uploaded is not None and st.button("♻️ 恢复备份", key="backup_restore"):
            log_action("恢复备份", file=getattr(uploaded, "name", ""))
            try:
                restored = restore_data_backup(uploaded)
                st.cache_data.clear()
                log_action("恢复备份完成", files=restored)
                st.toast(f"✅ 已恢复 {restored} 个文件")
                st.rerun()
            except Exception as e:
                st.error(f"恢复失败: {e}")
    st.info("数据源：沪深300成分股 (Biying)")
    st.caption("注：方块大小使用'成交额'代替'市值'，\n反映当日交易热度。")

    st.markdown("---")
    st.markdown("### 🛠️ 板块过滤")
    filter_cyb = st.checkbox("屏蔽创业板 (300开头)", value=False)
    # 默认勾选屏蔽科创板，因为其波动较大，可能影响整体热力图观感
    filter_kcb = st.checkbox("屏蔽科创板 (688开头)", value=True)
    filter_state = (filter_cyb, filter_kcb)
    if st.session_state.get("filter_state") != filter_state:
        st.session_state["filter_state"] = filter_state
        log_action("筛选条件变更", cyb=filter_cyb, kcb=filter_kcb)
    
# 加载数据
with st.spinner("正在初始化历史数据仓库..."):
    pool_code = st.session_state.get("index_pool_code", "000300")
    origin_df = fetch_history_data(pool_code)
    _start_auto_prefetch_if_needed(origin_df)

# --- 后台任务检测与控制 ---
# 检查是否有名为 "PrefetchWorker" 的后台线程
bg_thread = None
for t in threading.enumerate():
    if t.name == "PrefetchWorker":
        bg_thread = t
        break

# 更新 Sidebar UI
with st.sidebar:
    st.markdown("---")
    
    # 导航栏
    nav_option = st.radio("📡 功能导航", [NAV_OVERVIEW, NAV_HISTORY, NAV_STOCK, NAV_DIVERGENCE, NAV_MANAGER], index=0)
    prev_nav = st.session_state.get("nav_option_prev")
    if prev_nav != nav_option:
        st.session_state["nav_option_prev"] = nav_option
        log_action("功能导航切换", nav=nav_option)
    
    # (指数池选择已移至顶部)

    with st.expander("📥 后台数据预取", expanded=False):
        st.caption("后台静默下载过去分时数据 (5分钟K线)")
        st.info("💡 使用商用接口时，可以设置较大的预取天数以覆盖所有历史。推荐设置为 3650 (10年) 以初始化全量分时数据。")
        prefetch_days = st.number_input("预取天数", min_value=5, max_value=5000, value=365, step=10)
        
        if bg_thread and bg_thread.is_alive():
            st.info(f"🟢 后台任务运行中...\n请关注控制台(Console)日志")
            # 无法通过 Button 停止线程，除非使用 Event。暂不实现停止。
        else:
            if st.button("🚀 启动后台下载"):
                log_action("启动后台预取", days=prefetch_days)
                if not origin_df.empty:
                    # 获取日期列表
                    all_dates = sorted(origin_df['日期'].dt.date.unique())
                    target_prefetch_dates = all_dates[-prefetch_days:]
                    
                    # 启动线程
                    t = threading.Thread(
                        target=background_prefetch_task,
                        args=(target_prefetch_dates, origin_df),
                        name="PrefetchWorker",
                        daemon=True
                    )
                    add_script_run_ctx(t)
                    t.start()
                    st.rerun()
                else:
                    st.error("历史数据尚未就绪")

if not origin_df.empty:
    # --- 全局过滤逻辑 ---
    df = origin_df.copy()
    if filter_cyb:
        df = df[~df['代码'].astype(str).str.startswith('300')]
    if filter_kcb:
        df = df[~df['代码'].astype(str).str.startswith('688')]

    if df.empty:
        st.warning("过滤后没有剩余股票数据，请取消勾选过滤选项。")
        st.stop()

    # --- 时间选择器逻辑 (Session State 管理) ---
    available_dates = sorted(df['日期'].dt.date.unique())
    
    if nav_option == NAV_OVERVIEW:
        render_market_overview(origin_df)

    elif nav_option == NAV_HISTORY:
        render_history_view(df, available_dates)
    
    elif nav_option == NAV_STOCK:
        render_stock_analysis_view(origin_df)

    elif nav_option == NAV_MANAGER:
        render_data_manager(origin_df)

    elif nav_option == NAV_DIVERGENCE:
        render_divergence_view(df, available_dates)

else:
    st.warning("⚠️ 未获取到任何历史数据。可能原因：")
    st.markdown("""
    1. **网络连接问题**：无法连接到 AkShare 或 必盈 API。
    2. **接口限制**：数据源接口可能暂时不可用。
    3. **初始化中断**：如果是首次运行，请检查控制台日志是否有报错。
    """)
    if st.button("🔄 重试全量初始化"):
        st.cache_data.clear()
        if os.path.exists("data/csi300_history_cache.parquet"):
            os.remove("data/csi300_history_cache.parquet")
        st.rerun()
