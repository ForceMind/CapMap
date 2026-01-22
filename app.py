import streamlit as st
import pandas as pd
from datetime import datetime
import os
import threading

from modules.config import STOCK_POOLS
from modules.data_loader import fetch_history_data, fetch_intraday_data_v2, background_prefetch_task, build_fetch_plan
from modules.analysis import calculate_deviation_data, filter_deviation_data
from modules.visualization import plot_market_heatmap, plot_deviation_scatter, plot_intraday_charts
from modules.utils import add_script_run_ctx

st.set_page_config(
    page_title="A股资金全景分析",
    page_icon="⏪",
    layout="wide"
)

with st.sidebar:
    st.header("⚙️ 核心设置")

    selected_pool = st.selectbox(
        "🎯 目标指数池",
        list(STOCK_POOLS.keys()),
        index=0,
        key="sb_selected_pool"
    )

    st.markdown("---")
    st.header("🔧 数据管理")

    with st.expander("数据刷新与维护", expanded=True):
        st.write(f"当前管理对象: **{selected_pool}**")

        if st.button("🟢 刷新今日行情 (盘中)"):
            try:
                p_cfg = STOCK_POOLS[selected_pool]
                c_path = p_cfg["cache"]

                if os.path.exists(c_path):
                    _df = pd.read_parquet(c_path)
                    if not _df.empty:
                        _df['日期'] = pd.to_datetime(_df['日期'])
                    _today = datetime.now().date()
                    _df_new = _df[_df['日期'].dt.date < _today]
                    _df_new.to_parquet(c_path)
                    st.toast(f"已清除 [{selected_pool}] 今日缓存，正在重新同步...")
                else:
                    st.toast(f"[{selected_pool}] 暂无本地缓存，直接刷新...")

                st.cache_data.clear()
                st.rerun()
            except Exception as e:
                st.error(f"操作失败: {e}")

        if st.button("🧹 清空分时图缓存"):
            st.cache_data.clear()
            st.toast("✅ 所有内存缓存已清空，下次查看分时图将重新下载。")

        if st.button(f"🚨 重置 [{selected_pool}] 历史数据"):
            p_cfg = STOCK_POOLS[selected_pool]
            c_path = p_cfg["cache"]
            if os.path.exists(c_path):
                os.remove(c_path)
                st.toast(f"已删除 [{selected_pool}] 本地历史文件。")
            st.cache_data.clear()
            st.rerun()

        if st.checkbox("显示高级选项 (全局重置)"):
            if st.button("💣 毁灭吧赶紧的 (删除所有池数据)"):
                for p_name, p_val in STOCK_POOLS.items():
                    if os.path.exists(p_val["cache"]):
                        os.remove(p_val["cache"])
                st.cache_data.clear()
                st.rerun()

    st.markdown("---")
    st.markdown("### 🛠️ 板块过滤")
    filter_cyb = st.checkbox("屏蔽创业板 (300开头)", value=False)
    filter_kcb = st.checkbox("????? (688??)", value=True)

    st.markdown("---")
    nav_option = st.radio(
        "🧭 功能导航",
        ["📊 盘面回放", "🌊 资金偏离分析"],
        index=0
    )

    st.markdown("---")
    st.header("⏯️ 拉取控制")
    auto_fetch = st.checkbox("自动拉取历史数据", value=True)
    max_workers = st.slider("并发线程数", min_value=1, max_value=20, value=10)
    request_delay = st.slider("请求间隔(秒)", min_value=0.0, max_value=1.0, value=0.0, step=0.05)
    fetch_spot = st.checkbox("盘中补全(Spot)", value=True)
    manual_fetch = False
    if not auto_fetch:
        manual_fetch = st.button("开始拉取/刷新")

allow_download = auto_fetch or manual_fetch
confirm_key = f"fetch_confirmed_{selected_pool}"

if allow_download:
    plan = build_fetch_plan(selected_pool, max_workers, request_delay, fetch_spot)
    if plan["needs_update"]:
        if not st.session_state.get(confirm_key):
            if plan["est_seconds"]:
                est_text = f"{int(plan['est_seconds'])} 秒(估算)"
            else:
                est_text = "未知"
            plan_lines = [
                f"指数池: {plan['pool_name']} (代码 {plan['index_code']})",
                f"缓存文件: {plan['cache_file']}",
                f"已有缓存: {'是' if plan['has_cache'] else '否'} | 记录数 {plan['cached_rows']}",
                f"拉取区间: {plan['start_date_str']} - {plan['end_date_str']}",
                "接口说明:",
                "- index_stock_cons: 指数成分股列表",
                "- stock_zh_a_hist: 个股日K历史（主请求，易限频）",
                f"- stock_zh_a_spot_em: ???????{('?????' if plan['fetch_spot'] else '?????')}",
                "- 分时接口(如勾选分时图): stock_zh_a_hist_min_em / index_zh_a_hist_min_em",
                f"股票数量: {plan['total_stocks'] if plan['total_stocks'] is not None else '未知'}",
                f"线程数: {plan['max_workers']} | 请求间隔: {plan['request_delay']} 秒",
                f"预计耗时: {est_text}"
            ]
            st.warning("即将拉取数据，请确认是否继续：\n\n" + "\n".join([f"- {line}" for line in plan_lines]))
            if st.button("继续拉取"):
                st.session_state[confirm_key] = True
                st.rerun()
            st.stop()
    else:
        allow_download = False

with st.spinner(f"正在初始化 [{selected_pool}] 历史数据仓库..."):
    origin_df = fetch_history_data(
        selected_pool,
        allow_download=allow_download,
        max_workers=max_workers,
        request_delay=request_delay,
        fetch_spot=fetch_spot
    )

if confirm_key in st.session_state:
    st.session_state[confirm_key] = False

# --- 后台任务检测与控制 ---
bg_thread = None
for t in threading.enumerate():
    if t.name == "PrefetchWorker":
        bg_thread = t
        break

with st.sidebar:
    st.markdown("---")
    with st.expander("📥 后台数据预取", expanded=False):
        st.caption("后台静默下载最近 N 天分时数据")
        prefetch_days = st.number_input("预取天数", min_value=5, max_value=200, value=30, step=10)

        if bg_thread and bg_thread.is_alive():
            st.info("🟢 后台任务运行中...\n请关注控制台日志")
        else:
            if st.button("🚀 启动后台下载"):
                if not origin_df.empty:
                    all_dates = sorted(origin_df['日期'].dt.date.unique())
                    target_prefetch_dates = all_dates[-prefetch_days:]

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

if origin_df.empty:
    st.error("数据加载失败，请刷新重试。")
    st.stop()

# 全局过滤
filtered_df = origin_df.copy()
if filter_cyb:
    filtered_df = filtered_df[~filtered_df['代码'].astype(str).str.startswith('300')]
if filter_kcb:
    filtered_df = filtered_df[~filtered_df['代码'].astype(str).str.startswith('688')]

if filtered_df.empty:
    st.warning("过滤后没有剩余股票数据，请取消勾选过滤选项。")
    st.stop()

if nav_option == "📊 盘面回放":
    st.title(f"A股资金全景分析 - {selected_pool}")
    st.markdown(
        "> 🕹️ **操作指南**：\n"
        "> 1. 等待数据初始化完成（初次运行可能需要 2-3 分钟）。\n"
        "> 2. 拖动下方滑块选择历史日期。\n"
        "> 3. 观察当日盘面的资金流向与热度。"
    )

    available_dates = sorted(filtered_df['日期'].dt.date.unique())

    if 'selected_date_idx' not in st.session_state:
        st.session_state.selected_date_idx = len(available_dates) - 1

    if st.session_state.selected_date_idx >= len(available_dates):
        st.session_state.selected_date_idx = len(available_dates) - 1
    if st.session_state.selected_date_idx < 0:
        st.session_state.selected_date_idx = 0

    st.markdown("### 📅 选择回放日期")

    mode_col1, mode_col2 = st.columns([1, 3])
    with mode_col1:
        playback_mode = st.radio("回放模式", ["单日复盘", "多日走势拼接"], horizontal=True)

    if playback_mode == "单日复盘":
        col_prev, col_slider, col_next = st.columns([1, 6, 1])

        with col_prev:
            st.write("")
            st.write("")
            if st.button("⬅️ 前一天"):
                if st.session_state.selected_date_idx > 0:
                    st.session_state.selected_date_idx -= 1
                    st.rerun()

        with col_next:
            st.write("")
            st.write("")
            if st.button("后一天 ➡️"):
                if st.session_state.selected_date_idx < len(available_dates) - 1:
                    st.session_state.selected_date_idx += 1
                    st.rerun()

        with col_slider:
            current_date_val = available_dates[st.session_state.selected_date_idx]
            picked_date = st.date_input(
                "日期",
                value=current_date_val,
                min_value=available_dates[0],
                max_value=available_dates[-1],
                label_visibility="collapsed"
            )

            if picked_date != current_date_val:
                if picked_date in available_dates:
                    st.session_state.selected_date_idx = available_dates.index(picked_date)
                else:
                    closest_date = min(available_dates, key=lambda d: abs(d - picked_date))
                    st.session_state.selected_date_idx = available_dates.index(closest_date)
                    st.toast(f"📅 休市日，已自动定位到最近交易日: {closest_date}")
                st.rerun()

        target_dates = [available_dates[st.session_state.selected_date_idx]]
        selected_date = target_dates[0]
    if "last_selected_date" not in st.session_state:
        st.session_state.last_selected_date = selected_date
    if st.session_state.last_selected_date != selected_date:
        st.session_state["show_intraday"] = False
        st.session_state.last_selected_date = selected_date

    else:
        with mode_col2:
            date_range = st.date_input(
                "选择时间范围 (建议不超过5天，否则加载较慢)",
                value=[available_dates[-5] if len(available_dates) > 5 else available_dates[0], available_dates[-1]],
                min_value=available_dates[0],
                max_value=available_dates[-1]
            )

        if len(date_range) == 2:
            start_d, end_d = date_range
            target_dates = [d for d in available_dates if start_d <= d <= end_d]
            if not target_dates:
                st.warning("⚠️ 选定范围内无交易数据，已自动重置为最近交易日")
                target_dates = [available_dates[-1]]
            st.info(f"已选择 {len(target_dates)} 个交易日进行拼接展示")
            selected_date = target_dates[-1]
        else:
            st.warning("请选择完整的开始和结束日期")
            target_dates = [available_dates[-1]]
            selected_date = available_dates[-1]

    daily_df = filtered_df[filtered_df['日期'].dt.date == selected_date].copy()

    if daily_df.empty:
        st.warning(f"{selected_date} 当日无交易数据（可能是非交易日或数据缺失）。")
    else:
        median_chg = daily_df['涨跌幅'].median()
        total_turnover = daily_df['成交额'].sum() / 1e8
        top_gainer = daily_df.loc[daily_df['涨跌幅'].idxmax()]

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("当前回放日期", selected_date.strftime("%Y-%m-%d"))
        col2.metric("成分股中位数涨跌", f"{median_chg:.2f}%", delta=f"{median_chg:.2f}%", delta_color="normal")
        col3.metric("成分股总成交", f"{total_turnover:.1f} 亿")
        col4.metric("领涨龙头", f"{top_gainer['名称']} ({top_gainer['涨跌幅']:.2f}%)")

        st.markdown("---")
        st.subheader("📈 核心资产分时走势叠加")

        col_mode, col_num = st.columns([3, 1])
        with col_mode:
            chart_mode = st.radio("选股模式", ["成交额 Top (活跃度)", "指数贡献 Top (影响大盘)"], horizontal=True)
        with col_num:
            top_n = st.number_input(
                "标的数量",
                min_value=5,
                max_value=50,
                value=20,
                step=5,
                help="沪/深各取 N 个标的（即总数为 2N）",
                key="top_n_stocks_input"
            )

        st.caption(f"注：这里的排名是基于 **{selected_date}** 当日的数据计算的。如果是多日模式，则展示这些股票在过去几天的走势。")
        st.caption("注：指数贡献 = 涨跌幅 × 权重(近似为成交额/市值占比)。此模式能看到是谁在拉动或砸盘。")

        show_intraday = st.checkbox("加载分时走势 (需从网络实时拉取)", value=False)

        if show_intraday:
            progress_area = st.empty()

            if "成交额" in chart_mode:
                sort_col = '成交额'
            else:
                daily_df['abs_impact'] = (daily_df['涨跌幅'] * daily_df['成交额']).abs()
                sort_col = 'abs_impact'

            sh_pool = daily_df[daily_df['代码'].astype(str).str.startswith('6')].copy()
            sz_pool = daily_df[~daily_df['代码'].astype(str).str.startswith('6')].copy()

            sh_top = sh_pool.sort_values(sort_col, ascending=False).head(top_n)
            sz_top = sz_pool.sort_values(sort_col, ascending=False).head(top_n)

            top_stocks_df = pd.concat([sh_top, sz_top], ignore_index=True)

            target_stocks_list = []
            for _, row in top_stocks_df.iterrows():
                target_stocks_list.append((row['代码'], row['名称'], row['成交额']))

            all_intraday_data = []
            period_to_use = '1'

            if len(target_dates) > 5 and playback_mode == "多日走势拼接":
                if len(target_dates) > 30:
                    period_to_use = '15'
                    st.info(f"ℹ️ 您选择了 {len(target_dates)} 天：系统自动切换至【15分钟级】数据。")
                else:
                    period_to_use = '5'
                    st.info(f"ℹ️ 您选择了 {len(target_dates)} 天：系统自动切换至【5分钟级】数据。")
            elif len(target_dates) > 10:
                st.toast(f"⚠️ 您选择了 {len(target_dates)} 天的数据，加载可能较慢，请耐心等待...")

            target_dates_to_fetch = target_dates
            total_steps = len(target_dates_to_fetch)

            status_text = st.empty()
            fetch_progress = st.progress(0)

            for i, d_date in enumerate(target_dates_to_fetch):
                status_text.text(f"🔄 正在获取: {d_date.strftime('%Y-%m-%d')} ({i+1}/{total_steps})...")
                fetch_progress.progress((i + 1) / total_steps)

                d_str = d_date.strftime("%Y-%m-%d")
                day_results = fetch_intraday_data_v2(
                    target_stocks_list,
                    d_str,
                    period=period_to_use,
                    max_workers=max_workers,
                    request_delay=request_delay
                )

                for res in day_results:
                    res['data']['date_col'] = d_str
                    res['real_date'] = d_date

                all_intraday_data.extend(day_results)

            status_text.empty()
            fetch_progress.empty()
            progress_area.empty()

            if not all_intraday_data:
                st.warning("未能获取到分时数据")
            else:
                valid_dates = set()
                for item in all_intraday_data:
                    if 'real_date' in item:
                        valid_dates.add(item['real_date'].strftime("%Y-%m-%d"))

                days_list = sorted(list(valid_dates))
                if not days_list:
                    days_list = sorted(list(set([x.strftime("%Y-%m-%d") for x in target_dates_to_fetch])))

                fig_sh, fig_sz = plot_intraday_charts(all_intraday_data, days_list, daily_df, chart_mode)

                tab1, tab2 = st.tabs(["沪市 (SH)", "深市 (SZ)"])
                with tab1:
                    if fig_sh:
                        st.plotly_chart(fig_sh, use_container_width=True)
                with tab2:
                    if fig_sz:
                        st.plotly_chart(fig_sz, use_container_width=True)

        st.subheader(f"📊 {selected_date.strftime('%Y年%m月%d日')} 市场全景热力图")
        fig = plot_market_heatmap(daily_df)
        st.plotly_chart(fig, use_container_width=True)

        with st.expander("查看当日详细数据"):
            st.dataframe(
                daily_df[['代码', '名称', '收盘', '涨跌幅', '成交额']].style.format({
                    '收盘': '{:.2f}',
                    '涨跌幅': '{:.2f}%',
                    '成交额': '{:,.0f}'
                }),
                hide_index=True
            )

elif nav_option == "🌊 资金偏离分析":
    st.subheader("🌊 资金偏离度分析 (Alpha Divergence)")
    st.info("💡 **逻辑说明**：计算选定周期内每只股票相对于【市场中位数】的超额涨跌幅（偏离度）。\n\n如果某只股票 **成交额巨大** 且 **向下偏离极大**，通常意味着主力资金在大举出货；反之则是主力抢筹。")

    available_dates = sorted(filtered_df['日期'].dt.date.unique())
    col_d1, col_d2 = st.columns(2)
    with col_d1:
        date_range_div = st.date_input(
            "分析周期",
            value=[available_dates[-5] if len(available_dates) > 5 else available_dates[0], available_dates[-1]],
            min_value=available_dates[0],
            max_value=available_dates[-1],
            key="divergence_date_input"
        )

    target_dates_div = []
    if len(date_range_div) == 2:
        s_d, e_d = date_range_div
        target_dates_div = [d for d in available_dates if s_d <= d <= e_d]

    if not target_dates_div:
        st.warning("请选择有效的时间范围")
        st.stop()

    st.caption(f"已选取 {target_dates_div[0]} 至 {target_dates_div[-1]}，共 {len(target_dates_div)} 个交易日。")

    div_df, market_median_chg = calculate_deviation_data(filtered_df, target_dates_div)

    if div_df.empty:
        st.stop()

    st.markdown("### 🔎 策略筛选")
    strategy_mode = st.radio(
        "选择筛选策略",
        ["默认 (全部展示)", "🛡️ 护盘/控盘 (逆势大票)", "🔥 游资/活跃 (高换手/高波)", "☠️ 出货/砸盘 (放量下跌)"],
        horizontal=True
    )

    filtered_div = filter_deviation_data(div_df, strategy_mode=strategy_mode)

    col_m1, col_m2 = st.columns(2)
    col_m1.metric("基准(中位数)涨跌幅", f"{market_median_chg:.2f}%")
    col_m2.metric("当前策略筛选数量", f"{len(filtered_div)} 只")

    st.divider()

    if not filtered_div.empty:
        fig_scatter = plot_deviation_scatter(filtered_div, strategy_mode)
        if fig_scatter:
            st.plotly_chart(fig_scatter, use_container_width=True)
        else:
            st.warning("当前策略下无符合条件的标的。")
    else:
        st.warning("当前策略下无符合条件的标的。")

    col_list1, col_list2 = st.columns(2)

    with col_list1:
        st.subheader("🔥 资金抱团 (放量向上偏离)")
        buy_df = filtered_div[filtered_div['偏离度'] > 0].sort_values('区间总成交', ascending=False).head(20)
        st.dataframe(
            buy_df[['代码', '名称', '偏离度', '成交额(亿)', '区间涨跌幅']].style.format({
                '偏离度': '+{:.2f}%',
                '成交额(亿)': '{:.1f}',
                '区间涨跌幅': '{:.2f}%'
            }),
            hide_index=True
        )

    with col_list2:
        st.subheader("📉 资金出逃 (放量向下偏离)")
        sell_df = filtered_div[filtered_div['偏离度'] < 0].sort_values('区间总成交', ascending=False).head(20)
        st.dataframe(
            sell_df[['代码', '名称', '偏离度', '成交额(亿)', '区间涨跌幅']].style.format({
                '偏离度': '{:.2f}%',
                '成交额(亿)': '{:.1f}',
                '区间涨跌幅': '{:.2f}%'
            }),
            hide_index=True
        )
