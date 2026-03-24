import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import time

from core.data_access import fetch_intraday_data_v2, log_action, logger, _refresh_name_map_for_codes


def render_history_view(df, available_dates):
    if 'selected_date_idx' not in st.session_state:
        st.session_state.selected_date_idx = len(available_dates) - 1

    #确保索引不越界
    if st.session_state.selected_date_idx >= len(available_dates):
        st.session_state.selected_date_idx = len(available_dates) - 1
    if st.session_state.selected_date_idx < 0:
        st.session_state.selected_date_idx = 0

    # 布局：前一天 | 滑块 | 后一天
    st.markdown("### 📅 选择回放日期")
    
    # 模式选择
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
            # 原 select_slider 替换为 date_input 以支持快速年份选择
            current_date_val = available_dates[st.session_state.selected_date_idx]
            
            # Allow picking today to show hint
            max_d = available_dates[-1]
            try:
                today = pd.Timestamp.now().date()
                if today > max_d: max_d = today
            except: pass

            picked_date = st.date_input(
                "日期",
                value=current_date_val,
                min_value=available_dates[0],
                max_value=max_d,
                label_visibility="collapsed"
            )
            
            # 如果日期发生变化
            if picked_date != current_date_val:
                if picked_date in available_dates:
                    st.session_state.selected_date_idx = available_dates.index(picked_date)
                else:
                    # Enhance Logic: Provide specific hint for Today
                    if picked_date > available_dates[-1]:
                         st.toast(f"⚠️ {picked_date} 暂无数据。如需查看今日盘中行情，请点击侧边栏【刷新今日行情】。")
                    else:
                         st.toast("该日期无本地数据，已自动定位到最近交易日。如需补齐，请到“数据管理 -> 单日修复”。")

                    # Revert to valid date to prevent crash
                    closest_date = min(available_dates, key=lambda d: abs(d - picked_date))
                    st.session_state.selected_date_idx = available_dates.index(closest_date)
                    
                st.rerun()
        
        target_dates = [available_dates[st.session_state.selected_date_idx]] # 使用 state 中的日期
        selected_date = target_dates[0]
        display_date_str = selected_date.strftime("%Y-%m-%d")
        
    else: # 多日走势拼接
        with mode_col2:
            max_d = available_dates[-1]
            try:
                today = pd.Timestamp.now().date()
                if today > max_d: max_d = today
            except: pass

            date_range = st.date_input(
                "选择时间范围 (建议不超过5天，否则加载较慢)",
                value=[available_dates[-5] if len(available_dates)>5 else available_dates[0], available_dates[-1]],
                min_value=available_dates[0],
                max_value=max_d
            )
        
        if len(date_range) == 2:
            start_d, end_d = date_range
            # 筛选出范围内的交易日
            target_dates = [d for d in available_dates if start_d <= d <= end_d]
            if not target_dates: # 如果选定的范围内没有交易日 (例如全选了假期)
                if end_d > available_dates[-1]:
                    st.warning(f"⚠️ {end_d} 暂无数据。如需查看今日行情，请点击侧边栏【刷新今日行情】。")
                else:
                    st.warning("⚠️ 选定范围内无交易数据，已自动重置为最近交易日")
                target_dates = [available_dates[-1]]
            
            st.info(f"已选择 {len(target_dates)} 个交易日进行拼接展示")
            selected_date = target_dates[-1] # 用于下方显示统计面板的基准 (兼容旧代码变量名)
            display_date_str = f"{target_dates[0].strftime('%Y%m%d')} ~ {target_dates[-1].strftime('%Y%m%d')}"
        else:
            st.warning("请选择完整的开始和结束日期")
            target_dates = [available_dates[-1]]
            selected_date = available_dates[-1]
            display_date_str = selected_date.strftime("%Y-%m-%d")

    # --- 数据切片与统计 (兼容单日与多日) ---
    if len(target_dates) == 1:
        # 单日逻辑
        daily_df = df[df['日期'].dt.date == selected_date].copy()
        
        # --- 自动检测数据质量 & 修复机制 ---
        from core.data_access import refetch_daily_data, delete_daily_cache_for_date
        
        stock_count = len(daily_df)
        # CSI300 通常有 290-300 只票。如果少于 200，或者全为空，可能是有问题的缓存 (除非是早期数据)
        # 另外检测是否全是 0
        
        is_suspicious = False
        warning_msg = ""
        
        if stock_count == 0:
             # 如果选中的日期在 available_dates 里但筛选为空，这逻辑上不太可能，除非 df 变化了
             is_suspicious = True
             warning_msg = "数据为空"
        elif stock_count < 200:
             is_suspicious = True
             warning_msg = f"数据量过少 ({stock_count} 只，正常约 300 只)"
        
        if is_suspicious:
            st.warning(f"⚠️ 检测到 {display_date_str} 的数据可能不完整: {warning_msg}")
            c1, c2, c3 = st.columns([1, 1, 5])
            with c1:
                if st.button("🛠️ 尝试修复", help="重新从数据源拉取该日数据并覆盖缓存"):
                    with st.spinner("正在修复数据..."):
                        success, msg = refetch_daily_data(selected_date)
                        if success:
                            st.success(msg)
                            time.sleep(1)
                            st.cache_data.clear()
                            st.rerun()
                        else:
                            st.error(f"修复失败: {msg}")
            
            with c2:
                if st.button("🗑️ 删除今日", help="删除该日数据（下次启动时可能会重新尝试获取）"):
                     if delete_daily_cache_for_date(selected_date):
                         st.success("已删除。")
                         time.sleep(1)
                         st.cache_data.clear()
                         st.rerun()
                     else:
                         st.error("删除失败")

        if daily_df.empty:
            st.warning(f"{selected_date} 当日无交易数据。")
            st.stop()
            
        median_chg = daily_df['涨跌幅'].median()
        total_turnover = daily_df['成交额'].sum() / 1e8 
        top_gainer = daily_df.loc[daily_df['涨跌幅'].idxmax()]
        
        metric_label_date = "当前回放日期"
        metric_label_chg = "成分股中位数涨跌"
        metric_label_to = "成分股总成交"
        
    else:
        # 多日逻辑 (计算累计)
        # 1. 筛选出范围内所有数据
        start_date_ts = pd.Timestamp(target_dates[0])
        end_date_ts = pd.Timestamp(target_dates[-1])
        
        period_df = df[(df['日期'] >= start_date_ts) & (df['日期'] <= end_date_ts)].copy()
        
        if period_df.empty:
            st.stop()
            
        # 2. 计算区间累计涨跌幅
        # 方法: 对每个代码，找到首尾价格
        # 注意: 如果只用 period_df，首日的数据里的 '收盘' 是首日的收盘价。
        # 区间涨幅 = (End_Close - Start_Close) / Start_Close ? 
        # 或者更精确：Start_Close 应该是 Start_Date 的 前一日收盘价 (即 Start_Open / (1+Start_Chg))?
        # 简化起见，我们用 (End_Date Close - Start_Date Open) / Start_Date Open
        # 这样能包含 Start_Date 当天的涨跌
        
        agg_stats = []
        
        # 使用 groupby 加速
        grouped = period_df.groupby('代码')
        
        for code, group in grouped:
            group = group.sort_values('日期')
            if group.empty: continue
            
            first_row = group.iloc[0]
            last_row = group.iloc[-1]
            
            # 推算首日开盘价 = 收盘 / (1 + chg/100)
            # 这种反推如果是涨停板复权可能微小误差，但够用。
            # 也可以直接用 akshare 下载的 Open，但这里只有 Close/Chg
            # 假设 Chg 是精确的
            try:
                start_open = first_row['收盘'] / (1 + first_row['涨跌幅']/100)
                end_close = last_row['收盘']
                
                period_chg = (end_close - start_open) / start_open * 100
                period_turnover = group['成交额'].sum()
                
                agg_stats.append({
                    '代码': code,
                    '名称': first_row['名称'], # 假设没改名
                    '区间涨跌幅': period_chg,
                    '区间总成交': period_turnover
                })
            except:
                pass
        
        agg_df = pd.DataFrame(agg_stats)
        
        if agg_df.empty:
            st.warning("区间数据计算异常")
            st.stop()
            
        median_chg = agg_df['区间涨跌幅'].median()
        total_turnover = period_df['成交额'].sum() / 1e8
        top_gainer = agg_df.loc[agg_df['区间涨跌幅'].idxmax()]
        
        # 为了兼容后续 daily_df 的使用 (Treemap 和 选股)
        # 我们需要构造一个 "Proxy Daily DF"
        # 让后面的选股逻辑基于 "区间表现"
        daily_df = agg_df.rename(columns={'区间涨跌幅': '涨跌幅', '区间总成交': '成交额'}).copy()
        # 补齐其他字段
        # 收盘价用最后一天的
        # daily_df 还需要 '收盘' 用于展示? Treemap hover 需要
        # 我们可以 join 回去，但 Treemap hover 也可以只展示涨跌
        daily_df['收盘'] = 0 # Placeholder
        
        metric_label_date = "当前回放区间"
        metric_label_chg = "区间涨跌幅中位数"
        metric_label_to = "区间总成交"

    # --- 统一：补全股票名称 (防止显示为代码) ---
    _codes = daily_df['代码'].astype(str).unique().tolist()
    if _codes:
        _nmap = _refresh_name_map_for_codes(_codes, force=False)
        if _nmap:
            # fillna ensures we keep existing name if map misses (though usually map is better)
            # Use map to overwrite potential code-as-name
            daily_df['名称'] = daily_df['代码'].astype(str).map(_nmap).fillna(daily_df['名称'])

    # 显示指标行
    col1, col2, col3, col4 = st.columns(4)
    col1.metric(metric_label_date, display_date_str)
    col2.metric(metric_label_chg, f"{median_chg:.2f}%", 
                delta=f"{median_chg:.2f}%", delta_color="normal")
    col3.metric(metric_label_to, f"{total_turnover:.1f} 亿")
    top_name = str(top_gainer.get("名称", "未知"))
    top_pct = top_gainer.get("涨跌幅", None)
    if top_pct is None or (isinstance(top_pct, float) and pd.isna(top_pct)):
        top_pct = top_gainer.get("区间涨跌幅", None)
    top_pct_num = pd.to_numeric(top_pct, errors="coerce")
    if pd.isna(top_pct_num):
        top_text = f"{top_name} (N/A)"
    else:
        top_text = f"{top_name} ({top_pct_num:.2f}%)"
    col4.metric("领涨龙头", top_text)

    # --- 新增功能：分时走势叠加 ---
    st.markdown("---")
    st.subheader("📈 核心资产分时走势叠加")
    
    # 模式选择
    col_mode, col_num = st.columns([3, 1])
    with col_mode:
        chart_mode = st.radio("选股模式", ["成交额 Top (活跃度)", "指数贡献 Top (影响大盘)"], horizontal=True)
    with col_num:
        # 添加 key 避免 Bad setIn index 错误，并强制重置状态
        top_n = st.number_input("标的数量", min_value=5, max_value=50, value=20, step=5, 
                               help="沪/深各取 N 个标的（即总数为 2N）", key="top_n_stocks_input")

    st.caption(f"注：这里的排名是基于 **{selected_date}** 当日的数据计算的。如果是多日模式，则展示这些股票在过去几天的走势。")
    st.caption("注：指数贡献 = 涨跌幅 × 权重(近似为成交额/市值占比)。此模式能看到是谁在拉动或砸盘。")

    dates_sig = ("", "", 0)
    if target_dates:
        dates_sig = (
            target_dates[0].strftime("%Y-%m-%d"),
            target_dates[-1].strftime("%Y-%m-%d"),
            len(target_dates),
        )
    intraday_sig = (playback_mode, chart_mode, int(top_n), dates_sig)
    if st.session_state.get("intraday_sig") != intraday_sig:
        st.session_state["intraday_sig"] = intraday_sig
        log_action("\u5206\u65f6\u9009\u9879\u53d8\u66f4", playback=playback_mode, chart=chart_mode, top_n=top_n, dates=dates_sig)
        st.session_state["show_intraday"] = False

    show_intraday = st.checkbox("加载分时走势 (本地优先，无则网络拉取)", key="show_intraday")
    prev_show = st.session_state.get("show_intraday_prev", False)
    if show_intraday and not prev_show:
        log_action("\u52fe\u9009\u5206\u65f6\u52a0\u8f7d")
    st.session_state["show_intraday_prev"] = show_intraday
    
    if show_intraday:
        # 使用 placeholder 放置进度条，避免组件销毁导致的索引错乱
        progress_area = st.empty()
        
        # 统一选股逻辑：无论是成交额还是指数贡献，都按沪深分别取 Top N
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
        top_codes = top_stocks_df['代码'].astype(str).tolist()
        st.session_state["last_top_codes"] = top_codes
        name_map = _refresh_name_map_for_codes(top_codes, force=False)
        if name_map:
            top_stocks_df['名称'] = top_stocks_df['代码'].astype(str).map(name_map).fillna(top_stocks_df['名称'])

        target_stocks_list = []
        for _, row in top_stocks_df.iterrows():
            target_stocks_list.append((row['代码'], row['名称'], row['成交额'])) 
        
        all_intraday_data = [] 
        
        period_to_use = '5'
        
        if len(target_dates) > 5 and playback_mode == "多日走势拼接":
            if len(target_dates) > 30:
                period_to_use = '15' # 超过30天使用15分钟线
                st.info(f"ℹ️ 您选择了 {len(target_dates)} 天：系统自动切换至【15分钟级】数据。")
            else:
                period_to_use = '5'
                st.info(f"ℹ️ 您选择了 {len(target_dates)} 天：系统自动切换至【5分钟级】数据。")
        elif len(target_dates) > 10:
             st.toast(f"⚠️ 您选择了 {len(target_dates)} 天的数据，加载可能较慢，请耐心等待...")
        
        target_dates_to_fetch = target_dates
        total_steps = len(target_dates_to_fetch)
        logger.info("分时加载开始: 模式=%s 日期数=%s 标的数=%s 周期=%s", chart_mode, len(target_dates_to_fetch), len(target_stocks_list), period_to_use)

        # 改回扁平化结构，不再使用 container，减少 DOM 操作层级
        # 并发线程中缓存的 show_spinner=False 已经设置，这里应该安全了
        status_text = st.empty()
        fetch_progress = st.progress(0)
        for i, d_date in enumerate(target_dates_to_fetch):
            status_text.text(f"🔄 \u6b63\u5728\u83b7\u53d6: {d_date.strftime('%Y-%m-%d')} | \u5468\u671f={period_to_use}\u5206\u949f | \u76ee\u6807={len(target_stocks_list)}+\u6307\u65703 ({i+1}/{total_steps})")
            fetch_progress.progress((i + 1) / total_steps)
            
            d_str = d_date.strftime("%Y-%m-%d")
            day_results, day_failures, day_stats = fetch_intraday_data_v2(target_stocks_list, d_str, period=period_to_use)

            success = day_stats.get('success', 0)
            failed = day_stats.get('failed', 0)
            total_req = day_stats.get('total', 0)
            cache_hits = day_stats.get('cache', 0)
            network_calls = day_stats.get('network', 0)

            logger.info("\u5206\u65f6\u65e5\u6c47\u603b: date=%s total=%s success=%s failed=%s cache=%s network=%s", d_str, total_req, success, failed, cache_hits, network_calls)
            if day_failures:
                for err in day_failures[:3]:
                    logger.warning("\u5206\u65f6\u5931\u8d25: code=%s name=%s api=%s reason=%s", err.get('code'), err.get('name'), err.get('api'), err.get('reason'))

            for res in day_results:
                res["data"]["date_col"] = d_str
                res["real_date"] = d_date
            
            all_intraday_data.extend(day_results)
        
        logger.info("分时加载完成: 结果数=%s", len(all_intraday_data))
        # 数据拉取完毕后，清除进度组件
        status_text.empty()
        fetch_progress.empty()
        # 移除外层占位符的清理，因为已经不再使用
        progress_area.empty()
            
        if not all_intraday_data:
            st.warning("未能获取到分时数据")
        else:
                # --- 数据重组 ---
                # 将分散的数据合并为： { 'code': { 'name':..., 'is_index':..., 'full_data': DataFrame } }
                combined_series = {}
                
                for item in all_intraday_data:
                    code = item['code']
                    if code not in combined_series:
                        # 查找该股票在选定日(最后一日)的成交额，用于定线宽
                        to_val = 0
                        if not item.get('is_index'):
                            matches = daily_df[daily_df['代码'] == code]
                            if not matches.empty:
                                to_val = matches.iloc[0]['成交额']
                        
                        combined_series[code] = {
                            'name': item['name'],
                            'code': code,
                            'is_index': item.get('is_index', False),
                            'turnover': to_val, # 使用最后一天的成交额
                            'dfs': []
                        }
                    combined_series[code]['dfs'].append(item['data'])
                
                # 合并 DataFrame 并构建连续时间轴
                # 策略：不使用真实时间轴，而是使用 "交易分钟序列" (Trading Minute Index)
                # 每天 240 分钟。Day 1: 0-239, Day 2: 240-479...
                # 需要生成一个 X轴 Label Map
                
                final_plot_data = [] # List of {idx, pct_chg, ...}
                x_tick_vals = []
                x_tick_text = []
                
                # 为每一天生成标准时间序列 (避免缺失分钟导致的错位)
                # 09:30 - 11:30 (121 points usually inclusive? A股分钟线通常包含 11:30 和 15:00)
                # 分钟线通常是 09:31 - 11:30 (120 mins) 13:01 - 15:00 (120 mins). 
                # AkShare 返回的数据时间如果是 09:30 通常代表开盘?
                # 让我们通过观察第一条数据来决定逻辑，通常直接 concat 即可
                # 为了解决 GAP，我们在 UI 绘图层强制把 x 映射为 0, 1, 2...
                
                # 提取真正获取到数据的日期列表 (去除节假日/无数据日)
                valid_dates = set()
                for item in all_intraday_data:
                     if 'real_date' in item:
                         valid_dates.add(item['real_date'].strftime("%Y-%m-%d"))
                
                days_list = sorted(list(valid_dates))
                
                if not days_list:
                    # 如果过滤后居然没了（理论上外层 checked not empty），回退兜底
                     days_list = sorted(list(set([x.strftime("%Y-%m-%d") for x in target_dates_to_fetch])))
                
                # 构建基准时间网格 (Template) - 每天 240/241 个点
                # 09:30 - 11:30, 13:00 - 15:00
                dummy_date = "2000-01-01"
                morning_range = pd.date_range(f"{dummy_date} 09:30", f"{dummy_date} 11:30", freq="1min")
                afternoon_range = pd.date_range(f"{dummy_date} 13:00", f"{dummy_date} 15:00", freq="1min")
                daily_time_template = morning_range.union(afternoon_range) # Size approx 242
                
                # 计算总偏移量
                points_per_day = len(daily_time_template)
                
                for code, info in combined_series.items():
                    # 合并、排序
                    full_df = pd.concat(info['dfs']).sort_values(['date_col', 'time'])
                    
                    # 重新构造 X 轴 (Int)
                    # 算法：对于每一行，找到它是 第几天 的 第几分钟
                    # x_int = day_index * points_per_day + minute_index_in_day
                    
                    full_df['time_str'] = full_df['time'].dt.strftime("%H:%M:%S")
                    
                    x_values = []
                    
                    for idx, row in full_df.iterrows():
                        d_str = row['date_col']
                        t_str = row['time_str'] # 完整时间对象
                        
                        # 确定是第几天
                        day_idx = days_list.index(d_str) if d_str in days_list else 0
                        
                        # 确定是当天的第几分钟
                        # 简单转换：小时*60 + 分钟
                        t_obj = row['time'] # timestamp
                        mins_from_midnight = t_obj.hour * 60 + t_obj.minute
                        
                        # 把中午休市的时间压掉
                        # 9:30 (570) -> 11:30 (690). Length 120.
                        # 13:00 (780) -> 15:00 (900). 
                        
                        if mins_from_midnight <= 690: # Morning
                            offset = mins_from_midnight - 570 # 09:30 is 0
                        else: # Afternoon
                            offset = 120 + (mins_from_midnight - 780) # 13:00 starts at 120+
                            
                        final_x = day_idx * (240 + 20) + offset # 加20个单位的间隔让天与天之间有点空隙
                        x_values.append(final_x)
                        
                    full_df['x_int'] = x_values
                    
                    # 计算累计涨跌幅 (如果是多日，需要链式计算？)
                    # 简单方案：每天重新归零？还是多日连贯？
                    # 用户说 "拼起来形成完整的图"，通常意味着连贯趋势。
                    # 以第一天开盘价为基准
                    base_price = full_df['close'].iloc[0]
                    # 如果中间有断点，简单的 (close - base) / base 可能失真（因为昨收...）
                    # 准确做法：累乘每天的涨跌幅。
                    # 但这里只要大概趋势。如果用 (Px - P0) / P0，那跨日缺口会体现为直线的跳跃。
                    # 这符合 "真实价格走势"。
                    
                    full_df['cumulative_pct'] = (full_df['close'] - base_price) / base_price * 100
                    
                    info['plot_data'] = full_df
                
                # 生成 X 轴标签
                for i, d_str in enumerate(days_list):
                    base_x = i * (240 + 20)
                    day_label = d_str[5:] # MM-DD
                    
                    if len(days_list) > 1:
                        # 多日模式：只显示日期在中间 或者 开头
                        # 为了简洁，只在每天的中间显示一个日期
                        x_tick_vals.append(base_x + 120) 
                        x_tick_text.append(day_label)
                    else:
                        # 单日模式：显示详细时间点
                        # 09:30
                        x_tick_vals.append(base_x)
                        x_tick_text.append(f"{day_label}\n09:30")
                        # 11:30
                        x_tick_vals.append(base_x + 120)
                        x_tick_text.append("11:30/13:00")
                        # 15:00
                        x_tick_vals.append(base_x + 240)
                        x_tick_text.append("15:00")

                # 分类
                idx_data = [v for k,v in combined_series.items() if v['is_index']]
                stk_data = [v for k,v in combined_series.items() if not v['is_index']]
                
                sh_stocks = [v for v in stk_data if v['code'].startswith('6')]
                sz_stocks = [v for v in stk_data if not v['code'].startswith('6')]
                
                sh_index = [v for v in idx_data if v['code'] in ['000001', '000300']]
                sz_index = [v for v in idx_data if v['code'] in ['399001', '000300']]

                # 绘图函数 v3
                def plot_intraday_v3(stocks, indices, title_suffix):
                    fig = go.Figure()
                    
                    # 个股
                    if stocks:
                        max_to = max([s['turnover'] for s in stocks]) if stocks else 1
                        min_to = min([s['turnover'] for s in stocks]) if stocks else 0
                        
                        # 生成 distinct colors
                        color_palette = px.colors.qualitative.Alphabet + px.colors.qualitative.Dark24
                        
                        for i, s in enumerate(stocks):
                            if max_to == min_to: width=2
                            else: width = 1 + 3*(s['turnover'] - min_to)/(max_to - min_to)
                            
                            df_p = s['plot_data']
                            last_val = df_p['cumulative_pct'].iloc[-1]
                            
                            # 之前的红绿逻辑: color = 'rgba(214, 39, 40, 0.4)' if last_val > 0 else 'rgba(44, 160, 44, 0.4)'
                            # 现在改为区分颜色
                            color = color_palette[i % len(color_palette)]
                            
                            fig.add_trace(go.Scatter(
                                x=df_p['x_int'],
                                y=df_p['cumulative_pct'],
                                mode='lines',
                                name=s['name'], # Use name directly instead of code for legend
                                # line=dict(width=width, color=color),
                                # 个股线宽不需要太粗，颜色要清晰
                                line=dict(width=max(1.5, width), color=color),
                                hovertemplate=f"<b>{s['name']}</b><br>涨跌: %{{y:.2f}}%<br>时间: %{{customdata}}",
                                customdata=df_p['date_col'] + ' ' + df_p['time_str']
                            ))
                            
                    # 指数
                    idx_colors = {'000300': 'black', '000001': '#d62728', '399001': '#1f77b4'}
                    for idx in indices:
                        df_p = idx['plot_data']
                        c_code = idx.get('code', '000300')
                        
                        fig.add_trace(go.Scatter(
                            x=df_p['x_int'],
                            y=df_p['cumulative_pct'],
                            mode='lines',
                            name=idx['name'],
                            line=dict(width=3, color=idx_colors.get(c_code, 'black')),
                            hovertemplate=f"<b>{idx['name']}</b><br>涨跌: %{{y:.2f}}%"
                        ))

                    # 3. 分割线 (如果是多日)
                    if len(days_list) > 1:
                        for i in range(1, len(days_list)):
                            # 在每一天开始前画竖线
                            boundary = i * (240 + 20) - 10
                            fig.add_vline(x=boundary, line_width=1, line_dash="dash", line_color="gray")

                    fig.update_layout(
                        title=f"分时走势叠加 ({'多日拼接' if len(days_list)>1 else days_list[0]}) - {title_suffix}",
                        xaxis=dict(
                            tickmode='array',
                            tickvals=x_tick_vals,
                            ticktext=x_tick_text,
                            showgrid=True,
                            showspikes=True, # 显示垂直辅助线
                            spikemode='across',
                            spikesnap='cursor',
                            showline=True, 
                            linewidth=1, 
                            linecolor='black',
                            mirror=True
                        ),
                        yaxis=dict(
                            showspikes=True # Y轴也显示辅助线，方便看点位
                        ),
                        yaxis_title="累计涨跌幅 (%)",
                        hovermode="x unified", # 开启统一 Hover，显示该时间点所有数据
                        height=700, # 稍微调高高度以容纳更多 Hover 信息
                        legend=dict(orientation="h", y=1.02, x=1, xanchor='right') # 保持图例的布局
                    )
                    
                    # ⚠️ 关键修正：确保 Hover Tooltip 的排序按照 Y 轴数值 (从高到低)
                    # "closest" 模式配合 "compare" 可能无法生效，但在 "x unified" 模式下，
                    # 默认是按照 Trace 添加顺序排序的。
                    # Plotly (JS层) 在 x unified 下有默认排序逻辑 (通常是 value descending)，但在某些版本可能不稳定。
                    # 为了增强排序体验，我们可以尝试设置 layout.hoverlabel.namelength = -1
                    
                    fig.update_layout(hoverlabel=dict(namelength=-1))
                    
                    return fig

                tab1, tab2 = st.tabs(["沪市 (SH)", "深市 (SZ)"])
                
                with tab1:
                    st.plotly_chart(plot_intraday_v3(sh_stocks, sh_index, f"沪市 - {chart_mode}"), width="stretch")
                with tab2:
                    st.plotly_chart(plot_intraday_v3(sz_stocks, sz_index, f"深市 - {chart_mode}"), width="stretch")
    
    # --- 可视化 ---
    st.subheader(f"📊 {selected_date.strftime('%Y年%m月%d日')} 市场全景热力图")
    
    # A股专用色谱
    max_limit = 7
    min_limit = -7
    
    # 数据清洗：移除空名称和零成交额的记录，防止 Treemap 报错 "Non-leaves rows are not permitted"
    # 当 '名称' 为空字符串时，Plotly 会将其误判为根节点，导致层级冲突
    valid_mask = (daily_df['名称'].notna()) & (daily_df['名称'].astype(str).str.strip() != "") & (daily_df['成交额'] > 0)
    plot_df = daily_df[valid_mask].copy()

    # 优化显示：名称下显示代码
    plot_df['显示标签'] = plot_df['名称'].astype(str)
    # 如果想都显示： plot_df['显示标签'] = plot_df['名称'] + "\n" + plot_df['代码'].astype(str)
    
    if plot_df.empty:
        st.warning("暂无足够数据绘制市场全景热力图")
    else:
        fig = px.treemap(
            plot_df,
            path=['显示标签'], # 使用处理后的名称
            values='成交额', # 用成交额代表热度/权重 (因为历史市值难获取)
            color='涨跌幅',
            color_continuous_scale=['#00a65a', '#ffffff', '#dd4b39'], # 绿 -> 白 -> 红
            range_color=[min_limit, max_limit],
            hover_data={
                '显示标签': False,
                '名称': True,
                '代码': True,
                '收盘': True,
                '涨跌幅': ':.2f',
                '成交额': True
            },
            height=650
        )
        
        # 优化显示
        fig.update_traces(
            textinfo="label+value+percent entry",
            hovertemplate="<b>%{customdata[0]}</b> (%{customdata[1]})<br>收盘价: %{customdata[2]}<br>涨跌幅: %{color:.2f}%<br>成交额: %{value:.2s}"
        )
        fig.update_layout(
            margin=dict(t=10, l=10, r=10, b=10),
            coloraxis_colorbar=dict(title="涨跌幅(%)")
        )
        
        st.plotly_chart(fig, width="stretch")
    
    # 可选：显示详细数据表
    
    # 可选：显示详细数据表
    with st.expander("查看当日详细数据"):
        st.dataframe(
            daily_df[['代码', '名称', '收盘', '涨跌幅', '成交额']].style.format({
                '收盘': '{:.2f}',
                '涨跌幅': '{:.2f}%',
                '成交额': '{:,.0f}'
            }),
            hide_index=True
        )
