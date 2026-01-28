import streamlit as st
import plotly.graph_objects as go
import pandas as pd
from datetime import datetime, timedelta
from plotly.subplots import make_subplots

from core.data_access import (
    fetch_cached_min_data,
    get_all_stocks_list,
    DEFAULT_MIN_PERIOD
)

def render_stock_analysis_view(origin_df):
    st.subheader("📈 个股多日走势 & 历史分析")
    
    # --- 0. 准备历史日线背景数据 (用于技术分析) ---
    # 如果用户查询了某只股票，我们可以先展示在这两年数据中的走势图（日线）
    
    # 获取全市场股票及搜索支持
    all_stocks_df = get_all_stocks_list() # columns: code, name, pinyin
    
    # --- 1. 控件区域 ---
    col1, col2, col3, col4 = st.columns([2, 1.5, 1, 1.5])
    
    with col1:
        # 1. 统一模糊搜索框 (Smart Search)
        # 逻辑：
        # - 用户输入 text
        # - 触发 rerun
        # - 代码在 backend 过滤 search_text in code/name/pinyin
        # - 下方选框 selectbox 用于确认具体的票
        
        search_text = st.text_input("🔍 搜索股票 (代码/名称/拼音)", 
                                    placeholder="例如: 600519, 茅台, MT",
                                    key="sa_search_input")
        
        selected_code = None
        selected_name = "未明"

        # 过滤逻辑
        filtered_df = pd.DataFrame()
        if search_text:
            s_str = search_text.strip().upper()
            if not all_stocks_df.empty:
                filtered_df = all_stocks_df[
                    all_stocks_df['code'].str.contains(s_str) | 
                    all_stocks_df['name'].str.contains(s_str) |
                    all_stocks_df['pinyin'].str.contains(s_str, na=False)
                ].head(20) # 限制显示前20个以防卡顿
        
        # 构建下拉选项
        options_map = {}
        
        # 1. 优先展示搜索匹配结果
        if not filtered_df.empty:
            for _, row in filtered_df.iterrows():
                # 格式: 代码 | 名称
                label = f"{row['code']} | {row['name']}"
                options_map[label] = row['code']
                
        # 2. 如果没有匹配结果，但输入看起来像是一个6位代码
        #    强制添加一个选项，允许用户"回车"确认查询
        elif search_text and search_text.strip().isdigit() and len(search_text.strip()) == 6:
             manual_code = search_text.strip()
             label = f"{manual_code} | (直接查询)"
             options_map[label] = manual_code
             
        # 3. 如果搜索框为空，显示"历史/热门"缓存
        #    这样既保留了便捷性，又不会在搜索失败时干扰视线
        elif not search_text:
             if not origin_df.empty:
                 # 添加一个占位符，提示用户
                 options_map["📋 请输入代码或从下方选择..."] = None
                 
                 unique_stocks = origin_df.drop_duplicates(subset=['代码'])[['代码', '名称']]
                 for _, row in unique_stocks.iterrows():
                    label = f"{row['代码']} | {row['名称']}"
                    options_map[label] = row['代码']

        # Selectbox 用于显示结果
        # 如果 options_map 为空（搜了东西但没搜到，也不像代码），则显示提示
        if options_map:
            # 这里的 label_visibility="collapsed" 是为了让它看起来像是搜索框的一部分
            selection_label = st.selectbox(
                label="选择股票", 
                options=list(options_map.keys()), 
                index=0, 
                label_visibility="collapsed",
                key="sa_selectbox_result"
            )
            
            # 处理选中逻辑
            if selection_label and options_map[selection_label]:
                selected_code = options_map[selection_label]
                # 尝试分离名称
                parts = selection_label.split("|")
                # 如果是 (直接查询)，名字暂定未知
                if "(直接查询)" in parts[-1]:
                    selected_name = f"未知 ({selected_code})"
                else:
                    selected_name = parts[-1].strip()
            else:
                 # 选中了占位符
                 if selection_label == "📋 请输入代码或从下方选择...":
                     st.info("👆 请在上方输入代码、名称或拼音")
                     return
        else:
            # 搜索无结果情况
             st.warning("⚠️ 未找到匹配股票，请输入准确的6位代码")
             return

    with col2:
        # 日期范围选择
        valid_dates = sorted(origin_df['日期'].dt.date.unique())
        default_end = valid_dates[-1]
        default_start = valid_dates[-5] if len(valid_dates) > 5 else valid_dates[0]
        
        date_range = st.date_input(
            "选择日期范围",
            value=(default_start, default_end),
            min_value=valid_dates[0],
            max_value=valid_dates[-1]
        )

    with col3:
        # 指数叠加选项
        overlay_index = st.selectbox(
            "叠加指数",
            options=["None", "000300 (沪深300)", "000905 (中证500)", "000852 (中证1000)", "000001 (上证指数)"],
            index=1 # 默认沪深300
        )
        if st.toggle("更新列表", help="如果搜不到股票，点此强制更新全市场列表"):
             # 这里用 toggle+spinner 是一种比较轻量的方式，主要是为了触发逻辑
             with st.spinner("正在同步A股列表..."):
                 get_all_stocks_list(force_update=True)
             st.rerun()
    
    with col4:
        st.write("") # Spacer
        st.write("")
        do_search = st.button("📊 生成图表", type="primary", use_container_width=True)

    # --- 2. 主逻辑 ---
    if selected_code:
        # A. 展示 2年日线背景 (Day Level)
        # 从 origin_df 过滤该股票的所有历史数据
        stock_daily_df = origin_df[origin_df['代码'] == selected_code].sort_values('日期')
        
        if not stock_daily_df.empty:
            with st.expander(f"📊 {selected_name} ({selected_code}) 近两年日线概览 & 技术指标", expanded=True):
                # 计算 MA 和 ATR
                stock_daily_df['MA20'] = stock_daily_df['收盘'].rolling(window=20).mean()
                stock_daily_df['MA60'] = stock_daily_df['收盘'].rolling(window=60).mean()
                stock_daily_df['MA250'] = stock_daily_df['收盘'].rolling(window=250).mean()
                
                # ATR 计算
                high_low = stock_daily_df['最高'] - stock_daily_df['最低']
                high_close = (stock_daily_df['最高'] - stock_daily_df['收盘'].shift()).abs()
                low_close = (stock_daily_df['最低'] - stock_daily_df['收盘'].shift()).abs()
                tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
                stock_daily_df['ATR20'] = tr.rolling(window=20).mean()
                
                # 绘制日线图
                fig_daily = go.Figure()
                
                # K线
                fig_daily.add_trace(go.Candlestick(
                    x=stock_daily_df['日期'],
                    open=stock_daily_df['开盘'], high=stock_daily_df['最高'],
                    low=stock_daily_df['最低'], close=stock_daily_df['收盘'],
                    name='日K线'
                ))
                
                # 均线
                fig_daily.add_trace(go.Scatter(x=stock_daily_df['日期'], y=stock_daily_df['MA20'], mode='lines', line=dict(color='orange', width=1), name='MA20'))
                fig_daily.add_trace(go.Scatter(x=stock_daily_df['日期'], y=stock_daily_df['MA60'], mode='lines', line=dict(color='blue', width=1), name='MA60'))
                fig_daily.add_trace(go.Scatter(x=stock_daily_df['日期'], y=stock_daily_df['MA250'], mode='lines', line=dict(color='purple', width=2), name='MA250 (牛熊线)'))
                
                fig_daily.update_layout(
                    title=f"{selected_name} 日线趋势 (含MA250)",
                    xaxis_rangeslider_visible=False,
                    height=400,
                    margin=dict(l=20, r=20, t=40, b=20)
                )
                st.plotly_chart(fig_daily, use_container_width=True)
                
                # ATR 指标卡
                last_row = stock_daily_df.iloc[-1]
                atr_val = last_row['ATR20']
                price = last_row['收盘']
                atr_pct = (atr_val / price) * 100 if price > 0 else 0
                
                cols = st.columns(4)
                cols[0].metric("当前价格", f"{price:.2f}")
                cols[1].metric("MA250", f"{last_row['MA250']:.2f}" if pd.notnull(last_row['MA250']) else "N/A")
                cols[2].metric("ATR (20日波动)", f"{atr_val:.3f}")
                cols[3].metric("ATR占比 (波动率)", f"{atr_pct:.2f}%")
        else:
             st.info(f"暂无 {selected_code} 的本地日线缓存数据（可能是新股或未在初始化列表中）。")

    if do_search and len(date_range) == 2:
        start_date, end_date = date_range
        # 筛选日期范围内的valid dates
        target_dates = [d for d in valid_dates if start_date <= d <= end_date]
        
        if not target_dates:
            st.warning("所选范围内无有效交易日。")
            return

        with st.spinner(f"正在拉取 {selected_name} ({len(target_dates)} 天) 的分钟数据..."):
            stock_data_list = []
            index_data_list = []
            
            # 进度条
            progress_bar = st.progress(0)
            
            idx_code = None
            if overlay_index != "None":
                idx_code = overlay_index.split(" ")[0]


            for i, d in enumerate(target_dates):
                d_str = d.strftime("%Y-%m-%d")
                
                # 拉取个股
                df_stock = fetch_cached_min_data(selected_code, d_str, is_index=False, period=DEFAULT_MIN_PERIOD)
                if df_stock is not None and not df_stock.empty:
                    # 兼容性处理：检查列名并重命名标准列
                    # 标准列: time, open, high, low, close, volume
                    # 旧缓存: time, pct_chg, close (缺 open,high,low,volume)
                    
                    # 1. 确保 time 列存在并转换
                    # 注意：fetch_cached_min_data 不保证列名一定是英文，需要在这里再次做保障或映射
                    # 但我们在 data_access.py 里已经做了 rename，理论上这里拿到的应该是英文 standard columns
                    
                    if 'time' in df_stock.columns:
                        # 已经是 datetime 或 string
                        # 如果是 string, 需要 concat date
                        # Check type of first element
                        first_val = df_stock['time'].iloc[0]
                        if isinstance(first_val, str):
                            # 有些缓存可能只存了 "09:30:00"，需要加上日期
                            # 但 akshare 返回的是全时间 string "2023-01-01 09:30:00"
                            if len(first_val) <= 9: # 09:30:00
                                df_stock['time'] = pd.to_datetime(d_str + " " + df_stock['time'])
                            else:
                                df_stock['time'] = pd.to_datetime(df_stock['time'])
                        else:
                            # 已经是 timestamp
                            pass
                    elif '时间' in df_stock.columns:
                        df_stock.rename(columns={'时间': 'time'}, inplace=True)
                        # 处理同上... 略，假设 data_access 已经统一了
                        pass
                    
                    # 2. 补全缺失列 (针对旧缓存)
                    if 'open' not in df_stock.columns:    df_stock['open'] = df_stock['close']
                    if 'high' not in df_stock.columns:    df_stock['high'] = df_stock['close']
                    if 'low' not in df_stock.columns:     df_stock['low'] = df_stock['close']
                    if 'volume' not in df_stock.columns:  df_stock['volume'] = 0
                    if '成交量' in df_stock.columns:
                        df_stock['volume'] = df_stock['成交量']

                    stock_data_list.append(df_stock)
                
                # 拉取指数 (如果需要)
                if idx_code:
                    df_index = fetch_cached_min_data(idx_code, d_str, is_index=True, period=DEFAULT_MIN_PERIOD)
                    if df_index is not None and not df_index.empty:
                        # 同样处理指数的 time
                        if 'time' in df_index.columns:
                             first_val = df_index['time'].iloc[0]
                             if isinstance(first_val, str):
                                 if len(first_val) <= 9:
                                     df_index['time'] = pd.to_datetime(d_str + " " + df_index['time'])
                                 else:
                                     df_index['time'] = pd.to_datetime(df_index['time'])
                        
                        # 补全指数缺失
                        if 'close' not in df_index.columns and '收盘' in df_index.columns:
                            df_index['close'] = df_index['收盘']
                            
                        index_data_list.append(df_index)
                
                progress_bar.progress((i + 1) / len(target_dates))

            progress_bar.empty()


            if not stock_data_list:
                st.error("未找到所选股票的分钟数据，请检查缓存或尝试预取。")
                return

            # 合并数据
            df_full_stock = pd.concat(stock_data_list).sort_values('time').reset_index(drop=True)
            df_full_index = pd.concat(index_data_list).sort_values('time').reset_index(drop=True) if index_data_list else pd.DataFrame()

            # --- 数据对齐与时间格式化 (关键步骤：解决Gap问题) ---
            # 为了完美去除空隙，我们将使用 category 轴，这要求 x 轴必须是字符串且完全对齐。
            # 1. 以个股数据为主轴
            # 2. 将指数数据 merge 进来
            
            # 确保 time 是 datetime
            df_full_stock['time'] = pd.to_datetime(df_full_stock['time'])
            if not df_full_index.empty:
                df_full_index['time'] = pd.to_datetime(df_full_index['time'])
                # 重命名指数列以免冲突
                df_full_index = df_full_index[['time', 'close']].rename(columns={'close': 'close_index'})
                # Merge: left join，保证以个股时间为准
                df_merged = pd.merge(df_full_stock, df_full_index, on='time', how='left')
            else:
                df_merged = df_full_stock.copy()
                df_merged['close_index'] = np.nan

            # 生成字符串时间轴，用于 Category Mapping
            # 格式：MM-DD HH:MM
            df_merged['time_str'] = df_merged['time'].dt.strftime('%m-%d %H:%M')

            # --- 3. 绘图 ---
            # 创建子图: Row 1 = K线/价格 + 指数, Row 2 = 成交量
            fig = make_subplots(
                rows=2, cols=1, 
                shared_xaxes=True, 
                vertical_spacing=0.03, # 减小间距
                row_heights=[0.7, 0.3],
                specs=[[{"secondary_y": True}], [{"secondary_y": False}]]
            )

            # A. 个股 K线
            fig.add_trace(go.Candlestick(
                x=df_merged['time_str'],
                open=df_merged['open'],
                high=df_merged['high'],
                low=df_merged['low'],
                close=df_merged['close'],
                name=selected_name,
                increasing_line_color='#ef5350', # 鲜艳红
                increasing_fillcolor='#ef5350',
                decreasing_line_color='#26a69a', # 鲜艳绿
                decreasing_fillcolor='#26a69a'
            ), row=1, col=1)

            # B. 叠加指数 (右轴)
            if not df_full_index.empty:
                # 检查数据是否存在
                valid_idx = df_merged['close_index'].dropna()
                if not valid_idx.empty:
                    fig.add_trace(go.Scatter(
                        x=df_merged['time_str'],
                        y=df_merged['close_index'],
                        mode='lines',
                        name=f"指数: {idx_code}",
                        line=dict(color='rgba(255, 165, 0, 0.7)', width=2), # 半透明橙色
                        hoverinfo='y+name' 
                    ), row=1, col=1, secondary_y=True)

            # C. 成交量 (Row 2)
            colors = ['#ef5350' if r['close'] >= r['open'] else '#26a69a' for _, r in df_merged.iterrows()]
            fig.add_trace(go.Bar(
                x=df_merged['time_str'],
                y=df_merged['volume'],
                name="成交量",
                marker_color=colors
            ), row=2, col=1)

            # 布局优化
            fig.update_layout(
                title=dict(
                    text=f"{selected_name} ({start_date} ~ {end_date})",
                    y=0.98  # 稍微往上一点
                ),
                xaxis_rangeslider_visible=False,
                height=700, # 稍微高一点
                margin=dict(l=60, r=60, t=60, b=40),
                legend=dict(
                    orientation="h", 
                    y=1.01, 
                    x=0.5, 
                    xanchor="center"
                ),
                hovermode="x unified" # 统一显示 tooltip
            )
            
            # 使用 Category 轴彻底消除 Gap
            fig.update_xaxes(
                type='category', 
                tickmode='auto', 
                nticks=8, # 限制显示的数量，防止重叠
                row=2, col=1
            )
            
            # Row 1 不需要显示 x 轴 label (因为 shared_xaxes=True，通常只在最底下显示)
            # 但 Plotly 有时候 shared_xaxes 还是会显示 category 的 grid
            fig.update_xaxes(showticklabels=False, type='category', row=1, col=1)

            # Y轴设置
            fig.update_yaxes(title_text="价格", row=1, col=1, secondary_y=False)
            fig.update_yaxes(title_text="指数", row=1, col=1, secondary_y=True, showgrid=False) # 右轴不显示 grid，免得乱
            fig.update_yaxes(title_text="成交量", row=2, col=1)

            st.plotly_chart(fig, use_container_width=True)

