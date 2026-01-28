import streamlit as st
import plotly.graph_objects as go
import pandas as pd
from datetime import datetime, timedelta
from plotly.subplots import make_subplots

from core.data_access import (
    fetch_cached_min_data,
    DEFAULT_MIN_PERIOD
)

def render_stock_analysis_view(origin_df):
    st.subheader("📈 个股多日走势叠加分析")
    
    # --- 1. 控件区域 ---
    col1, col2, col3, col4 = st.columns([1.5, 1.5, 1, 1.5])
    
    with col1:
        # 获取所有股票代码和名称
        all_codes = sorted(origin_df['代码'].unique())
        # 创建搜索映射
        code_name_map = {}
        # 优化：只取每个代码的一行来做映射，加快速度
        unique_stocks = origin_df.drop_duplicates(subset=['代码'])[['代码', '名称']]
        for _, row in unique_stocks.iterrows():
            code_name_map[row['代码']] = f"{row['代码']} | {row['名称']}"
        
        selected_code_display = st.selectbox(
            "选择股票", 
            options=[code_name_map[c] for c in all_codes],
            index=0
        )
        selected_code = selected_code_display.split(" | ")[0]
        selected_name = selected_code_display.split(" | ")[1]

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
    
    with col4:
        st.write("") # Spacer
        st.write("")
        do_search = st.button("📊 生成图表", type="primary", use_container_width=True)

    # --- 2. 主逻辑 ---
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
                    # 确保时间列是 datetime
                    df_stock['time'] = pd.to_datetime(d_str + " " + df_stock['时间'])
                    stock_data_list.append(df_stock)
                
                # 拉取指数 (如果需要)
                if idx_code:
                    df_index = fetch_cached_min_data(idx_code, d_str, is_index=True, period=DEFAULT_MIN_PERIOD)
                    if df_index is not None and not df_index.empty:
                        df_index['time'] = pd.to_datetime(d_str + " " + df_index['时间'])
                        index_data_list.append(df_index)
                
                progress_bar.progress((i + 1) / len(target_dates))

            progress_bar.empty()

            if not stock_data_list:
                st.error("未找到所选股票的分钟数据，请检查缓存或尝试预取。")
                return

            # 合并数据
            df_full_stock = pd.concat(stock_data_list).sort_values('time').reset_index(drop=True)
            df_full_index = pd.concat(index_data_list).sort_values('time').reset_index(drop=True) if index_data_list else pd.DataFrame()

            # --- 3. 绘图 ---
            # 创建子图: Row 1 = K线/价格, Row 2 = 成交量
            fig = make_subplots(
                rows=2, cols=1, 
                shared_xaxes=True, 
                vertical_spacing=0.05,
                row_heights=[0.7, 0.3],
                specs=[[{"secondary_y": True}], [{"secondary_y": False}]]
            )

            # A. 个股 K线 (如果数据够细，或者直接画收盘线)
            # 在多日分钟图里，K线可能会太密，我们画线图，或者允许缩放
            # 如果是 Candlestick
            fig.add_trace(go.Candlestick(
                x=df_full_stock['time'],
                open=df_full_stock['开盘'],
                high=df_full_stock['最高'],
                low=df_full_stock['最低'],
                close=df_full_stock['收盘'],
                name=selected_name,
                increasing_line_color='red', increasing_fillcolor='red',
                decreasing_line_color='green', decreasing_fillcolor='green'
            ), row=1, col=1)

            # B. 叠加指数 (右轴)
            if not df_full_index.empty:
                fig.add_trace(go.Scatter(
                    x=df_full_index['time'],
                    y=df_full_index['收盘'],
                    mode='lines',
                    name=f"指数: {idx_code}",
                    line=dict(color='orange', width=1.5),
                    opacity=0.7
                ), row=1, col=1, secondary_y=True)

            # C. 成交量 (Row 2), 区分颜色
            colors = ['red' if r['收盘'] >= r['开盘'] else 'green' for _, r in df_full_stock.iterrows()]
            fig.add_trace(go.Bar(
                x=df_full_stock['time'],
                y=df_full_stock['成交量'],
                name="成交量",
                marker_color=colors
            ), row=2, col=1)

            # 布局优化
            fig.update_layout(
                title=f"{selected_name} ({start_date} ~ {end_date}) 分时走势",
                xaxis_rangeslider_visible=False,
                height=600,
                margin=dict(l=50, r=50, t=50, b=50),
                legend=dict(orientation="h", y=1.02, yanchor="bottom", x=0, xanchor="left")
            )
            
            # 去掉非交易时间的 gap (Plotly 的 rangebreaks 很难完美适配 A股多日分钟线，
            # 简单做法是使用 category 轴，但这会破坏时间刻度。
            # 复杂做法是配置 rangebreaks)
            # A股交易时间: 09:30-11:30, 13:00-15:00.
            # 这里尝试添加 rangebreaks
            fig.update_xaxes(
                rangebreaks=[
                    dict(pattern='hour', bounds=[15, 9.5]), # 每天 15:00 到 次日 9:30
                    dict(pattern='hour', bounds=[11.5, 13]), # 中午休市 11:30 - 13:00
                    dict(bounds=["sat", "mon"]) # 周末 (虽然我们只选了 trading dates，但 rangebreaks 是基于日历的)
                ],
                row=2, col=1
            )
            # 同步 row1 的 x轴
            fig.update_xaxes(matches='x', row=1, col=1)

            st.plotly_chart(fig, use_container_width=True)
