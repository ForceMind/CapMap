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
        # 1. 自动补全搜索框 (Combo box)
        # 获取缓存中的热门列表
        all_codes = sorted(origin_df['代码'].unique())
        code_name_map = {}
        unique_stocks = origin_df.drop_duplicates(subset=['代码'])[['代码', '名称']]
        for _, row in unique_stocks.iterrows():
            code_name_map[row['代码']] = f"{row['代码']} | {row['名称']}"
        
        # 默认列表
        options_list = [code_name_map[c] for c in all_codes]
        
        # 使用 selectbox 实现搜索 (Streamlit 原生支持输入筛选)
        # 但如果用户想要输入不在列表里的代码，selectbox 默认不支持 custom input
        # 变通方案：在 options 列表头部提供一个 "Custom Input..." 提示，
        # 或者教导用户如果搜不到，就去下面的 text_input 输入。
        # 更好的方案：既然有API，我们可以允许用户直接通过 text_input 覆盖。
        
        # 统一为一个控件：Selectbox with input functionality is hard in plain Streamlit.
        # We will keep the select box for cached stocks, and a small expander or just text input for "Others".
        
        # 但是用户说 "Input stock in ONE place, fuzzy search supported, call API to query"
        # 意味着如果 selectbox 搜不到，应该能 fallback 到 API 查询。
        # 这里用一个简单的模式：如果用户在 selectbox 没找到，可以选 "手动输入"，然后弹出 text input。
        
        # 实际上 Streamlit selectbox 已经很好用了。只有当 origin_df 缺少该票时才需要手动。
        # 增加一个 "🔍 搜索/输入代码"
        
        search_input = st.text_input("🔍 搜索/输入股票代码", placeholder="输入代码(如000001) 或 名称", help="支持模糊搜索")
        
        selected_code = None
        selected_name = "未命名"
        
        # 逻辑：如果 search input 有值，优先尝试匹配 manual input or filter list
        if search_input:
            search_str = search_input.strip()
            # 1. 尝试在现有缓存中模糊匹配
            matched = [opt for opt in options_list if search_str in opt]
            if matched:
                # 如果有匹配，显示匹配列表供选择
                selected_display = st.selectbox("请选择匹配结果", options=matched, index=0)
                selected_code = selected_display.split(" | ")[0]
                selected_name = selected_display.split(" | ")[1]
            else:
                # 2. 没匹配到，假设是新代码，直接使用 search_str 作为 code (如果是数字)
                if search_str.isdigit() and len(search_str) == 6:
                    selected_code = search_str
                    selected_name = f"未知 ({selected_code})"
                    st.caption("⚠️ 本地缓存未找到，尝试直接拉取数据...")
                else:
                    st.warning("未找到匹配股票，请输入准确的6位代码。")
        else:
             # 没输入，显示默认热门/全部列表
            selected_display = st.selectbox("选择或搜索缓存股票", options=options_list, index=0)
            selected_code = selected_display.split(" | ")[0]
            selected_name = selected_display.split(" | ")[1]

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

