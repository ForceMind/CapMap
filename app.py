import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import os

# --- Proxy Fix for System Environments ---
# Remove system proxies that might block requests to Eastmoney/Sina
for k in ['http_proxy', 'https_proxy', 'HTTP_PROXY', 'HTTPS_PROXY']:
    if k in os.environ:
        del os.environ[k]
# Force no proxy if needed by libraries
os.environ['NO_PROXY'] = '*'

# Import custom modules
from modules.config import STOCK_POOLS
from modules.data_loader import fetch_history_data, fetch_intraday_data_v2, background_prefetch_task
from modules.analysis import calculate_deviation_data, filter_deviation_data
from modules.visualization import plot_market_heatmap, plot_deviation_scatter, plot_intraday_charts
import modules.utils as utils

# --- Page Configuration ---
st.set_page_config(
    page_title="A股资金全景分析 (Pro)",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Sidebar Controls ---
st.sidebar.title("🎮 控制面板")

# 1. Strategy / Pool Selection
selected_pool = st.sidebar.selectbox(
    "📊 选择指数池", 
    options=list(STOCK_POOLS.keys()),
    index=0
)

# 2. Data Loading
if st.sidebar.button("🔄 刷新全部数据"):
    st.cache_data.clear()
    st.rerun()

# Load Historical Data
with st.spinner(f"🚀 正在调用AKShare接口获取 [{selected_pool}] 历史数据..."):
    full_df = fetch_history_data(selected_pool)

if full_df.empty:
    st.error("""
    **无法加载数据**
    
    可能有以下原因：
    1. **网络连接问题**: 无法连接到 AkShare 数据源 (EaseMoney/Sina)。已尝试绕过系统代理。
    2. **接口变动**: 数据源接口可能已更新。
    3. **非交易时间/数据未更新**: 如果是在开盘前，可能获取不到最新数据。
    
    建议：
    - 检查网络连接。
    - 尝试点击左侧 "刷新全部数据" 按钮。
    """)
    st.stop()

# 3. Date Selection
# Get available dates from data
available_dates = sorted(full_df['日期'].unique())
if not available_dates:
    st.error("数据源日期为空。")
    st.stop()

# Default to last available date
default_end_date = available_dates[-1]
# Default range: last 20 trading days
start_idx = max(0, len(available_dates) - 20)
default_start_date = available_dates[start_idx]

# Date Range Picker
st.sidebar.subheader("📅 时间范围")
col_d1, col_d2 = st.sidebar.columns(2)
with col_d1:
    start_date = st.date_input("开始日期", value=default_start_date, min_value=available_dates[0], max_value=available_dates[-1])
with col_d2:
    end_date = st.date_input("结束日期", value=default_end_date, min_value=available_dates[0], max_value=available_dates[-1])

# Convert to datetime for filtering
start_date = pd.Timestamp(start_date)
end_date = pd.Timestamp(end_date)

# Filter Data
mask = (full_df['日期'] >= start_date) & (full_df['日期'] <= end_date)
filtered_df = full_df.loc[mask].copy()

# Get dates actually present in the range
selected_dates = sorted(filtered_df['日期'].unique())
selected_dates_str = [pd.Timestamp(d).strftime('%Y-%m-%d') for d in selected_dates]


# --- Main Content ---
st.title(f"📈 {selected_pool} 资金情绪监控")
st.caption(f"数据范围: {start_date.strftime('%Y-%m-%d')} 至 {end_date.strftime('%Y-%m-%d')} | 包含 {len(selected_dates)} 个交易日")

# Tabs for different views
tab1, tab2, tab3 = st.tabs(["🗺️ 市场热力图", "🎯 资金偏离度分析", "📉 分时走势叠加"])

# --- Tab 1: Heatmap ---
with tab1:
    st.subheader("每日成交额资金分布")
    
    # Date slider for heatmap
    if len(selected_dates_str) > 0:
        hm_date_idx = st.slider(
            "选择日期查看热力图", 
            min_value=0, 
            max_value=len(selected_dates_str)-1, 
            value=len(selected_dates_str)-1,
            format="YYYY-MM-DD"
        )
        target_hm_date = selected_dates_str[hm_date_idx]
        st.info(f"当前展示日期: **{target_hm_date}**")
        
        daily_df = filtered_df[filtered_df['日期'] == pd.Timestamp(target_hm_date)]
        
        if not daily_df.empty:
            fig_hm = plot_market_heatmap(daily_df)
            st.plotly_chart(fig_hm, use_container_width=True)
        else:
            st.warning("该日期无数据。")
    else:
        st.warning("当前范围内无交易日数据。")


# --- Tab 2: Deviation Analysis ---
with tab2:
    st.subheader("资金偏离度与相关性分析")
    st.markdown("""
    **偏离度定义**: 个股涨跌幅 - 指数涨跌幅 (反映个股强弱)
    """)
    
    if len(selected_dates_str) < 2:
        st.warning("需要至少 2 个交易日的数据来计算区间偏离度。")
    else:
        # Calculate Deviation
        div_df, market_median_chg = calculate_deviation_data(full_df, selected_dates_str) # Pass correctly formatted dates
        
        if not div_df.empty:
            # --- Strategy Selection ---
            st.markdown("### 🔎 策略筛选")
            strategy_mode = st.radio("选择筛选策略", 
                                     ["默认 (全部展示)", 
                                      "🛡️ 护盘/控盘 (逆势大票)", 
                                      "🔥 游资/活跃 (高换手/高波)",
                                      "☠️ 出货/砸盘 (放量下跌)"], 
                                     horizontal=True)
            
            # Show Metrics
            col_m1, col_m2 = st.metrics = st.columns(2)
            col_m1.metric("基准(中位数)涨跌幅", f"{market_median_chg:.2f}%")
            
            # Apply Filter
            filtered_div = filter_deviation_data(div_df, strategy_mode=strategy_mode)
            
            col_m2.metric("当前策略筛选数量", f"{len(filtered_div)} 只")
            
            # Plot Scatter
            st.markdown("#### 资金偏离度分布")
            fig_sc = plot_deviation_scatter(filtered_div, strategy_mode)
            if fig_sc:
                st.plotly_chart(fig_sc, use_container_width=True)
            else:
                st.info("当前筛选无数据。")
            
            # --- Data Tables (Buy/Sell) ---
            st.divider()
            col_list1, col_list2 = st.columns(2)
            
            with col_list1:
                st.subheader("🔥 资金抱团 (向上偏离)")
                # 逻辑：偏离度 > 0, 按成交额降序
                buy_df = filtered_div[filtered_div['偏离度'] > 0].sort_values('成交额(亿)', ascending=False).head(50)
                st.dataframe(
                    buy_df[['代码', '名称', '偏离度', '成交额(亿)', '区间涨跌幅']].style.format({
                        '偏离度': '+{:.2f}%',
                        '成交额(亿)': '{:.1f}',
                        '区间涨跌幅': '{:.2f}%'
                    }),
                    use_container_width=True,
                    height=500
                )
                
            with col_list2:
                st.subheader("📉 资金出逃 (向下偏离)")
                # 逻辑：偏离度 < 0, 按成交额降序
                sell_df = filtered_div[filtered_div['偏离度'] < 0].sort_values('成交额(亿)', ascending=False).head(50)
                st.dataframe(
                    sell_df[['代码', '名称', '偏离度', '成交额(亿)', '区间涨跌幅']].style.format({
                        '偏离度': '{:.2f}%',
                        '成交额(亿)': '{:.1f}',
                        '区间涨跌幅': '{:.2f}%'
                    }),
                    use_container_width=True,
                    height=500
                )
        else:
            st.info("无法计算偏离度数据，请检查数据完整性。")


# --- Tab 3: Intraday Analysis ---
with tab3:
    st.subheader("分时走势深度复盘")
    
    # Intraday Controls
    col_i1, col_i2 = st.columns([1, 3])
    with col_i1:
        id_days_n = st.number_input("查看最近N天分时", min_value=1, max_value=5, value=1)
    
    # Determine dates for intraday
    # We take the LAST N dates from the selected_dates range
    if len(selected_dates_str) >= id_days_n:
        target_id_dates = selected_dates_str[-id_days_n:]
    else:
        target_id_dates = selected_dates_str
        
    with col_i2:
        st.write(f"正在加载分时数据范围: {target_id_dates}")

    # Stock Selection logic
    # Default to top 5 deviation stocks if div_df exists
    default_stocks = []
    if 'filtered_div' in locals() and not filtered_div.empty:
        default_stocks = filtered_div.head(3)['代码'].tolist()
    
    # User input for stocks
    selected_stocks_text = st.text_input("输入股票代码 (逗号分隔)", value=",".join(default_stocks))
    user_stocks = [s.strip() for s in selected_stocks_text.split(',') if s.strip()]
    
    # Intraday loader automatically fetches major indices (000300, 000001, 399001)
    target_stock_codes = list(set(user_stocks))
    
    if st.button("🚀 加载分时图表"):
        # Prepare Metadata for Names/Turnover (using latest available data)
        if not full_df.empty:
            meta_df = full_df.sort_values('日期').groupby('代码').tail(1).set_index('代码')
            meta_map = meta_df[['名称', '成交额']].to_dict('index')
        else:
            meta_map = {}

        # Construct arguments list [(code, name, turnover), ...]
        fetch_args = []
        known_indices = ["000300", "000001", "399001"]
        
        for c in target_stock_codes:
            if c in known_indices: continue # Skip if user entered index code manually
            
            info = meta_map.get(c, {})
            name = info.get('名称', c)
            to_val = info.get('成交额', 0)
            fetch_args.append((c, name, to_val))

        all_intraday = []
        progress_bar = st.progress(0)
        
        total_steps = len(target_id_dates)
        
        for i, d_str in enumerate(target_id_dates):
            # Fetch for one day
            day_data = fetch_intraday_data_v2(fetch_args, d_str)
            all_intraday.extend(day_data)
            progress_bar.progress((i + 1) / total_steps)
            
        progress_bar.empty()
        
        if all_intraday:
            # Need daily_df for turnover info (optional context)
            # Just grab the last day's daily_df for Context
            if not filtered_df.empty:
                last_daily_df = filtered_df[filtered_df['日期'] == pd.Timestamp(target_id_dates[-1])]
            else:
                 last_daily_df = pd.DataFrame(columns=['代码', '名称', '成交额'])

            
            fig_sh, fig_sz = plot_intraday_charts(all_intraday, target_id_dates, last_daily_df, selected_pool)
            
            if fig_sh: st.plotly_chart(fig_sh, use_container_width=True)
            if fig_sz: st.plotly_chart(fig_sz, use_container_width=True)
        else:
            st.warning("未获取到分时数据。可能是非交易日或接口限制。")

# Summary in Sidebar
st.sidebar.markdown("---")
st.sidebar.caption("v2.2 Modular - Refactored for Stability")
st.sidebar.caption("Data source: AkShare (Sina/Eastmoney)")
