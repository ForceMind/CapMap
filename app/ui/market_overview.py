import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

def calculate_market_breadth(df):
    """
    计算基于日线的市场广度指标
    """
    if df.empty:
        return None

    # 按日期分组计算每一天的指标
    dates = sorted(df['日期'].unique())
    breadth_data = []

    for d in dates:
        day_df = df[df['日期'] == d]
        total_stocks = len(day_df)
        if total_stocks == 0:
            continue
        
        # 涨跌家数
        up_count = len(day_df[day_df['涨跌幅'] > 0])
        down_count = len(day_df[day_df['涨跌幅'] < 0])
        flat_count = total_stocks - up_count - down_count
        
        # 涨停/跌停 (近似处理: 涨跌幅 > 9.8% 或 < -9.8%)
        limit_up = len(day_df[day_df['涨跌幅'] > 9.8])
        limit_down = len(day_df[day_df['涨跌幅'] < -9.8])
        
        # 简单平均涨跌幅
        avg_change = day_df['涨跌幅'].mean()
        
        # 中位数涨跌幅
        median_change = day_df['涨跌幅'].median()

        breadth_data.append({
            'date': d,
            'total': total_stocks,
            'up': up_count,
            'down': down_count,
            'flat': flat_count,
            'limit_up': limit_up,
            'limit_down': limit_down,
            'avg_change': avg_change,
            'median_change': median_change,
            # ADL (Advance-Decline Line) 每日净值 = 涨家 - 跌家
            'net_advances': up_count - down_count
        })
    
    breadth_df = pd.DataFrame(breadth_data)
    
    # 计算 ADL 累积值
    breadth_df['adl'] = breadth_df['net_advances'].cumsum()
    
    return breadth_df

def calculate_ma_stats(df):
    """
    计算均线站上比例 (需要按股票分组计算过去 N 天数据，比较耗时，这里简化只计算最近 T 天)
    警告：此操作在 Streamlit 中可能较慢，建议只取最近 60 个交易日
    """
    # 这里我们只取每个股票的最后一天数据来做"当前状态"的仪表盘
    # 如果要做历史趋势图，需要更复杂的滚动计算
    
    latest_date = df['日期'].max()
    latest_df = df[df['日期'] == latest_date].copy()
    
    # 简单的“单日强弱”判断：收盘价在当日均价之上
    # 由于只有日线数据 (收盘、开盘、高、低)，没有均价，可以用 (Open+Close)/2 近似
    latest_df['strong'] = latest_df['收盘'] > (latest_df['开盘'] + latest_df['收盘']) / 2
    
    return latest_df

def render_market_overview(df):
    st.header("📈 市场概览 (Market Overview)")
    
    if df.empty:
        st.warning("无数据")
        return

    # 1. 基础数据准备
    # 为了性能，基于已有的 df (所有股票近2年数据) 计算每日聚合指标
    with st.spinner("正在计算市场广度指标..."):
        breadth_df = calculate_market_breadth(df)
    
    last_day = breadth_df.iloc[-1]
    prev_day = breadth_df.iloc[-2] if len(breadth_df) > 1 else last_day
    
    # 2. 核心 KPI 仪表盘
    st.markdown("### 🔥 当日情绪指标")
    kpi1, kpi2, kpi3, kpi4 = st.columns(4)
    
    kpi1.metric("上涨家数", f"{int(last_day['up'])}", f"{int(last_day['up'] - prev_day['up'])}")
    kpi2.metric("下跌家数", f"{int(last_day['down'])}", f"{int(last_day['down'] - prev_day['down'])}", delta_color="inverse")
    kpi3.metric("涨停(>9.8%)", f"{int(last_day['limit_up'])}", f"{int(last_day['limit_up'] - prev_day['limit_up'])}")
    kpi4.metric("中位数涨跌", f"{last_day['median_change']:.2f}%", f"{last_day['median_change'] - prev_day['median_change']:.2f}%")

    # 3. 市场广度历史走势 (ADL & 涨跌分布)
    st.markdown("### 📊 腾落指标 (ADL) 与 涨跌分布")
    
    tab1, tab2 = st.tabs(["腾落线 (ADL)", "每日涨跌家数"])
    
    with tab1:
        st.caption("腾落线 (Advance-Decline Line)：反映市场内部上涨力量的累积。指数上涨但ADL下降，预示背离风险。")
        fig_adl = px.line(breadth_df, x='date', y='adl', title="全市场腾落线 (ADL)")
        fig_adl.update_layout(xaxis_title="", yaxis_title="ADL 值")
        st.plotly_chart(fig_adl, use_container_width=True)
        
    with tab2:
        # 堆叠柱状图显示上涨/下跌/平盘
        fig_count = go.Figure()
        fig_count.add_trace(go.Bar(x=breadth_df['date'], y=breadth_df['up'], name='上涨', marker_color='#fe4444'))
        fig_count.add_trace(go.Bar(x=breadth_df['date'], y=breadth_df['flat'], name='平盘', marker_color='#999999'))
        fig_count.add_trace(go.Bar(x=breadth_df['date'], y=-breadth_df['down'], name='下跌', marker_color='#00aa30')) # 下跌用负数显示在下方
        
        fig_count.update_layout(barmode='relative', title="每日涨跌家数分布 (红涨绿跌)", xaxis_title="", yaxis_title="家数")
        st.plotly_chart(fig_count, use_container_width=True)

    # 4. 赚钱效应 (平均/中位数涨跌幅)
    st.markdown("### 💰 赚钱效应 (平均 vs 中位数)")
    st.caption("由于指数常被在大盘股绑架，中位数涨跌幅更能代表大部分股票的真实表现。")
    
    fig_effect = go.Figure()
    fig_effect.add_trace(go.Scatter(x=breadth_df['date'], y=breadth_df['avg_change'], name='平均涨跌幅', line=dict(color='orange', width=1)))
    fig_effect.add_trace(go.Scatter(x=breadth_df['date'], y=breadth_df['median_change'], name='中位数涨跌幅', line=dict(color='purple', width=2)))
    
    # 增加零轴线
    fig_effect.add_hline(y=0, line_dash="dash", line_color="gray")
    
    fig_effect.update_layout(title="市场平均 vs 中位数涨跌幅趋势", xaxis_title="", yaxis_title="涨跌幅 (%)")
    st.plotly_chart(fig_effect, use_container_width=True)

