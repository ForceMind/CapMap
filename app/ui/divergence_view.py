import pandas as pd
import plotly.express as px
import streamlit as st


def render_divergence_view(df, available_dates):
    st.subheader("🌊 资金偏离度分析 (Alpha Divergence)")
    st.info("💡 **逻辑说明**：计算选定周期内每只股票相对于【市场中位数】的超额涨跌幅（偏离度）。\n\n如果某只股票 **成交额巨大** 且 **向下偏离极大**，通常意味着主力资金在大举出货；反之则是主力抢筹。")

    # 1. 周期选择 (Reuse simplified logic)
    available_dates = sorted(df['日期'].dt.date.unique())
    col_d1, col_d2 = st.columns(2)
    with col_d1:
        date_range_div = st.date_input(
            "分析周期",
            value=[available_dates[-5] if len(available_dates)>5 else available_dates[0], available_dates[-1]],
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
    
    # 2. 计算区间数据
    start_date_ts = pd.Timestamp(target_dates_div[0])
    end_date_ts = pd.Timestamp(target_dates_div[-1])
    
    div_period_df = df[(df['日期'] >= start_date_ts) & (df['日期'] <= end_date_ts)].copy()
    
    # 聚合
    div_stats = []
    grouped = div_period_df.groupby('代码')
    
    for code, group in grouped:
        group = group.sort_values('日期')
        if group.empty: continue
        
        first_row = group.iloc[0]
        last_row = group.iloc[-1]
        
        try:
            # 估算区间涨幅
            s_open = first_row['收盘'] / (1 + first_row['涨跌幅']/100)
            e_close = last_row['收盘']
            cum_pct = (e_close - s_open) / s_open * 100
            total_to = group['成交额'].sum()
            
            div_stats.append({
                '代码': code,
                '名称': first_row['名称'],
                '区间涨跌幅': cum_pct,
                '区间总成交': total_to
            })
        except:
            pass
            
    div_df = pd.DataFrame(div_stats)
    if div_df.empty:
        st.stop()
        
    # 3. 计算偏离度 (Deviation)
    market_median_chg = div_df['区间涨跌幅'].median()
    div_df['偏离度'] = div_df['区间涨跌幅'] - market_median_chg
    
    # 辅助列
    div_df['成交额(亿)'] = div_df['区间总成交'] / 1e8
    
    col_m1, col_m2 = st.columns(2)
    col_m1.metric("基准(中位数)涨跌幅", f"{market_median_chg:.2f}%")
    col_m2.metric("分析样本数", f"{len(div_df)} 只")
    
    st.divider()

    # 4. 可视化 - 散点图
    # X: 成交额(Log), Y: 偏离度, Color: 偏离度
    fig_scatter = px.scatter(
        div_df,
        x='成交额(亿)',
        y='偏离度',
        color='偏离度',
        text='名称', # 显示名字
        color_continuous_scale=['#00a65a', '#ffffff', '#dd4b39'],
        range_color=[-20, 20], # 限制颜色范围避免极值
        log_x=True,
        hover_data=['代码', '区间涨跌幅'],
        title=f"资金偏离度分布图 (X轴为成交额对数)"
    )
    fig_scatter.update_traces(textposition='top center')
    fig_scatter.update_layout(height=600)
    st.plotly_chart(fig_scatter, width="stretch")
    
    # 5. 榜单
    col_list1, col_list2 = st.columns(2)
    
    with col_list1:
        st.subheader("🔥 资金抱团 (放量向上偏离)")
        # 逻辑：成交额大 & 偏离度 > 0
        # 排序：综合分 = 成交额 * 偏离度 (仅参考) 或者按成交额降序看谁在涨
        # 用户需求：找出向上偏离的。通常想看“大资金买谁”。所以按成交额降序，且偏离度>0
        
        buy_df = div_df[div_df['偏离度'] > 0].sort_values('区间总成交', ascending=False).head(20)
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
        # 逻辑：成交额大 & 偏离度 < 0
        sell_df = div_df[div_df['偏离度'] < 0].sort_values('区间总成交', ascending=False).head(20)
        
        st.dataframe(
            sell_df[['代码', '名称', '偏离度', '成交额(亿)', '区间涨跌幅']].style.format({
                '偏离度': '{:.2f}%',
                '成交额(亿)': '{:.1f}',
                '区间涨跌幅': '{:.2f}%'
            }),
            hide_index=True
        )

