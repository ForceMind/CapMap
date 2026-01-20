import pandas as pd

def calculate_deviation_data(df, target_dates):
    """
    计算资金偏离度数据
    """
    if not target_dates:
        return pd.DataFrame(), 0.0
        
    start_date_ts = pd.Timestamp(target_dates[0])
    end_date_ts = pd.Timestamp(target_dates[-1])
    
    div_period_df = df[(df['日期'] >= start_date_ts) & (df['日期'] <= end_date_ts)].copy()
    
    if div_period_df.empty:
        return pd.DataFrame(), 0.0

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
            
            # Protect against division by zero
            if s_open == 0: continue
            
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
        return pd.DataFrame(), 0.0
        
    # 计算偏离度
    market_median_chg = div_df['区间涨跌幅'].median()
    div_df['偏离度'] = div_df['区间涨跌幅'] - market_median_chg
    div_df['成交额(亿)'] = div_df['区间总成交'] / 1e8
    
    return div_df, market_median_chg

def filter_deviation_data(div_df, strategy_mode="默认"):
    """
    根据策略过滤偏离度数据
    """
    if div_df.empty:
        return div_df

    if "护盘" in strategy_mode:
        threshold_to = div_df['成交额(亿)'].quantile(0.7)
        div_df = div_df[div_df['成交额(亿)'] >= threshold_to]
        div_df = div_df[div_df['偏离度'] > 2.0]
    
    elif "游资" in strategy_mode:
        div_df = div_df[div_df['偏离度'].abs() > 5.0]
        
    elif "出货" in strategy_mode:
        threshold_to = div_df['成交额(亿)'].quantile(0.5)
        div_df = div_df[(div_df['成交额(亿)'] >= threshold_to) & (div_df['偏离度'] < -3.0)]
        
    return div_df
