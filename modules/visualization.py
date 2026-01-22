import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import pandas as pd
import numpy as np

def plot_market_heatmap(daily_df):
    """
    绘制市场全景热力图
    """
    max_limit = 7
    min_limit = -7
    
    fig = px.treemap(
        daily_df,
        path=['名称'],
        values='成交额', 
        color='涨跌幅',
        color_continuous_scale=['#00a65a', '#ffffff', '#dd4b39'], # 绿 -> 白 -> 红
        range_color=[min_limit, max_limit],
        hover_data={
            '名称': True,
            '代码': True,
            '收盘': True,
            '涨跌幅': ':.2f',
            '成交额': True
        },
        height=650
    )
    
    fig.update_traces(
        textinfo="label+value+percent entry",
        hovertemplate="<b>%{label}</b><br>收盘价: %{customdata[2]}<br>涨跌幅: %{color:.2f}%<br>成交额: %{value:.2s}"
    )
    fig.update_layout(
        margin=dict(t=10, l=10, r=10, b=10),
        coloraxis_colorbar=dict(title="涨跌幅(%)")
    )
    
    return fig

def plot_deviation_scatter(div_df, strategy_mode):
    """
    绘制资金偏离度散点图
    """
    if div_df.empty:
        return None
        
    fig_scatter = px.scatter(
        div_df,
        x='成交额(亿)',
        y='偏离度',
        color='偏离度',
        text='名称',
        color_continuous_scale=['#00a65a', '#ffffff', '#dd4b39'],
        log_x=True,
        hover_data=['代码', '区间涨跌幅'],
        title=f"资金偏离度分布图 (X轴为成交额对数) - {strategy_mode}"
    )
    fig_scatter.update_traces(textposition='top center')
    fig_scatter.update_layout(height=600)
    return fig_scatter

def plot_intraday_charts(all_intraday_data, days_list, daily_df, chart_mode):
    """
    绘制分时叠加图
    """
    combined_series = {}
    
    for item in all_intraday_data:
        code = item['code']
        if code not in combined_series:
            to_val = 0
            if not item.get('is_index'):
                matches = daily_df[daily_df['代码'] == code]
                if not matches.empty:
                    to_val = matches.iloc[0]['成交额']
            
            combined_series[code] = {
                'name': item['name'],
                'code': code,
                'is_index': item.get('is_index', False),
                'turnover': to_val, 
                'dfs': []
            }
        combined_series[code]['dfs'].append(item['data']) # Ensure item['data'] has been pre-processed (columns: time, pct_chg, close, date_col)
    
    x_tick_vals = []
    x_tick_text = []

    # Prepare X Axis
    for i, d_str in enumerate(days_list):
        base_x = i * (240 + 20)
        day_label = d_str[5:]
        
        if len(days_list) > 1:
            x_tick_vals.append(base_x + 120) 
            x_tick_text.append(day_label)
        else:
            x_tick_vals.append(base_x)
            x_tick_text.append(f"{day_label}\n09:30")
            x_tick_vals.append(base_x + 120)
            x_tick_text.append("11:30/13:00")
            x_tick_vals.append(base_x + 240)
            x_tick_text.append("15:00")

    # Process data for plotting
    for code, info in combined_series.items():
        if not info['dfs']: continue
        try:
            full_df = pd.concat(info['dfs']).sort_values(['date_col', 'time'])
        except:
             continue
             
        full_df['time_str'] = full_df['time'].dt.strftime("%H:%M:%S")
        x_values = []
        
        for idx, row in full_df.iterrows():
            d_str = row['date_col']
            day_idx = days_list.index(d_str) if d_str in days_list else 0
            t_obj = row['time']
            mins_from_midnight = t_obj.hour * 60 + t_obj.minute
            
            if mins_from_midnight <= 690:
                offset = mins_from_midnight - 570
            else:
                offset = 120 + (mins_from_midnight - 780)
                
            final_x = day_idx * (240 + 20) + offset
            x_values.append(final_x)
            
        full_df['x_int'] = x_values
        base_price = full_df['close'].iloc[0]
        full_df['cumulative_pct'] = (full_df['close'] - base_price) / base_price * 100
        info['plot_data'] = full_df

    # Split into SH/SZ
    idx_data = [v for k,v in combined_series.items() if v['is_index']]
    stk_data = [v for k,v in combined_series.items() if not v['is_index']]
    
    sh_stocks = [v for v in stk_data if v['code'].startswith('6')]
    sz_stocks = [v for v in stk_data if not v['code'].startswith('6')]
    
    sh_index = [v for v in idx_data if v['code'] in ['000001', '000300']]
    sz_index = [v for v in idx_data if v['code'] in ['399001', '000300']]

    def _create_fig(stocks, indices, title_suffix):
        fig = go.Figure()
        
        # Stocks
        if stocks:
            max_to = max([s['turnover'] for s in stocks]) if stocks else 1
            min_to = min([s['turnover'] for s in stocks]) if stocks else 0
            color_palette = px.colors.qualitative.Alphabet + px.colors.qualitative.Dark24
            
            for i, s in enumerate(stocks):
                if 'plot_data' not in s: continue
                
                if max_to == min_to: width=2
                else: width = 1 + 3*(s['turnover'] - min_to)/(max_to - min_to)
                
                df_p = s['plot_data']
                color = color_palette[i % len(color_palette)]
                
                fig.add_trace(go.Scatter(
                    x=df_p['x_int'],
                    y=df_p['cumulative_pct'],
                    mode='lines',
                    name=s['name'],
                    line=dict(width=max(1.5, width), color=color),
                    hovertemplate=f"<b>{s['name']}</b><br>涨跌: %{{y:.2f}}%<br>时间: %{{customdata}}",
                    customdata=df_p['date_col'] + ' ' + df_p['time_str']
                ))

        # Indices
        idx_colors = {'000300': 'black', '000001': '#d62728', '399001': '#1f77b4'}
        for idx in indices:
            if 'plot_data' not in idx: continue
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

        # Dividers
        if len(days_list) > 1:
            for i in range(1, len(days_list)):
                boundary = i * (240 + 20) - 10
                fig.add_vline(x=boundary, line_width=1, line_dash="dash", line_color="gray")

        fig.update_layout(
            title=f"分时走势叠加 ({'多日拼接' if len(days_list)>1 else days_list[0]}) - {title_suffix}",
            xaxis=dict(
                tickmode='array',
                tickvals=x_tick_vals,
                ticktext=x_tick_text,
                showgrid=True,
                showspikes=True,
                spikemode='across',
                spikesnap='cursor',
                showline=True, 
                linewidth=1, 
                linecolor='black',
                mirror=True
            ),
            yaxis=dict(showspikes=True),
            yaxis_title="累计涨跌幅 (%)",
            hovermode="x unified",
            height=700,
            legend=dict(orientation="h", y=1.02, x=1, xanchor='right'),
            hoverlabel=dict(namelength=-1)
        )
        return fig

    fig_sh = _create_fig(sh_stocks, sh_index, f"沪市 - {chart_mode}")
    fig_sz = _create_fig(sz_stocks, sz_index, f"深市 - {chart_mode}")
    
    return fig_sh, fig_sz
