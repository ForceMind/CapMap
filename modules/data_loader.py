import pandas as pd
import tushare as ts
import akshare as ak  # 保留 akshare 作为备份或辅助? 或者完全移除? 用户说"更换"，我应尽量使用 Tushare
import os
import streamlit as st
import concurrent.futures
import threading
from datetime import datetime, timedelta
import random
import time

from .config import STOCK_POOLS, TUSHARE_TOKEN
from .utils import with_retry, get_start_date, add_script_run_ctx, get_script_run_ctx

# 初始化 Tushare
if TUSHARE_TOKEN == "YOUR_TUSHARE_TOKEN_HERE":
    st.warning("⚠️ 请在 modules/config.py 中设置有效的 Tushare Token，否则无法获取数据。")
else:
    try:
        ts.set_token(TUSHARE_TOKEN)
        pro = ts.pro_api()
    except Exception as e:
        st.error(f"Tushare 初始化失败: {e}")
        pro = None

def fetch_history_data(pool_name="沪深300 (大盘)", limit=None):
    """
    获取指定成分股过去2年的日线数据 (Tushare版)。
    """
    config = STOCK_POOLS.get(pool_name, STOCK_POOLS["沪深300 (大盘)"])
    cache_file = config["cache"]
    # 优先尝试使用 code (Tushare格式), 如果没有则回退到 ak_code for display, actually we need Tushare index code
    index_code = config.get("code", "399300.SZ")

    cached_df = pd.DataFrame()
    last_cached_date = None

    # 1. 尝试加载本地缓存
    if os.path.exists(cache_file):
        try:
            cached_df = pd.read_parquet(cache_file)
            if not cached_df.empty:
                last_cached_date = cached_df['日期'].max().date()
                st.toast(f"✅ 已加载本地缓存 [{pool_name}]，最新日期: {last_cached_date}")
        except Exception as e:
            st.error(f"读取缓存文件失败: {e}")

    # 2. 计算需要下载的时间范围
    today = datetime.now().date()
    
    if last_cached_date:
        if last_cached_date >= today:
             return cached_df
        start_date_str = (last_cached_date + timedelta(days=1)).strftime("%Y%m%d")
    else:
        start_date_str = get_start_date(2)
        
    end_date_str = today.strftime("%Y%m%d")

    # 如果不需要更新
    if start_date_str > end_date_str:
        return cached_df

    # 状态容器
    status_text = st.empty()
    progress_bar = st.progress(0)
    
    try:
        # 如果是增量更新
        is_incremental = not cached_df.empty
        if not is_incremental:
            status_text.text(f"正在初始化 [{pool_name}] 历史数据 (Tushare)...")
        else:
            status_text.text(f"正在检查增量数据 ({start_date_str} - {end_date_str})...")

        if pro is None:
             st.error("Tushare 未初始化，请检查 Token")
             return cached_df

        # 获取成分股列表
        status_text.text(f"正在获取 [{pool_name}] 成分股列表...")
        
        stock_list_data = [] # List of dict: {'code': '000001', 'name': '平安银行', 'ts_code': '000001.SZ'}
        
        # 优先尝试 Tushare
        try:
            # Check rate limit or points issues
            cons_df = with_retry(lambda: pro.index_member(index_code=index_code), retries=2, delay=2.0)
            if cons_df is not None and not cons_df.empty:
                # cons_df columns: index_code, con_code, con_name
                # Note: valid columns might depend on permissions.
                for _, row in cons_df.iterrows():
                    ts_c = row['con_code']
                    nm = row['con_name'] if 'con_name' in row else ts_c
                    # symbol is code without suffix
                    sym = ts_c.split('.')[0]
                    stock_list_data.append({'code': sym, 'name': nm, 'ts_code': ts_c})
        except Exception as e:
            print(f"Tushare index_member failed: {e}")
            cons_df = pd.DataFrame()

        # Fallback to AkShare if Tushare failed or returned empty
        if not stock_list_data:
            st.warning("Tushare 获取成分股失败，尝试使用 AkShare 作为备用元数据源...")
            try:
                ak_code = config.get("ak_code", "000300")
                cons_df_ak = with_retry(lambda: ak.index_stock_cons(symbol=ak_code), retries=3, delay=2.0)
                if cons_df_ak is not None and not cons_df_ak.empty:
                    # AkShare standardizes: 品种代码, 品种名称
                    if 'variety' in cons_df_ak.columns:
                        c_col, n_col = 'variety', 'name'
                    elif '品种代码' in cons_df_ak.columns:
                        c_col, n_col = '品种代码', '品种名称'
                    else:
                        c_col, n_col = cons_df_ak.columns[0], cons_df_ak.columns[1]
                    
                    for _, row in cons_df_ak.iterrows():
                        sym = str(row[c_col])
                        nm = row[n_col]
                        # generate ts_code
                        if sym.startswith('6'): ts_c = f"{sym}.SH"
                        elif sym.startswith('8'): ts_c = f"{sym}.BJ"
                        else: ts_c = f"{sym}.SZ"
                        
                        stock_list_data.append({'code': sym, 'name': nm, 'ts_code': ts_c})
                        
            except Exception as e:
                 st.error(f"无法获取成分股列表 (Tushare & AkShare failed): {e}")
                 return cached_df if not cached_df.empty else pd.DataFrame()
        
        if not stock_list_data:
             return cached_df if not cached_df.empty else pd.DataFrame()

        stock_map = {item['ts_code']: item for item in stock_list_data}
        stock_list = [item['ts_code'] for item in stock_list_data]
        if limit:
            stock_list = stock_list[:limit]
        total_stocks = len(stock_list)


        new_data_list = []
        total_stocks = len(stock_list)

        # 循环获取历史
        def fetch_one_stock(ts_code):
            try:
                # 获取日线
                df_hist = ts.pro_bar(ts_code=ts_code, adj='qfq', start_date=start_date_str, end_date=end_date_str)
                
                if df_hist is not None and not df_hist.empty:
                    # Rename columns
                    df_hist = df_hist.rename(columns={
                        'trade_date': '日期',
                        'close': '收盘',
                        'pct_chg': '涨跌幅',
                        'amount': '成交额'
                    })
                    
                    df_hist['日期'] = pd.to_datetime(df_hist['日期'])
                    # Tushare amount is '千元', Akshare was '元'. Multiply by 1000
                    df_hist['成交额'] = df_hist['成交额'] * 1000
                    
                    cols_needed = ['日期', '收盘', '涨跌幅', '成交额']
                    df_hist = df_hist[cols_needed].copy()
                    
                    # sym = symbol_map.get(ts_code, ts_code.split('.')[0])
                    # nm = name_map.get(ts_code, sym)
                    info = stock_map.get(ts_code, {})
                    sym = info.get('code', ts_code.split('.')[0])
                    nm = info.get('name', sym)
                    
                    df_hist['代码'] = sym
                    df_hist['名称'] = nm
                    
                    return df_hist

            except Exception:
                pass
            return None

        # Serial execution to respect Tushare rate limits (50 req/min => 1.2s delay)
        for i, ts_code in enumerate(stock_list):
             if i % 5 == 0:
                 progress_bar.progress((i + 1) / total_stocks)
                 status_text.text(f"正在同步数据 [{pool_name}]: {i+1}/{total_stocks} (Tushare限速 fetching...)")
             
             # Enforce rate limit
             time.sleep(1.25)
             
             res = fetch_one_stock(ts_code)
             if res is not None:
                 new_data_list.append(res)
                
        status_text.empty()
        progress_bar.empty()
        
        # 合并逻辑
        if new_data_list:
            new_df = pd.concat(new_data_list, ignore_index=True)
            # 类型转换
            new_df['日期'] = pd.to_datetime(new_df['日期'])
            new_df['涨跌幅'] = pd.to_numeric(new_df['涨跌幅'], errors='coerce')
            new_df['成交额'] = pd.to_numeric(new_df['成交额'], errors='coerce')
            new_df['收盘'] = pd.to_numeric(new_df['收盘'], errors='coerce')
            
            if cached_df.empty:
                final_df = new_df
            else:
                st.toast(f"📥 成功获取 {len(new_df)} 条新记录 ({pool_name})")
                final_df = pd.concat([cached_df, new_df], ignore_index=True)
                final_df.drop_duplicates(subset=['日期', '代码'], keep='last', inplace=True)
        else:
            final_df = cached_df
            
        if final_df.empty:
            return pd.DataFrame()

        final_df = final_df.sort_values('日期')
        
        # 使用最新的 stock_names 更新 DataFrame 中的名称列
        # Map symbol -> name
        final_stock_names = {item['code']: item['name'] for item in stock_list_data}
        if final_df is not None and not final_df.empty:
            final_df['名称'] = final_df['代码'].map(final_stock_names).fillna(final_df['名称'])
        
        # 保存缓存
        if new_data_list or cached_df.empty:
            try:
                if not os.path.exists("data"):
                    os.makedirs("data")
                final_df.to_parquet(cache_file)
                if not cached_df.empty:
                    st.toast(f"💾 [{pool_name}] 增量数据已合并并保存")
                else:
                    st.success(f"💾 [{pool_name}] 全量数据已初始化")
            except Exception as e:
                st.warning(f"无法保存缓存: {e}")

        return final_df

    except Exception as e:
        status_text.empty()
        progress_bar.empty()
        st.error(f"全局数据错误: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600*24, show_spinner=False)
def fetch_cached_min_data(symbol, date_str, is_index=False, period='1'):
    """
    原子化获取单个标的的分时数据 (Tushare版)，独立缓存。
    """
    start_time = f"{date_str} 09:30:00"
    end_time = f"{date_str} 15:00:00"
    
    # 简单的重试机制
    max_retries = 3

    # Helper for ts_code
    def get_ts_code(sym, is_idx):
        if is_idx:
            if sym == "000300": return "399300.SZ"
            if sym == "000001": return "000001.SH"
            if sym == "399001": return "399001.SZ"
            return sym
        else:
            if sym.startswith('6'): return f"{sym}.SH"
            if sym.startswith('0') or sym.startswith('3'): return f"{sym}.SZ"
            if sym.startswith('8'): return f"{sym}.BJ"
            return f"{sym}.SH"

    ts_code = get_ts_code(symbol, is_index)
    
    for attempt in range(max_retries):
        try:
            # period '1' -> '1min'
            freq = '1min' if period == '1' else period
            
            # ts.pro_bar handles min data via 'ft_mins' or 'stk_mins'
            # Requires start_date and end_date as strings.
            # Tushare needs date string for pro_bar if it's daily, 
            # but for minutes, it might vary.
            # Using pro_bar is safest wrapper.
            
            # Warning: Tushare min data consumes points.
            df = ts.pro_bar(ts_code=ts_code, freq=freq, start_date=start_time, end_date=end_time)
            
            # If fail (e.g. Rate Limit 2/min or No Points), fallback to AkShare
            if df is None or df.empty:
                 try:
                     parts = ts_code.split('.')
                     code_val = parts[0]
                     suffix = parts[1] if len(parts) > 1 else 'SZ'
                     prefix = 'sz' if suffix == 'SZ' else 'sh' if suffix == 'SH' else 'bj'
                     
                     df_ak = pd.DataFrame()
                     if is_index:
                        symbol_ak = f"{prefix}{code_val}"
                        df_ak = ak.index_zh_a_hist_min_em(symbol=symbol_ak, period=period)
                        if not df_ak.empty:
                            df_ak.rename(columns={'时间': 'time', '开盘': 'open', '收盘': 'close', '最高': 'high', '最低': 'low', '成交量': 'vol'}, inplace=True)
                     else:
                        symbol_ak = f"{prefix}{code_val}"
                        df_ak = ak.stock_zh_a_minute(symbol=symbol_ak, period=period, adjust='qfq')
                        if not df_ak.empty:
                            df_ak.rename(columns={'day': 'time'}, inplace=True)
                            
                     if not df_ak.empty:
                         # Filter for the specific date
                         df_ak['time'] = pd.to_datetime(df_ak['time'])
                         mask = (df_ak['time'] >= pd.to_datetime(start_time)) & (df_ak['time'] <= pd.to_datetime(end_time))
                         df = df_ak.loc[mask].copy()
                 except Exception as e:
                     # print(f"AkShare fallback failed: {e}")
                     pass
            
            if df is not None and not df.empty:
                # Rename columns
                # Tushare: trade_time, open, close, high, low, vol, amount
                # Akshare: 时间, 开盘, 收盘, 最高, 最低, 成交量, 成交额
                
                # App expects: 'time', 'open', 'close'
                if 'trade_time' in df.columns:
                    df.rename(columns={'trade_time': 'time', 'open': 'open', 'close': 'close'}, inplace=True)
                
                # Check column mapping
                if 'time' not in df.columns: 
                    # fallback if Tushare returns trade_date and trade_time split?
                    pass
                
                # Sort by time
                df = df.sort_values('time')
                return df
                
        except Exception:
            time.sleep(1)
            
    return pd.DataFrame()

# --- 后台预取线程逻辑 ---
@st.cache_data(ttl=3600*24, show_spinner=False)
def fetch_cached_min_data_wrapper(symbol, date_str, is_index=False, period='1'):
    """Wrapper to be called by background thread"""
    # This is just a direct call to the cached function
    # In background thread, we can call this.
    return fetch_cached_min_data(symbol, date_str, is_index, period)

def background_prefetch_task(date_list, origin_df):
    """
    后台线程：执行数据预取。
    """
    total_dates = len(date_list)
    print(f"\n[后台任务] 开始预取 {total_dates} 天的数据。")
    
    current_backoff = 0 # 秒
    
    indices_codes = ["000300", "000001", "399001"]
    
    for i, d in enumerate(date_list):
        d_str = d.strftime("%Y-%m-%d")
        print(f"[后台任务] 正在处理: {d_str} ({i+1}/{total_dates})")
        
        # 筛选
        daily = origin_df[origin_df['日期'].dt.date == d]
        if daily.empty: continue
        
        # Top 25
        top_stocks = daily.sort_values('成交额', ascending=False).head(25)['代码'].tolist()
        
        # 任务列表
        tasks = []
        for code in indices_codes: tasks.append((code, d_str, True))
        for code in top_stocks: tasks.append((code, d_str, False))
        
        # 内层逐个执行 (为了方便控制退避，且后台任务不急于一时的并发，稳定第一)
        for t_code, t_date, t_is_index in tasks:
            
            # Indefinite retry loop with backoff
            while True:
                try:
                    # 检查退避
                    if current_backoff > 0:
                        print(f"[后台任务] 处于冷却状态。等待 {current_backoff} 秒...")
                        time.sleep(current_backoff)
                        
                    fetch_cached_min_data(t_code, t_date, is_index=t_is_index, period='1')
                    
                    # Success
                    if current_backoff > 0:
                        print(f"[后台任务] 已恢复。重置退避时间。")
                        current_backoff = 0
                    
                    time.sleep(0.1)
                    break # 跳出 while，处理下一个 task

                except Exception as e:
                    print(f"[后台任务] 获取 {t_code} ({t_date}) 失败: {e}")
                    # 触发退避机制
                    if current_backoff == 0:
                        current_backoff = 60
                    else:
                        current_backoff *= 2
                    
                    print(f"[后台任务] 退避时间增加到 {current_backoff}秒。正在重试同一任务...")
    
    print("[后台任务] 所有任务已完成。")


def fetch_intraday_data_v2(stock_codes, target_date_str, period='1'):
    """
    获取指定股票列表 + 三大指数 的分钟级数据 (并发版)。
    """
    results = [] 
    
    # 定义需要获取的指数
    indices_map = {
        "000300": "📊 沪深300",
        "000001": "📈 上证指数",
        "399001": "📉 深证成指"
    }

    # 任务列表
    tasks = []

    # 1. 提交指数任务
    for idx_code, idx_name in indices_map.items():
        tasks.append({
            'type': 'index',
            'code': idx_code,
            'name': idx_name,
            'to_val': 99999999999
        })

    # 2. 提交个股任务
    for code, name, to_val in stock_codes:
        tasks.append({
            'type': 'stock',
            'code': code,
            'name': name,
            'to_val': to_val
        })
        
    def _worker(task):
        try:
            is_index = (task['type'] == 'index')
            data = fetch_cached_min_data(task['code'], target_date_str, is_index=is_index, period=period)
            if data is not None:
                return {
                    'code': task['code'],
                    'name': task['name'],
                    'data': data,
                    'turnover': task['to_val'],
                    'is_index': is_index
                }
        except Exception:
            pass
        return None

    # 并发执行
    ctx = get_script_run_ctx()
    def _worker_wrapper(t):
        if ctx:
            add_script_run_ctx(threading.current_thread(), ctx)
        return _worker(t)

    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
        future_to_task = {executor.submit(_worker_wrapper, t): t for t in tasks}
        
        for future in concurrent.futures.as_completed(future_to_task):
            res = future.result()
            if res:
                results.append(res)
            
    return results
