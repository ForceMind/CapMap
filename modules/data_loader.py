import pandas as pd
import akshare as ak
import os
import streamlit as st
import concurrent.futures
import threading
from datetime import datetime, timedelta
import random
import time

from .config import STOCK_POOLS
from .utils import with_retry, get_start_date, add_script_run_ctx, get_script_run_ctx

def fetch_history_data(pool_name="沪深300 (大盘)"):
    """
    获取指定成分股过去2年的日线数据。
    逻辑复刻自 app1.py (稳定版)，支持多指数池。
    """
    config = STOCK_POOLS.get(pool_name, STOCK_POOLS["沪深300 (大盘)"])
    cache_file = config["cache"]
    index_code = config["code"]

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
            status_text.text(f"正在初始化 [{pool_name}] 历史数据...")
        else:
            status_text.text(f"正在检查增量数据 ({start_date_str} - {end_date_str})...")

        # 获取成分股列表
        status_text.text(f"正在获取 [{pool_name}] 成分股列表...")
        try:
            # 增加重试
            cons_df = with_retry(lambda: ak.index_stock_cons(symbol=index_code), retries=5, delay=2.0)
        except:
             if not cached_df.empty:
                 st.warning("成分股列表获取失败 (网络原因)，使用缓存数据")
                 return cached_df
             return pd.DataFrame()
        
        if cons_df is None or cons_df.empty:
             st.warning(f"无法获取 [{pool_name}] 成分股列表 (可能是 AkShare 接口变动或网络超时)")
             return cached_df if not cached_df.empty else pd.DataFrame()

        if 'variety' in cons_df.columns:
            code_col, name_col = 'variety', 'name'
        elif '品种代码' in cons_df.columns:
            code_col, name_col = '品种代码', '品种名称'
        else:
            code_col = cons_df.columns[0]
            name_col = cons_df.columns[1]
            
        # 强转 String
        stock_list = cons_df[code_col].apply(str).tolist() 
        stock_names = dict(zip(stock_list, cons_df[name_col]))
        
        # --- 尝试获取今日实时数据 (Spot) ---
        today_spot_map = {}
        try:
            # Low frequency
            spot_df = ak.stock_zh_a_spot_em()
            if spot_df is not None and not spot_df.empty:
                spot_df['代码'] = spot_df['代码'].astype(str)
                
                # 1. 更新名称映射
                new_names = dict(zip(spot_df['代码'], spot_df['名称']))
                stock_names.update(new_names)
                
                # 2. 准备今日数据映射
                if end_date_str >= start_date_str:
                    today_spot_map = spot_df.set_index('代码').to_dict('index')
        except Exception as e:
            # 非致命错误
            print(f"Update spots failed: {e}")

        new_data_list = []
        total_stocks = len(stock_list)

        # 循环获取历史
        def fetch_one_stock(code, name):
            try:
                # 获取日线
                df_hist = ak.stock_zh_a_hist(symbol=code, start_date=start_date_str, end_date=end_date_str, adjust="qfq")
                
                # 检查是否包含今天
                fetched_today = False
                if df_hist is not None and not df_hist.empty:
                    df_hist['日期'] = pd.to_datetime(df_hist['日期'])
                    if end_date_str in df_hist['日期'].dt.strftime("%Y%m%d").values:
                        fetched_today = True
                else:
                    df_hist = pd.DataFrame()

                # 补全今天
                if (not fetched_today) and (end_date_str == datetime.now().strftime("%Y%m%d")):
                    if code in today_spot_map:
                        row = today_spot_map[code]
                        try:
                             new_row = pd.DataFrame([{
                                 '日期': pd.to_datetime(end_date_str),
                                 '收盘': row['最新价'],
                                 '涨跌幅': row['涨跌幅'],
                                 '成交额': row['成交额'],
                                 '代码': code,
                                 '名称': name
                             }])
                             df_hist = pd.concat([df_hist, new_row], ignore_index=True)
                        except:
                            pass
                
                if df_hist is not None and not df_hist.empty:
                    # 确保列存在
                    cols_needed = ['日期', '收盘', '涨跌幅', '成交额']
                    for c in cols_needed:
                        if c not in df_hist.columns: return None
                    
                    df_hist = df_hist[cols_needed].copy()
                    df_hist['代码'] = code
                    df_hist['名称'] = name
                    return df_hist
            except Exception:
                pass
            return None

        # Use concurrency as in app1.py
        ctx = get_script_run_ctx()
        def fetch_one_stock_wrapper(code, name):
            if ctx:
                add_script_run_ctx(threading.current_thread(), ctx)
            return fetch_one_stock(code, name)

        # 恢复 app1.py 的 max_workers=10
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
             future_map = {executor.submit(fetch_one_stock_wrapper, c, stock_names.get(c, c)): c for c in stock_list}
             
             for i, future in enumerate(concurrent.futures.as_completed(future_map)):
                 # Update progress
                 if i % 10 == 0:
                     progress_bar.progress((i + 1) / total_stocks)
                     status_text.text(f"正在同步数据 [{pool_name}]: {i+1}/{total_stocks}")
                 
                 res = future.result()
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
                # 合并旧数据和新数据，并去重
                st.toast(f"📥 成功获取 {len(new_df)} 条新记录 ({pool_name})")
                final_df = pd.concat([cached_df, new_df], ignore_index=True)
                final_df.drop_duplicates(subset=['日期', '代码'], keep='last', inplace=True)
        else:
            final_df = cached_df
            
        if final_df.empty:
            return pd.DataFrame()

        final_df = final_df.sort_values('日期')
        
        # 使用最新的 stock_names 更新 DataFrame 中的名称列
        if final_df is not None and not final_df.empty:
            final_df['名称'] = final_df['代码'].map(stock_names).fillna(final_df['名称'])
        
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
    原子化获取单个标的的分时数据，独立缓存。
    """
    start_time = f"{date_str} 09:30:00"
    end_time = f"{date_str} 15:00:00"
    
    # 指数退避策略全局变量 (简单模拟)
    if not hasattr(fetch_cached_min_data, "current_backoff"):
        fetch_cached_min_data.current_backoff = 0
            
    # 简单的重试机制
    max_retries = 3
    
    for attempt in range(max_retries):
        try:
            if is_index:
                df = ak.index_zh_a_hist_min_em(symbol=symbol, period=period, start_date=start_time, end_date=end_time)
            else:
                df = ak.stock_zh_a_hist_min_em(symbol=symbol, start_date=start_time, end_date=end_time, period=period, adjust='qfq')
            
            if df is not None and not df.empty:
                # 统一列名
                if '时间' in df.columns:
                    df.rename(columns={'时间': 'time', '开盘': 'open', '收盘': 'close'}, inplace=True)
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
