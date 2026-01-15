import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import akshare as ak
from datetime import datetime, timedelta
import time
import random
import os
import concurrent.futures
import threading
import sys

# 配置页面信息
st.set_page_config(
    page_title="A股历史盘面回放系统",
    page_icon="⏪",
    layout="wide"
)

# -----------------------------------------------------------------------------
# 1. 核心数据逻辑
# -----------------------------------------------------------------------------

CACHE_FILE = "data/csi300_history_cache.parquet"

def get_start_date(years_back=2):
    """计算 N 年前的日期，返回 YYYYMMDD 字符串"""
    target = datetime.now() - timedelta(days=365 * years_back)
    return target.strftime("%Y%m%d")

def fetch_history_data():
    """
    获取沪深300成分股过去2年的日线数据。
    增量更新逻辑：
    1. 尝试读取本地缓存。
    2. 如果有缓存，检查缓存中最新的日期。
    3. 如果 最新日期 < 昨天 (或今天收盘后)，则只下载增量数据（为了简单可靠，AkShare日线接口通常是按段下载，或者全量下载）。
       * 修正策略：由于 ak.stock_zh_a_hist 接口参数是 start_date 和 end_date，
         我们可以只下载 [缓存最新日期+1, 今天] 的数据，然后 append 到缓存中。
    """
    cached_df = pd.DataFrame()
    last_cached_date = None

    # 1. 尝试加载本地缓存
    if os.path.exists(CACHE_FILE):
        try:
            cached_df = pd.read_parquet(CACHE_FILE)
            if not cached_df.empty:
                last_cached_date = cached_df['日期'].max().date()
                st.toast(f"✅ 已加载本地缓存，最新日期: {last_cached_date}")
        except Exception as e:
            st.error(f"读取缓存文件失败: {e}")

    # 2. 计算需要下载的时间范围
    today = datetime.now().date()
    
    # 如果缓存里的日期已经是今天，且现在是盘中，可能用户想刷新
    # 但简单起见，我们设定：如果缓存最新日期 < 今天，肯定要尝试下载。
    # 如果缓存最新日期 == 今天，只有当强制刷新时才通过(外部控制)，这里函数内部先假设"已是最新"
    # 但为了支持盘中刷新，如果 last_cached_date == today，我们其实可以重拉今天的。
    # 这里我们只处理 last_cached_date < today 的自动增量, 或者 force refresh (caller clears cache)
    
    if last_cached_date:
        if last_cached_date >= today:
             # 如果已经有今天的数据，暂时直接返回 (用户需点击强制刷新来更新今日盘中数据)
             # 但为了能够"自动"拉取盘中，如果 last_cached_date == today，我们做个判断？
             # 现在的逻辑是：如果缓存文件存在且日期>=今天，就不动了。
             # 这导致如果早上9点跑了一次（有数据），下午3点再跑，还是旧的。
             # 改进：如果是今天，且现在还没收盘，或者刚收盘，允许覆盖？
             # 暂保留原逻辑防止频繁请求，依靠 "强制刷新" 按钮来清空缓存。
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
            status_text.text("正在初始化全量历史数据...")
        else:
            status_text.text(f"正在检查增量数据 ({start_date_str} - {end_date_str})...")

        # 获取成分股列表
        try:
            cons_df = ak.index_stock_cons(symbol="000300")
        except:
             if not cached_df.empty:
                 st.warning("成分股列表获取失败，使用缓存数据")
                 return cached_df
             return pd.DataFrame()
        
        if cons_df is None or cons_df.empty:
             return cached_df if not cached_df.empty else pd.DataFrame()

        if 'variety' in cons_df.columns:
            code_col, name_col = 'variety', 'name'
        elif '品种代码' in cons_df.columns:
            code_col, name_col = '品种代码', '品种名称'
        else:
            code_col = cons_df.columns[0]
            name_col = cons_df.columns[1]
            
        stock_list = cons_df[code_col].tolist()
        stock_names = dict(zip(cons_df[code_col], cons_df[name_col]))
        
        new_data_list = []
        total_stocks = len(stock_list)
        
        # --- 尝试获取今日实时数据 (Spot) 作为补充 ---
        # 很多时候 stock_zh_a_hist 在盘中不返回当日数据，或者有些源不返回。
        # 我们可以拉取 ak.stock_zh_a_spot_em() 获取所有A股实时行情，然后过滤出 CSI300
        # 仅当我们需要 "今天" 的数据时 (start_date_str <= today_str)
        today_spot_map = {}
        has_today_hist = False # 标记是否通过 hist 接口拿到了今天数据
        
        if end_date_str >= start_date_str:
             try:
                 spot_df = ak.stock_zh_a_spot_em()
                 if spot_df is not None and not spot_df.empty:
                     # spot_df columns: 代码, 名称, 最新价, 涨跌幅, 成交额 ...
                     # 建立映射: code -> row
                     spot_df['代码'] = spot_df['代码'].astype(str)
                     today_spot_map = spot_df.set_index('代码').to_dict('index')
             except Exception as e:
                 print(f"Spot fetch failed: {e}")

        # 循环获取历史
        # 使用 ThreadPoolExecutor 加速增量历史下载 (如果需要下载很多天)
        # 但 akshare 接口频繁调用可能受限，适度并发
        
        def fetch_one_stock(code, name):
            try:
                # 获取日线
                df_hist = ak.stock_zh_a_hist(symbol=code, start_date=start_date_str, end_date=end_date_str, adjust="qfq")
                
                # 检查是否包含今天
                # 如果 df_hist 不包含今天，但我们有 today_spot_map，则人工补一行
                fetched_today = False
                if df_hist is not None and not df_hist.empty:
                    df_hist['日期'] = pd.to_datetime(df_hist['日期'])
                    if end_date_str in df_hist['日期'].dt.strftime("%Y%m%d").values:
                        fetched_today = True
                else:
                    df_hist = pd.DataFrame()

                # 如果没有拉到今天的数据，且我们需要今天 (end_date_str == today)，补全
                if (not fetched_today) and (end_date_str == datetime.now().strftime("%Y%m%d")):
                    if code in today_spot_map:
                        row = today_spot_map[code]
                        # 构造一行
                        # 必须字段: 日期, 收盘, 涨跌幅, 成交额, 代码, 名称
                        # spot row keys: '最新价', '涨跌幅', '成交额'
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
                    if '日期' not in df_hist.columns: return None
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

        # 如果是增量只差1天，其实单线程也快。如果是初始化，并发。
        # Use concurrency
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
             future_map = {executor.submit(fetch_one_stock, c, stock_names.get(c, c)): c for c in stock_list}
             
             for i, future in enumerate(concurrent.futures.as_completed(future_map)):
                 # Update progress
                 if i % 10 == 0:
                     progress_bar.progress((i + 1) / total_stocks)
                     status_text.text(f"正在同步数据: {i+1}/{total_stocks}")
                 
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
                st.toast(f"📥 成功获取 {len(new_df)} 条新记录")
                final_df = pd.concat([cached_df, new_df], ignore_index=True)
                # 按 '日期' + '代码' 去重，保留新的（如果重叠）
                final_df.drop_duplicates(subset=['日期', '代码'], keep='last', inplace=True)
        else:
            # 没下载到新数据（可能是假期）
            final_df = cached_df
            
        if final_df.empty:
            return pd.DataFrame()

        final_df = final_df.sort_values('日期')
        
        # 只有当有新数据 或者 是首次下载时，才保存
        if new_data_list or cached_df.empty:
            try:
                if not os.path.exists("data"):
                    os.makedirs("data")
                final_df.to_parquet(CACHE_FILE)
                if not cached_df.empty:
                    st.toast("💾 增量数据已合并并保存")
                else:
                    st.success("💾 全量数据已初始化")
            except Exception as e:
                st.warning(f"无法保存缓存: {e}")

        return final_df

    except Exception as e:
        st.error(f"全局数据错误: {e}")
        status_text.empty()
        progress_bar.empty()
        return pd.DataFrame()

# -----------------------------------------------------------------------------
@st.cache_data(ttl=3600*24)
def fetch_cached_min_data(symbol, date_str, is_index=False, period='1'):
    """
    原子化获取单个标的的分时数据，独立缓存。
    避免因股票列表组合变化导致整个缓存失效。
    params:
    period: '1', '5', '15', '30', '60'
    """
    start_time = f"{date_str} 09:30:00"
    end_time = f"{date_str} 15:00:00"
    
    # 指数退避策略全局变量 (简单模拟，实际环境应用类封装)
    # 使用函数属性暂存状态
    if not hasattr(fetch_cached_min_data, "current_backoff"):
        fetch_cached_min_data.current_backoff = 0
            
    # 简单的重试机制
    max_retries = 3
    
    # 如果处于"冷却期"内? 这里简化为：每次失败后增加等待时间，成功则重置
    
    for attempt in range(max_retries):
        try:
            if is_index:
                # 指数接口
                df = ak.index_zh_a_hist_min_em(symbol=symbol, period=period, start_date=start_time, end_date=end_time)
            else:
                # 个股接口
                df = ak.stock_zh_a_hist_min_em(symbol=symbol, start_date=start_time, end_date=end_time, period=period, adjust='qfq')
            
            if df is not None and not df.empty:
                # 成功 - 重置退避
                if fetch_cached_min_data.current_backoff > 0:
                     print(f"[{datetime.now().time()}] API Recovered. Resetting backoff.")
                     fetch_cached_min_data.current_backoff = 0

                # 统一列名
                if '时间' in df.columns:
                    df.rename(columns={'时间': 'time', '开盘': 'open', '收盘': 'close'}, inplace=True)
                
                # 简单清洗
                df['time'] = pd.to_datetime(df['time'])
                
                # 计算涨跌幅 (相对于当日开盘)
                base_price = df['open'].iloc[0]
                df['pct_chg'] = (df['close'] - base_price) / base_price * 100
                
                return df[['time', 'pct_chg', 'close']]
                
        except Exception as e:
            # 失败处理逻辑
            # 如果是特定的 API 限制错误 (需分析 e，这里简单假设所有异常都可能由频率导致)
            # 增加退避时间
            if fetch_cached_min_data.current_backoff == 0:
                fetch_cached_min_data.current_backoff = 60 # 初始 1 分钟
            else:
                fetch_cached_min_data.current_backoff *= 2 # 翻倍
            
            wait_time = fetch_cached_min_data.current_backoff
            
            # 只有当这是后台预取任务时才进行长时间等待? 
            # 前台实时拉取不宜等待太久。这里我们添加一个上下文判断是不现实的。
            # 但既然用户提到了"翻倍等待"，这通常是针对后台爬虫。
            # 对于前台交互，等待1分钟用户早跑了。
            # 为了兼容，我们只在 "预取/爬虫" 模式下启用此逻辑？ 
            # 但 fetch_cached_min_data 是通用函数。
            # 妥协：如果等待时间很长 (>5s)，则可以认为这是一个需要长时间恢复的错误，
            # 在前台直接失败比较好。在后台则 sleep。
            # 但这里无法区分。我们假设此严格的退避策略只在外部控制循环中生效比较好。
            # 修改：将严格的退避逻辑移到调用方的 loop 中 (Task Worker)，
            # 这里的 fetch_cached_min_data 只负责单次尝试。
            pass

    return None

# --- 新增：后台预取线程逻辑 ---
def background_prefetch_task(date_list, origin_df):
    """
    后台线程：执行数据预取。
    """
    total_dates = len(date_list)
    print(f"\n[Background Worker] Started prefetch for {total_dates} days.")
    
    current_backoff = 0 # 秒
    
    indices_codes = ["000300", "000001", "399001"]
    
    for i, d in enumerate(date_list):
        d_str = d.strftime("%Y-%m-%d")
        print(f"[Background Worker] Processing: {d_str} ({i+1}/{total_dates})")
        
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
        # 如果要并发，也必须在并发发生异常时捕获并触发退避。
        # 简单起见，这里按顺序或小批次执行。
        
        for t_code, t_date, t_is_index in tasks:
            
            # Indefinite retry loop with backoff
            while True:
                try:
                    # 检查退避
                    if current_backoff > 0:
                        print(f"[Background Worker] In cool-down state. Waiting {current_backoff} seconds...")
                        time.sleep(current_backoff)
                        
                    # 尝试拉取 (fetch_cached_min_data 内部有缓存，如果已存在会直接返回)
                    # 为了测试 API 连接，如果缓存已存在，其实不会触发网络请求。
                    # 我们需要假设 fetch_cached_min_data 会处理网络。
                    # 注意：fetch_cached_min_data 被 @st.cache_data 装饰。
                    # 在后台线程调用 st.cache_data 装饰的函数通常是没问题的。
                    
                    fetch_cached_min_data(t_code, t_date, is_index=t_is_index, period='1')
                    # 只有当我们需要更多数据时才拉5分钟
                    # fetch_cached_min_data(t_code, t_date, is_index=t_is_index, period='5') 
                    
                    # Success
                    if current_backoff > 0:
                        print(f"[Background Worker] Recovered. Resetting backoff.")
                        current_backoff = 0
                    
                    # 拉取成功后稍微 sleep 一下避免过于频繁 (0.1s)
                    time.sleep(0.1)
                    break # 跳出 while，处理下一个 task

                except Exception as e:
                    print(f"[Background Worker] Error fetching {t_code} on {t_date}: {e}")
                    # 触发退避机制
                    if current_backoff == 0:
                        current_backoff = 60
                    else:
                        current_backoff *= 2
                    
                    print(f"[Background Worker] Backoff increased to {current_backoff}s. Retrying same task...")
                    # Loop continues, will sleep at start of next iteration
    
    print("[Background Worker] All tasks completed.")


def fetch_intraday_data_v2(stock_codes, target_date_str, period='1'):
    """
    获取指定股票列表 + 三大指数 的分钟级数据 (并发版)。
    v2: 增加上证、深证指数，优化缓存，原子化调用。
    v3: 引入多线程并发加速
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
    # 线程数不宜过多，以免触发反爬限制，10-20左右较为安全
    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
        future_to_task = {executor.submit(_worker, t): t for t in tasks}
        
        for future in concurrent.futures.as_completed(future_to_task):
            res = future.result()
            if res:
                results.append(res)
            
    return results

# 2. UI 布局
# -----------------------------------------------------------------------------

st.title("A股历史盘面回放系统 (沪深300 Market Replay)")

st.markdown("""
> 🕹️ **操作指南**：
> 1. 等待数据初始化完成（初次运行可能需要 2-3 分钟）。
> 2. 拖动下方滑块选择历史日期。
> 3. 观察当日盘面的资金流向与热度。
""")

# 侧边栏
with st.sidebar:
    st.header("控制台")
    if st.button("🔄 强制刷新数据"):
        if os.path.exists(CACHE_FILE):
            os.remove(CACHE_FILE)
            st.toast("已删除本地缓存，即将重新获取...")
        st.cache_data.clear()
        st.rerun()
    st.info("数据源：沪深300成分股 (AkShare)")
    st.caption("注：方块大小使用'成交额'代替'市值'，\n反映当日交易热度。")

    st.markdown("---")
    st.markdown("### 🛠️ 板块过滤")
    filter_cyb = st.checkbox("屏蔽创业板 (300开头)", value=False)
    filter_kcb = st.checkbox("屏蔽科创板 (688开头)", value=False)
    
# 加载数据
with st.spinner("正在初始化历史数据仓库..."):
    origin_df = fetch_history_data()

# --- 后台任务检测与控制 ---
# 检查是否有名为 "PrefetchWorker" 的后台线程
bg_thread = None
for t in threading.enumerate():
    if t.name == "PrefetchWorker":
        bg_thread = t
        break

# 更新 Sidebar UI
with st.sidebar:
    st.markdown("---")
    with st.expander("📥 后台数据预取", expanded=False):
        st.caption("后台静默下载最近 N 天分时数据")
        prefetch_days = st.number_input("预取天数", min_value=5, max_value=200, value=30, step=10)
        
        if bg_thread and bg_thread.is_alive():
            st.info(f"🟢 后台任务运行中...\n请关注控制台(Console)日志")
            # 无法通过 Button 停止线程，除非使用 Event。暂不实现停止。
        else:
            if st.button("🚀 启动后台下载"):
                if not origin_df.empty:
                    # 获取日期列表
                    all_dates = sorted(origin_df['日期'].dt.date.unique())
                    target_prefetch_dates = all_dates[-prefetch_days:]
                    
                    # 启动线程
                    t = threading.Thread(
                        target=background_prefetch_task,
                        args=(target_prefetch_dates, origin_df),
                        name="PrefetchWorker",
                        daemon=True
                    )
                    t.start()
                    st.rerun()
                else:
                    st.error("历史数据尚未就绪")

if not origin_df.empty:
    # --- 全局过滤逻辑 ---
    df = origin_df.copy()
    if filter_cyb:
        df = df[~df['代码'].astype(str).str.startswith('300')]
    if filter_kcb:
        df = df[~df['代码'].astype(str).str.startswith('688')]

    if df.empty:
        st.warning("过滤后没有剩余股票数据，请取消勾选过滤选项。")
        st.stop()

    # --- 时间选择器逻辑 (Session State 管理) ---
    available_dates = sorted(df['日期'].dt.date.unique())
    
    if 'selected_date_idx' not in st.session_state:
        st.session_state.selected_date_idx = len(available_dates) - 1

    #确保索引不越界
    if st.session_state.selected_date_idx >= len(available_dates):
        st.session_state.selected_date_idx = len(available_dates) - 1
    if st.session_state.selected_date_idx < 0:
        st.session_state.selected_date_idx = 0

    # 布局：前一天 | 滑块 | 后一天
    st.markdown("### 📅 选择回放日期")
    
    # 模式选择
    mode_col1, mode_col2 = st.columns([1, 3])
    with mode_col1:
        playback_mode = st.radio("回放模式", ["单日复盘", "多日走势拼接"], horizontal=True)

    if playback_mode == "单日复盘":
        col_prev, col_slider, col_next = st.columns([1, 6, 1])
        
        with col_prev:
            st.write("") 
            st.write("")
            if st.button("⬅️ 前一天"):
                if st.session_state.selected_date_idx > 0:
                    st.session_state.selected_date_idx -= 1
                    st.rerun()

        with col_next:
            st.write("")
            st.write("")
            if st.button("后一天 ➡️"):
                if st.session_state.selected_date_idx < len(available_dates) - 1:
                    st.session_state.selected_date_idx += 1
                    st.rerun()

        with col_slider:
            # 原 select_slider 替换为 date_input 以支持快速年份选择
            current_date_val = available_dates[st.session_state.selected_date_idx]
            
            picked_date = st.date_input(
                "日期",
                value=current_date_val,
                min_value=available_dates[0],
                max_value=available_dates[-1],
                label_visibility="collapsed"
            )
            
            # 如果日期发生变化
            if picked_date != current_date_val:
                if picked_date in available_dates:
                    st.session_state.selected_date_idx = available_dates.index(picked_date)
                else:
                    # 如果选中的是非交易日，寻找最近的交易日
                    closest_date = min(available_dates, key=lambda d: abs(d - picked_date))
                    st.session_state.selected_date_idx = available_dates.index(closest_date)
                    st.toast(f"📅 休市日，已自动定位到最近交易日: {closest_date}")
                st.rerun()
        
        target_dates = [available_dates[st.session_state.selected_date_idx]] # 使用 state 中的日期
        selected_date = target_dates[0]
        
    else: # 多日走势拼接
        with mode_col2:
            date_range = st.date_input(
                "选择时间范围 (建议不超过5天，否则加载较慢)",
                value=[available_dates[-5] if len(available_dates)>5 else available_dates[0], available_dates[-1]],
                min_value=available_dates[0],
                max_value=available_dates[-1]
            )
        
        if len(date_range) == 2:
            start_d, end_d = date_range
            # 筛选出范围内的交易日
            target_dates = [d for d in available_dates if start_d <= d <= end_d]
            if not target_dates: # 如果选定的范围内没有交易日 (例如全选了假期)
                 st.warning("⚠️ 选定范围内无交易数据，已自动重置为最近交易日")
                 target_dates = [available_dates[-1]]
            
            st.info(f"已选择 {len(target_dates)} 个交易日进行拼接展示")
            selected_date = target_dates[-1] # 用于下方显示统计面板的基准
        else:
            st.warning("请选择完整的开始和结束日期")
            target_dates = [available_dates[-1]]
            selected_date = available_dates[-1]

    # --- 数据切片与统计 (以最后一天或选中日为准) ---
    daily_df = df[df['日期'].dt.date == selected_date].copy()
    
    if daily_df.empty:
        st.warning(f"{selected_date} 当日无交易数据（可能是非交易日或数据缺失）。")
    else:
        # 当日统计指标
        median_chg = daily_df['涨跌幅'].median()
        total_turnover = daily_df['成交额'].sum() / 1e8 # 亿元
        top_gainer = daily_df.loc[daily_df['涨跌幅'].idxmax()]
        top_loser = daily_df.loc[daily_df['涨跌幅'].idxmin()]
        
        # 显示指标行
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("当前回放日期", selected_date.strftime("%Y-%m-%d"))
        col2.metric("成分股中位数涨跌", f"{median_chg:.2f}%", 
                    delta=f"{median_chg:.2f}%", delta_color="normal") # A股习惯需结合 Streamlit theme, 用 normal 需自行脑补红绿
        col3.metric("成分股总成交", f"{total_turnover:.1f} 亿")
        col4.metric("领涨龙头", f"{top_gainer['名称']} ({top_gainer['涨跌幅']:.2f}%)")
    # --- 新增功能：分时走势叠加 ---
    st.markdown("---")
    st.subheader("📈 核心资产分时走势叠加")
    
    # 模式选择
    chart_mode = st.radio("选股模式", ["成交额 Top 10 (活跃度)", "指数贡献 Top 20 (影响大盘)"], horizontal=True)
    st.caption("注：指数贡献 = 涨跌幅 × 权重(近似为成交额/市值占比)。此模式能看到是谁在拉动或砸盘。")

    show_intraday = st.checkbox("加载分时走势 (需从网络实时拉取)", value=False)
    
    if show_intraday:
        with st.spinner(f"正在拉取 {len(target_dates)} 天的分钟线数据 (范围: {target_dates[0]} ~ {target_dates[-1]})..."):
            
            if "成交额" in chart_mode:
                # 原逻辑：成交额最高
                top_stocks_df = daily_df.sort_values('成交额', ascending=False).head(10)
            else:
                # 新逻辑：指数贡献度 (上海 Top 20 + 深圳 Top 20)
                # Impact = abs(涨跌幅 * 成交额) 
                daily_df['abs_impact'] = (daily_df['涨跌幅'] * daily_df['成交额']).abs()
                
                # 分别筛选沪市和深市
                sh_pool = daily_df[daily_df['代码'].astype(str).str.startswith('6')].copy()
                sz_pool = daily_df[~daily_df['代码'].astype(str).str.startswith('6')].copy()
                
                sh_top = sh_pool.sort_values('abs_impact', ascending=False).head(20)
                sz_top = sz_pool.sort_values('abs_impact', ascending=False).head(20)
                
                top_stocks_df = pd.concat([sh_top, sz_top], ignore_index=True)

            # 准备参数列表
            target_stocks_list = []
            for _, row in top_stocks_df.iterrows():
                target_stocks_list.append((row['代码'], row['名称'], 0)) # Turnover temporarily 0, unused in fetch
            
            # 循环获取所有目标日期的数据并合并
            all_intraday_data = [] # List of results
            
            # 自动调整数据精度策略
            # 1分钟线通常只能获取最近5天
            # 5分钟线通常能获取最近1-2个月
            period_to_use = '1'
            if len(target_dates) > 5 and playback_mode == "多日走势拼接":
                period_to_use = '5'
                st.info(f"ℹ️ 您选择了 {len(target_dates)} 天：系统自动切换至【5分钟级】数据，以支持查看更久远的历史走势。")
            elif len(target_dates) > 10:
                 st.toast(f"⚠️ 您选择了 {len(target_dates)} 天的数据，加载可能较慢，请耐心等待...")
            
            target_dates_to_fetch = target_dates

            # 进度条
            fetch_progress = st.progress(0)
            
            for i, d_date in enumerate(target_dates_to_fetch):
                fetch_progress.progress((i + 1) / len(target_dates_to_fetch))
                d_str = d_date.strftime("%Y-%m-%d")
                
                # 获取该日所有数据
                # 注意：turnover 需要传入该日实际的 turnover，这里我们做一个简化：
                # 依然用 fetch_intraday_data_v2，但它返回的 turnover 是输入参数。
                # 实际上画图时我们希望线宽随【当日】成交额变化？或者保持一致？
                # 如果是多日拼接，建议线宽固定或取平均。简单起见，线宽使用最后一天的成交额定级。
                
                day_results = fetch_intraday_data_v2(target_stocks_list, d_str, period=period_to_use)
                
                # 为数据添加 'date_str' 标识
                for res in day_results:
                     res['data']['date_col'] = d_str
                     res['real_date'] = d_date
                
                all_intraday_data.extend(day_results)
            
            fetch_progress.empty()
            
            if not all_intraday_data:
                st.warning("未能获取到分时数据")
            else:
                # --- 数据重组 ---
                # 将分散的数据合并为： { 'code': { 'name':..., 'is_index':..., 'full_data': DataFrame } }
                combined_series = {}
                
                for item in all_intraday_data:
                    code = item['code']
                    if code not in combined_series:
                        # 查找该股票在选定日(最后一日)的成交额，用于定线宽
                        to_val = 0
                        if not item.get('is_index'):
                            matches = daily_df[daily_df['代码'] == code]
                            if not matches.empty:
                                to_val = matches.iloc[0]['成交额']
                        
                        combined_series[code] = {
                            'name': item['name'],
                            'code': code,
                            'is_index': item.get('is_index', False),
                            'turnover': to_val, # 使用最后一天的成交额
                            'dfs': []
                        }
                    combined_series[code]['dfs'].append(item['data'])
                
                # 合并 DataFrame 并构建连续时间轴
                # 策略：不使用真实时间轴，而是使用 "交易分钟序列" (Trading Minute Index)
                # 每天 240 分钟。Day 1: 0-239, Day 2: 240-479...
                # 需要生成一个 X轴 Label Map
                
                final_plot_data = [] # List of {idx, pct_chg, ...}
                x_tick_vals = []
                x_tick_text = []
                
                # 为每一天生成标准时间序列 (避免缺失分钟导致的错位)
                # 09:30 - 11:30 (121 points usually inclusive? A股分钟线通常包含 11:30 和 15:00)
                # 分钟线通常是 09:31 - 11:30 (120 mins) 13:01 - 15:00 (120 mins). 
                # AkShare 返回的数据时间如果是 09:30 通常代表开盘?
                # 让我们通过观察第一条数据来决定逻辑，通常直接 concat 即可
                # 为了解决 GAP，我们在 UI 绘图层强制把 x 映射为 0, 1, 2...
                
                # 提取真正获取到数据的日期列表 (去除节假日/无数据日)
                valid_dates = set()
                for item in all_intraday_data:
                     if 'real_date' in item:
                         valid_dates.add(item['real_date'].strftime("%Y-%m-%d"))
                
                days_list = sorted(list(valid_dates))
                
                if not days_list:
                    # 如果过滤后居然没了（理论上外层 checked not empty），回退兜底
                     days_list = sorted(list(set([x.strftime("%Y-%m-%d") for x in target_dates_to_fetch])))
                
                # 构建基准时间网格 (Template) - 每天 240/241 个点
                # 09:30 - 11:30, 13:00 - 15:00
                dummy_date = "2000-01-01"
                morning_range = pd.date_range(f"{dummy_date} 09:30", f"{dummy_date} 11:30", freq="1min")
                afternoon_range = pd.date_range(f"{dummy_date} 13:00", f"{dummy_date} 15:00", freq="1min")
                daily_time_template = morning_range.union(afternoon_range) # Size approx 242
                
                # 计算总偏移量
                points_per_day = len(daily_time_template)
                
                for code, info in combined_series.items():
                    # 合并、排序
                    full_df = pd.concat(info['dfs']).sort_values(['date_col', 'time'])
                    
                    # 重新构造 X 轴 (Int)
                    # 算法：对于每一行，找到它是 第几天 的 第几分钟
                    # x_int = day_index * points_per_day + minute_index_in_day
                    
                    full_df['time_str'] = full_df['time'].dt.strftime("%H:%M:%S")
                    
                    x_values = []
                    
                    for idx, row in full_df.iterrows():
                        d_str = row['date_col']
                        t_str = row['time_str'] # 完整时间对象
                        
                        # 确定是第几天
                        day_idx = days_list.index(d_str) if d_str in days_list else 0
                        
                        # 确定是当天的第几分钟
                        # 简单转换：小时*60 + 分钟
                        t_obj = row['time'] # timestamp
                        mins_from_midnight = t_obj.hour * 60 + t_obj.minute
                        
                        # 把中午休市的时间压掉
                        # 9:30 (570) -> 11:30 (690). Length 120.
                        # 13:00 (780) -> 15:00 (900). 
                        
                        if mins_from_midnight <= 690: # Morning
                            offset = mins_from_midnight - 570 # 09:30 is 0
                        else: # Afternoon
                            offset = 120 + (mins_from_midnight - 780) # 13:00 starts at 120+
                            
                        final_x = day_idx * (240 + 20) + offset # 加20个单位的间隔让天与天之间有点空隙
                        x_values.append(final_x)
                        
                    full_df['x_int'] = x_values
                    
                    # 计算累计涨跌幅 (如果是多日，需要链式计算？)
                    # 简单方案：每天重新归零？还是多日连贯？
                    # 用户说 "拼起来形成完整的图"，通常意味着连贯趋势。
                    # 以第一天开盘价为基准
                    base_price = full_df['close'].iloc[0]
                    # 如果中间有断点，简单的 (close - base) / base 可能失真（因为昨收...）
                    # 准确做法：累乘每天的涨跌幅。
                    # 但这里只要大概趋势。如果用 (Px - P0) / P0，那跨日缺口会体现为直线的跳跃。
                    # 这符合 "真实价格走势"。
                    
                    full_df['cumulative_pct'] = (full_df['close'] - base_price) / base_price * 100
                    
                    info['plot_data'] = full_df
                
                # 生成 X 轴标签 (只显示每天的 9:30, 10:30, 11:30/13:00, 14:00, 15:00)
                # 或者只显示日期 + 关键点
                for i, d_str in enumerate(days_list):
                    base_x = i * (240 + 20)
                    day_label = d_str[5:] # MM-DD
                    # 09:30
                    x_tick_vals.append(base_x)
                    x_tick_text.append(f"{day_label}\n09:30")
                    # 11:30
                    x_tick_vals.append(base_x + 120)
                    x_tick_text.append("11:30/13:00")
                    # 15:00
                    x_tick_vals.append(base_x + 240)
                    x_tick_text.append("15:00")

                # 分类
                idx_data = [v for k,v in combined_series.items() if v['is_index']]
                stk_data = [v for k,v in combined_series.items() if not v['is_index']]
                
                sh_stocks = [v for v in stk_data if v['code'].startswith('6')]
                sz_stocks = [v for v in stk_data if not v['code'].startswith('6')]
                
                sh_index = [v for v in idx_data if v['code'] in ['000001', '000300']]
                sz_index = [v for v in idx_data if v['code'] in ['399001', '000300']]

                # 绘图函数 v3
                def plot_intraday_v3(stocks, indices, title_suffix):
                    fig = go.Figure()
                    
                    # 个股
                    if stocks:
                        max_to = max([s['turnover'] for s in stocks]) if stocks else 1
                        min_to = min([s['turnover'] for s in stocks]) if stocks else 0
                        
                        # 生成 distinct colors
                        color_palette = px.colors.qualitative.Alphabet + px.colors.qualitative.Dark24
                        
                        for i, s in enumerate(stocks):
                            if max_to == min_to: width=2
                            else: width = 1 + 3*(s['turnover'] - min_to)/(max_to - min_to)
                            
                            df_p = s['plot_data']
                            last_val = df_p['cumulative_pct'].iloc[-1]
                            
                            # 之前的红绿逻辑: color = 'rgba(214, 39, 40, 0.4)' if last_val > 0 else 'rgba(44, 160, 44, 0.4)'
                            # 现在改为区分颜色
                            color = color_palette[i % len(color_palette)]
                            
                            fig.add_trace(go.Scatter(
                                x=df_p['x_int'],
                                y=df_p['cumulative_pct'],
                                mode='lines',
                                name=s['name'],
                                # line=dict(width=width, color=color),
                                # 个股线宽不需要太粗，颜色要清晰
                                line=dict(width=max(1.5, width), color=color),
                                hovertemplate=f"<b>{s['name']}</b><br>涨跌: %{{y:.2f}}%<br>时间: %{{customdata}}",
                                customdata=df_p['date_col'] + ' ' + df_p['time_str']
                            ))
                            
                    # 指数
                    idx_colors = {'000300': 'black', '000001': '#d62728', '399001': '#1f77b4'}
                    for idx in indices:
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

                    # 3. 分割线 (如果是多日)
                    if len(days_list) > 1:
                        for i in range(1, len(days_list)):
                            # 在每一天开始前画竖线
                            boundary = i * (240 + 20) - 10
                            fig.add_vline(x=boundary, line_width=1, line_dash="dash", line_color="gray")

                    fig.update_layout(
                        title=f"分时走势叠加 ({'多日拼接' if len(days_list)>1 else days_list[0]}) - {title_suffix}",
                        xaxis=dict(
                            tickmode='array',
                            tickvals=x_tick_vals,
                            ticktext=x_tick_text,
                            showgrid=True,
                            showspikes=True, # 显示垂直辅助线
                            spikemode='across',
                            spikesnap='cursor',
                            showline=True, 
                            linewidth=1, 
                            linecolor='black',
                            mirror=True
                        ),
                        yaxis=dict(
                            showspikes=True # Y轴也显示辅助线，方便看点位
                        ),
                        yaxis_title="累计涨跌幅 (%)",
                        hovermode="x unified", # 开启统一 Hover，显示该时间点所有数据
                        height=700, # 稍微调高高度以容纳更多 Hover 信息
                        legend=dict(orientation="h", y=1.02, x=1, xanchor='right') # 保持图例的布局
                    )
                    
                    # ⚠️ 关键修正：确保 Hover Tooltip 的排序按照 Y 轴数值 (从高到低)
                    # "closest" 模式配合 "compare" 可能无法生效，但在 "x unified" 模式下，
                    # 默认是按照 Trace 添加顺序排序的。
                    # Plotly (JS层) 在 x unified 下有默认排序逻辑 (通常是 value descending)，但在某些版本可能不稳定。
                    # 为了增强排序体验，我们可以尝试设置 layout.hoverlabel.namelength = -1
                    
                    fig.update_layout(hoverlabel=dict(namelength=-1))
                    
                    return fig

                tab1, tab2 = st.tabs(["沪市 (SH)", "深市 (SZ)"])
                
                with tab1:
                    st.plotly_chart(plot_intraday_v3(sh_stocks, sh_index, "沪市权重股"), use_container_width=True)
                with tab2:
                    st.plotly_chart(plot_intraday_v3(sz_stocks, sz_index, "深市权重股"), use_container_width=True)


        st.divider()
        
        # --- 可视化 ---
        st.subheader(f"📊 {selected_date.strftime('%Y年%m月%d日')} 市场全景热力图")
        
        # A股专用色谱
        max_limit = 7
        min_limit = -7
        
        fig = px.treemap(
            daily_df,
            path=['名称'],
            values='成交额', # 用成交额代表热度/权重 (因为历史市值难获取)
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
        
        # 优化显示
        fig.update_traces(
            textinfo="label+value+percent entry",
            hovertemplate="<b>%{label}</b><br>收盘价: %{customdata[2]}<br>涨跌幅: %{color:.2f}%<br>成交额: %{value:.2s}"
        )
        fig.update_layout(
            margin=dict(t=10, l=10, r=10, b=10),
            coloraxis_colorbar=dict(title="涨跌幅(%)")
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # 可选：显示详细数据表
        with st.expander("查看当日详细数据"):
            st.dataframe(
                daily_df[['代码', '名称', '收盘', '涨跌幅', '成交额']].style.format({
                    '收盘': '{:.2f}',
                    '涨跌幅': '{:.2f}%',
                    '成交额': '{:,.0f}'
                })
            )

else:
    st.error("数据加载失败，请刷新重试。")
