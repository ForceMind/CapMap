import streamlit as st
import pandas as pd
import akshare as ak
from datetime import datetime, timedelta
import time
import random
import os
import concurrent.futures
import threading
import sys
import io
import zipfile
import shutil
import json
import logging
from pypinyin import lazy_pinyin

from core.providers import (
    fetch_biying_daily,
    fetch_biying_intraday,
    fetch_biying_stock_list,
    fetch_biying_index_cons, # Add this
    fetch_biying_all_realtime, # 新增
    fetch_biying_stock_info, # 新增
    get_biying_licence,
    get_provider_order,
)

# 尝试导入 Streamlit 上下文管理器，用于多线程场景
try:
    from streamlit.runtime.scriptrunner import add_script_run_ctx, get_script_run_ctx
except ImportError:
    from streamlit.scriptrunner import add_script_run_ctx, get_script_run_ctx

# 1. 核心数据逻辑
# -----------------------------------------------------------------------------

CACHE_FILE = "data/csi300_history_cache.parquet"
MIN_CACHE_DIR = "data/min_cache"
NAME_MAP_FILE = "data/name_map.json"
NAME_REFRESH_FILE = "data/name_refresh.json"
NAME_REFRESH_TTL_HOURS = 24 * 180
NAME_REFRESH_MIN_INTERVAL_MINUTES = 30
NAME_MAP_VERSION = 1
ALL_STOCKS_CACHE_FILE = "data/all_stocks_list.csv"

def get_all_stocks_list(force_update=False):
    """
    获取所有 A 股列表 (代码, 名称, 拼音首字母)
    优先读取缓存，过期或不存在则调用 AkShare
    """
    # 检查缓存是否存在
    if not force_update and os.path.exists(ALL_STOCKS_CACHE_FILE):
        try:
            # 简单检查文件是否超过 24 小时
            mtime = os.path.getmtime(ALL_STOCKS_CACHE_FILE)
            if time.time() - mtime < 24 * 3600:
                df = pd.read_csv(ALL_STOCKS_CACHE_FILE, dtype={'code': str})
                return df
        except Exception as e:
            logging.error(f"Error reading stock list cache: {e}")
    
    # ---------------------------------------------------------
    # 优先尝试从 Biying 拉取 (User Request: Prioritize Biying)
    # ---------------------------------------------------------
    status_msg = st.empty() if 'st' in globals() else None
    
    try:
        if status_msg: status_msg.info("⏳ 正在从必盈(Biying)同步全市场股票列表...")
        licence = get_biying_licence()
        by_stocks_map = fetch_biying_stock_list(licence)
        
        if by_stocks_map:
            logging.info(f"Fetched {len(by_stocks_map)} stocks from Biying.")
            df = pd.DataFrame(list(by_stocks_map.items()), columns=['code', 'name'])
            
            # Pinyin generation
            if status_msg: status_msg.info("⏳ 正在生成拼音索引...")
            def get_pinyin_first_letters(text):
                try:
                    if not isinstance(text, str): return ""
                    return "".join([p[0].upper() for p in lazy_pinyin(text) if p])
                except:
                    return ""
            df['pinyin'] = df['name'].apply(get_pinyin_first_letters)
            
            # Save
            if not os.path.exists("data"):
                os.makedirs("data")
            df.to_csv(ALL_STOCKS_CACHE_FILE, index=False)
            
            if status_msg:
                status_msg.success(f"✅ 股票列表已更新(Biying源) (共 {len(df)} 只)")
                time.sleep(1)
                status_msg.empty()
            return df
        else:
            if status_msg: status_msg.warning("⚠️ 必盈接口返回空，尝试 AkShare 备用...")
            logging.warning("Biying stock list returned empty.")
            
    except Exception as e:
        logging.error(f"Biying fetch failed: {e}")
        if status_msg: status_msg.error(f"Biying List Error: {e}")

    # ---------------------------------------------------------
    # Fallback to Biying Realtime All (If basic list failed)
    # ---------------------------------------------------------
    try:
        from core.providers import fetch_biying_all_realtime
        status_msg = st.empty() if 'st' in globals() else None
        
        # 只有当 Biying 基本列表获取失败时才尝试这个，或者可以默认优先用这个？
        # 目前流程是先试 fetch_biying_stock_list，如果失败了才到这里。
        # 我们用这个作为 AkShare 之前的第一道防线
        if status_msg: status_msg.info("⏳ 尝试从必盈(Biying)获取全量实时快照作为列表...")
        licence = get_biying_licence()
        real_df = fetch_biying_all_realtime(licence)
        
        if not real_df.empty:
             logging.info(f"Fetched {len(real_df)} stocks from Biying Snapshot.")
             df = real_df[['code', 'name']].copy()
             
             # Pinyin
             if status_msg: status_msg.info("⏳ 正在生成拼音索引...")
             def get_pinyin_first_letters(text):
                try:
                    if not isinstance(text, str): return ""
                    return "".join([p[0].upper() for p in lazy_pinyin(text) if p])
                except:
                    return ""
             df['pinyin'] = df['name'].apply(get_pinyin_first_letters)
             
             # Save
             if not os.path.exists("data"):
                os.makedirs("data")
             df.to_csv(ALL_STOCKS_CACHE_FILE, index=False)
             
             if status_msg:
                status_msg.success(f"✅ 股票列表已更新(Biying快照) (共 {len(df)} 只)")
                time.sleep(1)
                status_msg.empty()
             return df
    except Exception as e:
        logging.error(f"Biying Snapshot fetch failed: {e}")

    # If Biying also failed or no licence
    if os.path.exists(ALL_STOCKS_CACHE_FILE):
         if 'st' in globals():
             st.toast("⚠️ 无法获取股票列表 (需配置Biying Licence)，使用缓存")
         logging.warning("Failed to fetch stock list from Biying. Using cache.")
         return pd.read_csv(ALL_STOCKS_CACHE_FILE, dtype={'code': str})
    
    return pd.DataFrame(columns=['code', 'name', 'pinyin'])

APP_LOG_FILE = "logs/app.log"
INTRADAY_WORKERS = int(os.environ.get("INTRADAY_WORKERS", "50"))
INTRADAY_DELAY_SEC = float(os.environ.get("INTRADAY_DELAY_SEC", "0.05"))
DEFAULT_MIN_PERIOD = os.environ.get("DEFAULT_MIN_PERIOD", "5")
AUTO_PREFETCH_ENABLED = os.environ.get("AUTO_PREFETCH_ENABLED", "1") == "1"
AUTO_PREFETCH_TIME = os.environ.get("AUTO_PREFETCH_TIME", "15:15")
AUTO_PREFETCH_DELAY_SEC = float(os.environ.get("AUTO_PREFETCH_DELAY_SEC", "10"))
AUTO_PREFETCH_RETRY_SLEEP_SEC = float(os.environ.get("AUTO_PREFETCH_RETRY_SLEEP_SEC", "300"))
AUTO_PREFETCH_MAX_RETRIES = int(os.environ.get("AUTO_PREFETCH_MAX_RETRIES", "0"))
AUTO_PREFETCH_STATE_FILE = "data/auto_prefetch_state.json"

def _init_logging():
    log_path = os.path.abspath(APP_LOG_FILE)
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    
    logger = logging.getLogger("capmap")
    logger.setLevel(logging.INFO)
    
    # 清理旧的 handlers 防止重复
    logger.handlers = []

    # 1. 文件输出
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # 2. 控制台输出 (Stdout)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    logger.propagate = False

    logging.captureWarnings(True)
    for name, level in (("akshare", logging.INFO), ("py.warnings", logging.WARNING)):
        other = logging.getLogger(name)
        other.setLevel(level)
        # 清理旧的 handlers
        other.handlers = []
        other.addHandler(file_handler)
        other.addHandler(console_handler)
        other.propagate = False
    
    return logger

logger = _init_logging()

def _fmt_kv(kwargs):
    parts = []
    for k, v in kwargs.items():
        try:
            parts.append(f"{k}={v}")
        except Exception:
            parts.append(f"{k}=?")
    return " ".join(parts)

def log_action(action, **kwargs):
    # 仅用于调试前端操作，默认不输出到 INFO 级别日志
    if kwargs:
        logger.debug("前端操作: %s | %s", action, _fmt_kv(kwargs))
    else:
        logger.debug("前端操作: %s", action)

CODE_COL_CANDIDATES = [
    "\u4ee3\u7801",
    "\u8bc1\u5238\u4ee3\u7801",
    "\u54c1\u79cd\u4ee3\u7801",
    "variety",
    "symbol",
    "code",
]
NAME_COL_CANDIDATES = [
    "\u540d\u79f0",
    "\u8bc1\u5238\u7b80\u79f0",
    "\u54c1\u79cd\u540d\u79f0",
    "name",
    "\u80a1\u7968\u7b80\u79f0",
    "\u80a1\u7968\u540d\u79f0",
]
NAME_ITEM_CANDIDATES = [
    "\u80a1\u7968\u7b80\u79f0",
    "\u80a1\u7968\u540d\u79f0",
    "\u8bc1\u5238\u7b80\u79f0",
    "\u540d\u79f0",
]

def _normalize_date_str(date_str):
    try:
        dt = pd.to_datetime(date_str)
        return dt.strftime("%Y-%m-%d"), dt.strftime("%Y%m%d")
    except Exception:
        s = str(date_str)
        return s, s.replace("-", "")

def _min_cache_path(symbol, date_key, period, is_index):
    kind = "index" if is_index else "stock"
    return os.path.join(MIN_CACHE_DIR, f"p{period}", kind, str(symbol), f"{date_key}.csv")

def _read_min_cache(path):
    if os.path.exists(path):
        try:
            return pd.read_csv(path, parse_dates=["time"])
        except Exception as e:
            logger.warning("读取分时缓存失败: %s", e)
    return None

def _write_min_cache(path, df):
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp_path = f"{path}.tmp"
        df.to_csv(tmp_path, index=False)
        os.replace(tmp_path, path)
    except Exception as e:
        logger.warning("保存分时缓存失败: %s", e)

def _parse_hhmm(value):
    try:
        parts = str(value).split(":")
        if len(parts) == 2:
            return int(parts[0]), int(parts[1])
    except Exception:
        pass
    return 15, 15

def _load_prefetch_state():
    if not os.path.exists(AUTO_PREFETCH_STATE_FILE):
        return {}
    try:
        with open(AUTO_PREFETCH_STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return data
    except Exception as e:
        logger.warning("\u8bfb\u53d6\u81ea\u52a8\u9884\u53d6\u72b6\u6001\u5931\u8d25: %s", e)
    return {}

def _save_prefetch_state(state):
    try:
        os.makedirs(os.path.dirname(AUTO_PREFETCH_STATE_FILE), exist_ok=True)
        with open(AUTO_PREFETCH_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f)
    except Exception as e:
        logger.warning("\u4fdd\u5b58\u81ea\u52a8\u9884\u53d6\u72b6\u6001\u5931\u8d25: %s", e)

def _is_trading_day(target_date, origin_df):
    if origin_df is None or origin_df.empty:
        return False
    try:
        return target_date in set(origin_df['日期'].dt.date)
    except Exception:
        return False

def _get_daily_codes(origin_df, target_date):
    if origin_df is None or origin_df.empty:
        return [], {}
    daily = origin_df[origin_df['日期'].dt.date == target_date]
    if daily.empty:
        return [], {}
    codes = daily['代码'].astype(str).tolist()
    name_map = dict(zip(daily['代码'].astype(str), daily['名称']))
    return codes, name_map

def _scan_cached_dates(period=DEFAULT_MIN_PERIOD, is_index=False):
    base = os.path.join(MIN_CACHE_DIR, f"p{period}", "index" if is_index else "stock")
    if not os.path.exists(base):
        return []
    dates = set()
    for root, _, files in os.walk(base):
        for f in files:
            if f.endswith('.csv'):
                dates.add(f[:-4])
    return sorted(dates)

def _get_cached_codes_for_date(date_key, codes, period=DEFAULT_MIN_PERIOD, is_index=False):
    cached = set()
    for code in codes:
        path = _min_cache_path(code, date_key, period, is_index)
        if os.path.exists(path):
            cached.add(code)
    return cached

def _serial_fetch_intraday(date_str, codes, name_map, include_indices=True, delay_sec=10, retry_sleep_sec=300, max_retries=3, job_tag="manual"):
    indices_map = {
        "000300": "\u6caa\u6df1300",
        "000001": "\u4e0a\u8bc1\u6307\u6570",
        "399001": "\u6df1\u8bc1\u6210\u6307",
    }
    tasks = []
    if include_indices:
        for idx_code, idx_name in indices_map.items():
            tasks.append({"code": idx_code, "name": idx_name, "is_index": True})
    for code in codes:
        tasks.append({"code": str(code), "name": name_map.get(str(code), str(code)), "is_index": False})
    logger.info("\u4efb\u52a1\u5f00\u59cb(%s): date=%s total=%s delay=%.1fs retry=%.0fs max_retries=%s", job_tag, date_str, len(tasks), delay_sec, retry_sleep_sec, max_retries)
    success = 0
    failed = 0
    for t in tasks:
        code = t['code']
        name = t['name']
        is_index = t['is_index']
        api_name = "index_zh_a_hist_min_em" if is_index else "stock_zh_a_hist_min_em"
        attempt = 0
        while True:
            try:
                data = fetch_cached_min_data(code, date_str, is_index=is_index, period=DEFAULT_MIN_PERIOD, raise_on_error=True)
                if data is None or data.empty:
                    raise RuntimeError("\u63a5\u53e3\u8fd4\u56de\u7a7a")
                success += 1
                logger.info("\u9884\u53d6\u6210\u529f(%s): code=%s name=%s api=%s", job_tag, code, name, api_name)
                break
            except Exception as e:
                attempt += 1
                logger.warning("\u9884\u53d6\u5931\u8d25(%s): code=%s name=%s api=%s attempt=%s err=%s", job_tag, code, name, api_name, attempt, e)
                if max_retries > 0 and attempt >= max_retries:
                    failed += 1
                    logger.warning("\u9884\u53d6\u653e\u5f03(%s): code=%s name=%s", job_tag, code, name)
                    break
                time.sleep(retry_sleep_sec)
        if delay_sec > 0:
            time.sleep(delay_sec)
    logger.info("\u4efb\u52a1\u5b8c\u6210(%s): date=%s success=%s failed=%s", job_tag, date_str, success, failed)
    return success, failed

def _auto_prefetch_worker(date_str, codes, name_map, ctx=None):
    if ctx:
        add_script_run_ctx(threading.current_thread(), ctx)
    state = {"date": date_str, "status": "running", "updated": int(time.time())}
    _save_prefetch_state(state)
    success, failed = _serial_fetch_intraday(
        date_str,
        codes,
        name_map,
        include_indices=True,
        delay_sec=AUTO_PREFETCH_DELAY_SEC,
        retry_sleep_sec=AUTO_PREFETCH_RETRY_SLEEP_SEC,
        max_retries=AUTO_PREFETCH_MAX_RETRIES,
        job_tag="auto",
    )
    state = {"date": date_str, "status": "done" if failed == 0 else "partial", "success": success, "failed": failed, "updated": int(time.time())}
    _save_prefetch_state(state)

def _start_manual_prefetch(date_str, codes, name_map, include_indices=True):
    if not codes and not include_indices:
        return False
    ctx = get_script_run_ctx()
    def _worker():
        if ctx:
            add_script_run_ctx(threading.current_thread(), ctx)
        _serial_fetch_intraday(
            date_str,
            codes,
            name_map,
            include_indices=include_indices,
            delay_sec=AUTO_PREFETCH_DELAY_SEC,
            retry_sleep_sec=AUTO_PREFETCH_RETRY_SLEEP_SEC,
            max_retries=AUTO_PREFETCH_MAX_RETRIES,
            job_tag="manual",
        )
    t = threading.Thread(target=_worker, daemon=True)
    t.start()
    return True

def _start_auto_prefetch_if_needed(origin_df):
    if not AUTO_PREFETCH_ENABLED:
        return
    now = datetime.now()
    h, m = _parse_hhmm(AUTO_PREFETCH_TIME)
    if now.hour < h or (now.hour == h and now.minute < m):
        return
    today = now.date()
    if not _is_trading_day(today, origin_df):
        return
    today_str = today.strftime("%Y-%m-%d")
    state = _load_prefetch_state()
    if state.get("date") == today_str and state.get("status") in ("running", "done"):
        return
    codes, name_map = _get_daily_codes(origin_df, today)
    if not codes:
        return
    if st.session_state.get("auto_prefetch_started"):
        return
    st.session_state["auto_prefetch_started"] = True
    ctx = get_script_run_ctx()
    t = threading.Thread(target=_auto_prefetch_worker, args=(today_str, codes, name_map, ctx), daemon=True)
    t.start()

def _load_name_refresh_state():
    if not os.path.exists(NAME_REFRESH_FILE):
        return {}
    try:
        with open(NAME_REFRESH_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return data
    except Exception as e:
        logger.warning("读取名称刷新记录失败: %s", e)
    return {}

def _save_name_refresh_state(state):
    try:
        os.makedirs(os.path.dirname(NAME_REFRESH_FILE), exist_ok=True)
        with open(NAME_REFRESH_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f)
    except Exception as e:
        logger.warning("保存名称刷新记录失败: %s", e)

def _load_name_map():
    if not os.path.exists(NAME_MAP_FILE):
        return {}
    try:
        with open(NAME_MAP_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return {str(k): v for k, v in data.items()}
    except Exception as e:
        logger.warning("读取名称映射失败: %s", e)
    return {}

def _save_name_map(name_map):
    try:
        os.makedirs(os.path.dirname(NAME_MAP_FILE), exist_ok=True)
        with open(NAME_MAP_FILE, "w", encoding="utf-8") as f:
            json.dump(name_map, f, ensure_ascii=False)
    except Exception as e:
        logger.warning("保存名称映射失败: %s", e)

def _resolve_code_name_columns(df):
    if df is None or df.empty:
        return None, None
    cols = list(df.columns)
    for code_col in CODE_COL_CANDIDATES:
        if code_col in cols:
            for name_col in NAME_COL_CANDIDATES:
                if name_col in cols:
                    return code_col, name_col
    for code_col in cols:
        if "\u4ee3\u7801" in str(code_col):
            for name_col in cols:
                if ("\u540d\u79f0" in str(name_col)) or ("\u7b80\u79f0" in str(name_col)):
                    return code_col, name_col
    if len(cols) >= 2:
        return cols[0], cols[1]
    return None, None

def _name_map_from_df(df):
    if df is None or df.empty:
        return {}
    code_col, name_col = _resolve_code_name_columns(df)
    if not code_col or not name_col:
        return {}
    try:
        sub = df[[code_col, name_col]].copy()
        sub[code_col] = sub[code_col].astype(str)
        sub[name_col] = sub[name_col].astype(str)
        return dict(zip(sub[code_col], sub[name_col]))
    except Exception as e:
        logger.warning("名称映射构建失败: %s", e)
        return {}

def _extract_name_from_kv_df(df):
    if df is None or df.empty:
        return None
    item_col = None
    value_col = None
    if "item" in df.columns and "value" in df.columns:
        item_col, value_col = "item", "value"
    elif "\u9879\u76ee" in df.columns and "\u503c" in df.columns:
        item_col, value_col = "\u9879\u76ee", "\u503c"
    if not item_col or not value_col:
        return None
    try:
        mapping = dict(zip(df[item_col], df[value_col]))
    except Exception:
        return None
    for key in NAME_ITEM_CANDIDATES:
        if key in mapping and mapping[key]:
            return str(mapping[key]).strip()
    return None

def _fetch_name_for_code(code):
    """获取单个股票名称 (优先 Biying)"""
    code = str(code)
    try:
        from core.providers import fetch_biying_stock_info, get_biying_licence
        licence = get_biying_licence()
        if licence:
             # Biying API: /hscp/gsjj/{code} -> 返回包括股票名称的信息
             info = fetch_biying_stock_info(code, licence)
             # 可能的返回: {'dm': '600000', 'mc': '浦发银行', ...}
             if info and isinstance(info, dict):
                 name = info.get("mc") or info.get("name") or info.get("名称")
                 if name:
                     return name
    except Exception as e:
        logger.warning(f"Biying name fetch failed: {e}")

    # AkShare fallback removed
    return None

def _should_refresh_names(state, now_ts):
    last_attempt = state.get("last_attempt_ts")
    if isinstance(last_attempt, (int, float)):
        if now_ts - last_attempt < NAME_REFRESH_MIN_INTERVAL_MINUTES * 60:
            return False
    last_refresh = state.get("last_refresh_ts")
    if isinstance(last_refresh, (int, float)):
        if now_ts - last_refresh < NAME_REFRESH_TTL_HOURS * 3600:
            return False
    return True

def _refresh_name_map_if_needed(force=False):
    now_ts = int(time.time())
    state = _load_name_refresh_state()
    if state.get("name_map_version") != NAME_MAP_VERSION:
        force = True
    if (not force) and (not _should_refresh_names(state, now_ts)):
        logger.info("名称映射无需刷新，使用本地缓存")
        return _load_name_map()
    state["last_attempt_ts"] = now_ts
    _save_name_refresh_state(state)
    logger.info("开始刷新名称映射 (force=%s)", force)
    providers = get_provider_order()
    licence = get_biying_licence()
    if "biying" in providers and licence:
        try:
            name_map = fetch_biying_stock_list(licence)
        except Exception as e:
            logger.warning("必盈名称源调用失败: %s", e)
            name_map = {}
        if name_map:
            _save_name_map(name_map)
            logger.info("名称映射更新成功: source=biying count=%s", len(name_map))
            state["last_refresh_ts"] = now_ts
            state["name_map_version"] = NAME_MAP_VERSION
            state["last_source"] = "biying"
            _save_name_refresh_state(state)
            return name_map
    def _try_source(label, fn):
        try:
            df = fn()
        except Exception as e:
            logger.warning("名称源调用失败: %s err=%s", label, e)
            return {}
        name_map = _name_map_from_df(df)
        if not name_map:
            return {}
        state["last_source"] = label
        return name_map

    sources = [("stock_zh_a_spot_em", lambda: ak.stock_zh_a_spot_em())]
    if hasattr(ak, "stock_info_a_code_name"):
        sources.append(("stock_info_a_code_name", lambda: ak.stock_info_a_code_name()))
    if hasattr(ak, "stock_zh_a_spot"):
        sources.append(("stock_zh_a_spot", lambda: ak.stock_zh_a_spot()))
    sources.append(("index_stock_cons_000300", lambda: ak.index_stock_cons(symbol="000300")))

    for label, fn in sources:
        name_map = _try_source(label, fn)
        if name_map:
            _save_name_map(name_map)
            logger.info("名称映射更新成功: source=%s count=%s", label, len(name_map))
            state["last_refresh_ts"] = now_ts
            state["name_map_version"] = NAME_MAP_VERSION
            _save_name_refresh_state(state)
            return name_map
    logger.warning("名称映射刷新失败，使用本地缓存")
    return _load_name_map()

def _refresh_name_map_for_codes(codes, force=False):
    codes = [str(c) for c in codes if c is not None and str(c).strip()]
    logger.info("名称补齐开始: codes=%s force=%s", len(codes), force)
    if not codes:
        return _refresh_name_map_if_needed(force=force)

    name_map = _refresh_name_map_if_needed(force=force)
    if not name_map:
        name_map = _load_name_map()

    state = _load_name_refresh_state()
    now_ts = int(time.time())
    last_refresh = state.get("last_refresh_ts")
    global_fresh = (
        isinstance(last_refresh, (int, float))
        and now_ts - last_refresh < NAME_REFRESH_TTL_HOURS * 3600
        and bool(name_map)
    )
    if global_fresh and (not force):
        logger.info("名称补齐完成: 无需更新")
        return name_map

    code_state = state.get("code_refresh_ts")
    if not isinstance(code_state, dict):
        code_state = {}

    updated = False
    updated_count = 0
    for code in codes:
        last_ts = code_state.get(code)
        if (not force) and isinstance(last_ts, (int, float)):
            if now_ts - last_ts < NAME_REFRESH_TTL_HOURS * 3600:
                continue
        name = _fetch_name_for_code(code)
        if name:
            name_map[code] = name
            code_state[code] = now_ts
            updated = True
            updated_count += 1

    if updated:
        _save_name_map(name_map)
        state["code_refresh_ts"] = code_state
        _save_name_refresh_state(state)
        logger.info("名称补齐完成: 更新 %s 条", updated_count)
    else:
        logger.info("名称补齐完成: 无需更新")
    return name_map

def _refresh_cached_names(cached_df):
    if cached_df is None or cached_df.empty:
        return cached_df
    if '代码' not in cached_df.columns:
        return cached_df
    name_map = _refresh_name_map_if_needed()
    if not name_map:
        return cached_df
    cached_df['代码'] = cached_df['代码'].astype(str)
    if '名称' in cached_df.columns:
        cached_df['名称'] = cached_df['代码'].map(name_map).fillna(cached_df['名称'])
    else:
        cached_df['名称'] = cached_df['代码'].map(name_map)
    return cached_df

def build_data_backup_zip():
    data_dir = "data"
    if not os.path.isdir(data_dir):
        return None
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, _, files in os.walk(data_dir):
            for name in files:
                abs_path = os.path.join(root, name)
                rel_path = os.path.relpath(abs_path, data_dir)
                zf.write(abs_path, os.path.join("data", rel_path))
    buf.seek(0)
    return buf.read()

def restore_data_backup(uploaded_file):
    data_dir = "data"
    os.makedirs(data_dir, exist_ok=True)
    uploaded_file.seek(0)
    restored = 0
    with zipfile.ZipFile(uploaded_file) as zf:
        for member in zf.infolist():
            name = member.filename.replace("\\", "/")
            if name.endswith("/"):
                continue
            if name.startswith("/") or ".." in name.split("/"):
                continue
            parts = name.split("/")
            if parts and parts[0] == "data":
                parts = parts[1:]
            if not parts:
                continue
            dest_path = os.path.join(data_dir, *parts)
            os.makedirs(os.path.dirname(dest_path), exist_ok=True)
            with zf.open(member) as src, open(dest_path, "wb") as dst:
                shutil.copyfileobj(src, dst)
            restored += 1
    return restored

def clear_min_cache():
    if os.path.isdir(MIN_CACHE_DIR):
        shutil.rmtree(MIN_CACHE_DIR, ignore_errors=True)


def delete_daily_cache_for_date(date_obj):
    """
    删除指定日期的日线缓存数据
    """
    if not os.path.exists(CACHE_FILE):
        return False
    try:
        df = pd.read_parquet(CACHE_FILE)
        if df.empty:
            return False
        
        # 确保日期列类型一致
        df['日期'] = pd.to_datetime(df['日期'])
        target_ts = pd.Timestamp(date_obj)
        
        # 过滤掉该日期的数据
        new_df = df[df['日期'].dt.date != target_ts.date()].copy()
        
        if len(new_df) < len(df):
            new_df.to_parquet(CACHE_FILE)
            logger.info("已删除日期 %s 的缓存数据", date_obj)
            return True
        return False
    except Exception as e:
        logger.error("删除日线缓存失败: %s", e)
        return False

def refetch_daily_data(date_obj):
    """
    强制重新获取指定日期的日线数据并更新缓存
    """
    try:
        date_str = date_obj.strftime("%Y%m%d")
        logger.info("开始修复/重取日期 %s 的数据", date_str)
        
        # 1. 获取成分股
        try:
            cons_df = ak.index_stock_cons(symbol="000300")
        except:
            cons_df = None
            
        # 如果获取不到，尝试从现有缓存中提取代码列表 (假设缓存里其他天的数据是好的)
        if cons_df is None or cons_df.empty:
             if os.path.exists(CACHE_FILE):
                 cached_df = pd.read_parquet(CACHE_FILE)
                 if not cached_df.empty:
                     codes = cached_df['代码'].unique().tolist()
                     # 构造伪 cons_df
                     cons_df = pd.DataFrame({'代码': codes, '名称': ['']*len(codes)})
                     logger.info("使用缓存中的代码列表进行修复: %s 个", len(codes))

        if cons_df is None or cons_df.empty:
            return False, "无法获取成分股列表"

        if 'variety' in cons_df.columns:
            code_col = 'variety'
        elif '品种代码' in cons_df.columns:
            code_col = '品种代码'
        else:
            code_col = cons_df.columns[0]
        
        stock_list = cons_df[code_col].tolist()
        
        # 2. 定义单日获取函数 (复用 fetch_biying_daily / akshare)
        providers = get_provider_order()
        licence = get_biying_licence()
        
        new_rows = []
        
        # 使用多线程加速
        def _worker(code):
            # 优先顺序
            row = None
            for p in providers:
                if p == 'biying' and licence:
                    try:
                        # fetch_biying_daily 返回的是 DataFrame
                        d = fetch_biying_daily(code, date_str, date_str, licence)
                        if d is not None and not d.empty:
                            return d.assign(代码=code)
                    except:
                        pass
                
                # akshare block removed as part of migration to Biying
                pass
            return None

        # 简单进度显示（在日志中）
        ctx = get_script_run_ctx()
        def _worker_wrapper(code):
             if ctx:
                 add_script_run_ctx(threading.current_thread(), ctx)
             return _worker(code)

        with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
            project = {executor.submit(_worker_wrapper, c): c for c in stock_list}
            for future in concurrent.futures.as_completed(project):
                res = future.result()
                if res is not None and not res.empty:
                    new_rows.append(res)
        
        if not new_rows:
            return False, "未能获取到任何有效数据"
            
        # 3. 合并与保存
        new_df = pd.concat(new_rows, ignore_index=True)
        # 确保类型
        new_df['日期'] = pd.to_datetime(new_df['日期'])
        new_df['涨跌幅'] = pd.to_numeric(new_df['涨跌幅'], errors='coerce')
        new_df['成交额'] = pd.to_numeric(new_df['成交额'], errors='coerce')
        new_df['收盘'] = pd.to_numeric(new_df['收盘'], errors='coerce')
        
        # 读取旧缓存并剔除当日数据
        if os.path.exists(CACHE_FILE):
             old_df = pd.read_parquet(CACHE_FILE)
             old_df['日期'] = pd.to_datetime(old_df['日期'])
             # 剔除
             target_ts = pd.Timestamp(date_obj)
             old_df = old_df[old_df['日期'].dt.date != target_ts.date()]
             final_df = pd.concat([old_df, new_df], ignore_index=True)
        else:
             final_df = new_df
        
        # 补充名称
        final_df = _refresh_cached_names(final_df)
        final_df = final_df.sort_values('日期')
        
        final_df.to_parquet(CACHE_FILE)
        return True, f"成功修复，获取到 {len(new_rows)} 只股票数据"

    except Exception as e:
        logger.error("修复数据失败: %s", e)
        return False, str(e)

def get_start_date(years_back=2):
    """计算 N 年前的日期，返回 YYYYMMDD 字符串"""
    target = datetime.now() - timedelta(days=365 * years_back)
    return target.strftime("%Y%m%d")

def fetch_history_data(index_pool="000300"):
    """
    获取成分股历史数据。支持不同指数池切换。
    index_pool: "000300" (沪深300), "000905" (中证500), "000852" (中证1000)
    """
    # 映射文件与名称
    pool_meta = {
        "000300": {"name": "csi300", "desc": "沪深300"},
        "000905": {"name": "csi500", "desc": "中证500"},
        "000852": {"name": "csi1000", "desc": "中证1000"}
    }
    meta = pool_meta.get(index_pool, pool_meta["000300"])
    file_key = meta["name"]
    pool_desc = meta["desc"]
    
    current_cache_file = f"data/{file_key}_history_cache.parquet"
    
    logger.info(f"开始加载历史数据 Pool={index_pool} ({pool_desc})")
    
    # 确保 data 目录存在
    if not os.path.exists("data"):
        os.makedirs("data")
        
    cached_df = pd.DataFrame()
    last_cached_date = None
    
    providers = get_provider_order()
    licence = get_biying_licence()

    # 1. 尝试加载本地缓存
    if os.path.exists(current_cache_file):
        try:
            cached_df = pd.read_parquet(current_cache_file)
            if not cached_df.empty:
                last_cached_date = cached_df['日期'].max().date()
                st.toast(f"✅ [{pool_desc}] 日线行情已就绪: {last_cached_date}")
        except Exception as e:
            st.error(f"读取[{pool_desc}]缓存失败: {e}")

    # 2. 计算需要下载的时间范围
    today = datetime.now().date()
    
    if last_cached_date:
        if last_cached_date >= today:
             return _refresh_cached_names(cached_df)
        start_date_str = (last_cached_date + timedelta(days=1)).strftime("%Y%m%d")
    else:
        # 如果是首次下载，默认下载2年
        start_date_str = get_start_date(2)
        
    end_date_str = today.strftime("%Y%m%d")

    # 如果不需要更新
    if start_date_str > end_date_str:
        return _refresh_cached_names(cached_df)

    # -------------------------------------------------------------------------
    # 开始下载更新流程
    # -------------------------------------------------------------------------
    status_text = st.empty()
    status_text.info(f"⏳ 正在更新 {pool_desc} 成分股数据 ({start_date_str}-{end_date_str})...")
    progress_bar = st.progress(0)
    
    try:
        # A. 获取成分股列表 (优先 Biying, 其次 Biying Stock List 过滤? 不推荐, 再次 AkShare)
        cons_codes = []
        
        # 1. Biying Interface (Requires implementation in providers.py)
        if licence:
            from core.providers import fetch_biying_index_cons
            # 注意: Biying 的指数代码可能不一样, 但通用标准是一样的
            try:
                cons_codes = fetch_biying_index_cons(index_pool, licence)
            except Exception as e:
                logger.warning(f"Biying index cons err: {e}")

        # 2. AkShare Fallback (If Biying failed or empty)
        if not cons_codes:
            msg = f"正在尝试从 AkShare 获取 {pool_desc} 成分股..."
            if licence: msg += " (Biying获取为空)"
            st.write(msg)
            
            try:
                # AkShare 接口: index_stock_cons
                cons_df = ak.index_stock_cons(symbol=index_pool)
                cons_codes = cons_df['variety'].tolist()
            except Exception as e:
                logger.warning(f"AkShare index cons failed: {e}")
        
        # 3. Last resort fallback / check
        if not cons_codes:
             st.error(f"❌ 无法获取 {pool_desc} 成分股列表，请检查网络或配置。")
             return _refresh_cached_names(cached_df)

        # Filter valid codes
        cons_codes = [c for c in cons_codes if str(c).isdigit() and len(str(c))==6]
        logger.info(f"Target Cons Count: {len(cons_codes)}")

        # B. 并发下载日线
        new_dfs = []
        total_stocks = len(cons_codes)
        max_workers = 30 if licence else 5
        
        ctx = get_script_run_ctx()
        
        def _fetch_stock_daily(code):
            if ctx:
                add_script_run_ctx(threading.current_thread(), ctx) # Attach context
                
            # 1. Try Biying (Priority)
            if licence:
                from core.providers import _fetch_biying_history_raw
                # Biying history raw returns list of dicts or rows
                try:
                    rows = _fetch_biying_history_raw(code, start_date_str, end_date_str, "daily", licence, adj="qfq")
                    if rows:
                        _df = pd.DataFrame(rows)
                        # Map columns based on observed keys from Biying documentation or typical structure
                        # Usually: d(date), o(open), c(close), h(high), l(low), v(vol), e(amount), zf(change pct)
                        
                        rename_map = {}
                        if 'd' in _df.columns: rename_map['d'] = '日期'
                        if 'c' in _df.columns: rename_map['c'] = '收盘'
                        if 'e' in _df.columns: rename_map['e'] = '成交额'
                        if 'zf' in _df.columns: rename_map['zf'] = '涨跌幅' # Percentage
                        
                        # Fallback for keys if different
                        if not rename_map and 'date' in _df.columns:
                             rename_map = {'date': '日期', 'close': '收盘', 'amount': '成交额', 'pct_chg': '涨跌幅'}

                        if '日期' in rename_map.values() or 'd' in _df.columns:
                            _df = _df.rename(columns=rename_map)
                            # Ensure columns exist
                            if '收盘' in _df.columns:
                                _df['代码'] = code
                                _df['日期'] = pd.to_datetime(_df['日期'])
                                # Fill missing cols
                                for col in ['涨跌幅', '成交额']:
                                    if col not in _df.columns: _df[col] = 0.0
                                return _df[['日期', '收盘', '涨跌幅', '成交额', '代码']]
                except Exception as e:
                    pass # Try next provider
            
            # 2. AkShare fallback removed as requested.
            # If Biying fails, we return None.
            return None

        # Execute
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_code = {executor.submit(_fetch_stock_daily, code): code for code in cons_codes}
            
            completed_count = 0
            for future in concurrent.futures.as_completed(future_to_code):
                completed_count += 1
                if completed_count % 20 == 0 or completed_count == total_stocks:
                    progress_bar.progress(completed_count / total_stocks)
                
                res = future.result()
                if res is not None and not res.empty:
                    new_dfs.append(res)
        
        progress_bar.empty()
        
        # Merge Results
        if new_dfs:
            df_new_all = pd.concat(new_dfs, ignore_index=True)
            # Type conversion
            df_new_all['日期'] = pd.to_datetime(df_new_all['日期'])
            for col in ['收盘', '涨跌幅', '成交额']:
                if col in df_new_all.columns:
                    df_new_all[col] = pd.to_numeric(df_new_all[col], errors='coerce')
            
            # Append to cache
            if not cached_df.empty:
                # Remove overlaps
                cached_df = cached_df[cached_df['日期'] < pd.to_datetime(start_date_str)]
                final_df = pd.concat([cached_df, df_new_all], ignore_index=True)
            else:
                final_df = df_new_all
            
            # Save
            final_df = final_df.sort_values(['代码', '日期'])
            status_text.success(f"✅ [{pool_desc}] 更新完成: {len(df_new_all)} 条新记录")
            
            final_df = _refresh_cached_names(final_df)
            final_df.to_parquet(current_cache_file)
            
            return final_df
        else:
            status_text.warning("未获取到新数据 (可能是非交易日)")
            return _refresh_cached_names(cached_df)

    except Exception as e:
        status_text.error(f"[{pool_desc}] 更新失败: {e}")
        logger.error(f"History update failed: {e}")
        return _refresh_cached_names(cached_df)


def _refresh_cached_names(df):
    if df.empty: return df
    
    # 尝试读取通用映射
    name_map = {}
    if os.path.exists(NAME_MAP_FILE):
        try:
            with open(NAME_MAP_FILE, 'r', encoding='utf-8') as f:
                name_map = json.load(f)
        except:
            pass
            
    # 只更新名称列，保留其他
    if '代码' in df.columns:
        # 如果 df 中没有名称列，或者我们想更新它
        df['名称'] = df['代码'].apply(lambda c: name_map.get(str(c), str(c)))
        
    return df

# -----------------------------------------------------------------------------
@st.cache_data(ttl=3600*24, show_spinner=False)
def fetch_cached_min_data(symbol, date_str, is_index=False, period=DEFAULT_MIN_PERIOD, raise_on_error=False):
    """
    ????????????????????
    ????????????????????
    params:
    period: '5', '15', '30', '60'
    """
    date_str_norm, date_key = _normalize_date_str(date_str)
    cache_path = _min_cache_path(symbol, date_key, period, is_index)
    cached_df = _read_min_cache(cache_path)
    if cached_df is not None and not cached_df.empty:
        logger.info("??????: code=%s date=%s period=%s index=%s path=%s", symbol, date_str_norm, period, is_index, cache_path)
        return cached_df

    providers = get_provider_order()
    licence = get_biying_licence()
    logger.info("??????????????: code=%s date=%s period=%s index=%s provider_order=%s", symbol, date_str_norm, period, is_index, providers)

    start_time = f"{date_str_norm} 09:30:00"
    end_time = f"{date_str_norm} 15:00:00"

    if not hasattr(fetch_cached_min_data, "current_backoff"):
        fetch_cached_min_data.current_backoff = 0

    max_retries = 3
    last_err = None

    def _normalize_biying_period(p):
        p = str(p)
        if p in ("5", "15", "30", "60"):
            return p
        return "5"

    def _try_biying():
        nonlocal last_err
        if not licence:
            logger.info("?? licence ??????????")
            return None
        period_use = _normalize_biying_period(period)
        try:
            df = fetch_biying_intraday(symbol, date_str_norm, period_use, licence, is_index=is_index)
            if df is not None and not df.empty:
                logger.info("????????: code=%s date=%s period=%s rows=%s", symbol, date_str_norm, period_use, len(df))
                _write_min_cache(cache_path, df)
                return df
            logger.warning("?????????: code=%s date=%s period=%s", symbol, date_str_norm, period_use)
        except Exception as exc:
            last_err = exc
            logger.warning("????????: code=%s date=%s period=%s err=%s", symbol, date_str_norm, period_use, exc)
        return None

    # _try_akshare removed. Biying is the sole provider.

    for provider in providers:
        if provider == "biying":
            df = _try_biying()
            if df is not None and not df.empty:
                return df
        elif provider == "akshare":
            # AkShare removed
            pass

    if raise_on_error and last_err is not None:
        raise last_err
    return None
def background_prefetch_task(date_list, origin_df):
    """
    后台线程：执行数据预取 (并发版)。
    """
    total_dates = len(date_list)
    logger.info("后台任务开始预取 %s 天数据 (并发数: %s)", total_dates, INTRADAY_WORKERS)
    
    indices_codes = ["000300", "000001", "399001", "000905", "000852"]
    
    # 获取当前上下文
    ctx = get_script_run_ctx()

    def _fetch_one(args):
        t_code, t_date, t_is_index = args
        try:
           fetch_cached_min_data(t_code, t_date, is_index=t_is_index, period=DEFAULT_MIN_PERIOD)
        except Exception as e:
           logger.warning("后台任务获取失败: code=%s date=%s err=%s", t_code, t_date, e)

    def _worker_wrapper(args):
        if ctx:
             add_script_run_ctx(threading.current_thread(), ctx)
        _fetch_one(args)

    # 全局线程池
    with concurrent.futures.ThreadPoolExecutor(max_workers=INTRADAY_WORKERS) as executor:
        for i, d in enumerate(date_list):
            d_str = d.strftime("%Y-%m-%d")
            logger.info("后台任务处理中: %s (%s/%s)", d_str, i + 1, total_dates)
            
            # 筛选
            daily = origin_df[origin_df['日期'].dt.date == d]
            if daily.empty: continue
            
            # Top 25
            top_stocks = daily.sort_values('成交额', ascending=False).head(25)['代码'].tolist()
            
            # 任务列表
            tasks = []
            for code in indices_codes: tasks.append((code, d_str, True))
            for code in top_stocks: tasks.append((code, d_str, False))
            
            # 提交当前日期的所有任务
            futures = [executor.submit(_worker_wrapper, task) for task in tasks]
            
            # 等待当前日期完成，再进行下一天 (便于进度跟踪)
            concurrent.futures.wait(futures)
            
            # 极短休眠
            time.sleep(0.01)
    
    logger.info("后台任务已完成")


def fetch_intraday_data_v2(stock_codes, target_date_str, period=DEFAULT_MIN_PERIOD):
    """
    获取指定股票列表 + 三大指数 的分钟级数据 (并发版)。
    v2: 增加上证、深证指数，优化缓存，原子化调用。
    v3: 引入多线程并发加速
    """
    results = [] 
    failures = [] 
    
    # 定义需要获取的指数
    indices_map = {
        "000300": "📊 沪深300",
        "000001": "📈 上证指数",
        "399001": "📉 深证成指",
        "000905": "📊 中证500",
        "000852": "📊 中证1000"
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
        
    stats = {'total': len(tasks), 'success': 0, 'failed': 0, 'cache': 0, 'network': 0}

    def _worker(task):
        is_index = (task['type'] == 'index')
        providers = get_provider_order()
        api_name = "->".join(providers) if providers else "unknown"
        _, date_key = _normalize_date_str(target_date_str)
        cache_path = _min_cache_path(task['code'], date_key, period, is_index)
        cached_df = _read_min_cache(cache_path)
        if cached_df is not None and not cached_df.empty:
            item = {
                'code': task['code'],
                'name': task['name'],
                'data': cached_df,
                'turnover': task['to_val'],
                'is_index': is_index
            }
            return item, None, 'cache'
        try:
            data = fetch_cached_min_data(task['code'], target_date_str, is_index=is_index, period=period, raise_on_error=True)
            if data is not None and not data.empty:
                item = {
                    'code': task['code'],
                    'name': task['name'],
                    'data': data,
                    'turnover': task['to_val'],
                    'is_index': is_index
                }
                return item, None, 'network'
            err = {
                'code': task['code'],
                'name': task['name'],
                'date': target_date_str,
                'period': period,
                'api': api_name,
                'reason': '\u63a5\u53e3\u8fd4\u56de\u7a7a',
                'is_index': is_index,
                'source': 'network'
            }
            return None, err, 'network'
        except Exception as e:
            err = {
                'code': task['code'],
                'name': task['name'],
                'date': target_date_str,
                'period': period,
                'api': api_name,
                'reason': str(e),
                'is_index': is_index,
                'source': 'network'
            }
            return None, err, 'network'

    # 并发执行
    # 线程数不宜过多，以免触发反爬限制，10-20左右较为安全
    if INTRADAY_WORKERS <= 1:
        logger.info("\u5206\u65f6\u62c9\u53d6\u6a21\u5f0f: \u4e32\u884c delay=%.2fs", INTRADAY_DELAY_SEC)
        for t in tasks:
            item, err, source = _worker(t)
            if item:
                results.append(item)
                stats['success'] += 1
            if err:
                failures.append(err)
                stats['failed'] += 1
            if source in stats:
                stats[source] += 1
            if source == 'network' and INTRADAY_DELAY_SEC > 0:
                time.sleep(INTRADAY_DELAY_SEC)
    else:
        logger.info("\u5206\u65f6\u62c9\u53d6\u6a21\u5f0f: \u5e76\u53d1 workers=%s", INTRADAY_WORKERS)
        # ????????????????
        ctx = get_script_run_ctx()
        def _worker_wrapper(t):
            if ctx:
                add_script_run_ctx(threading.current_thread(), ctx)
            return _worker(t)

        with concurrent.futures.ThreadPoolExecutor(max_workers=INTRADAY_WORKERS) as executor:
            future_to_task = {executor.submit(_worker_wrapper, t): t for t in tasks}
            
            for future in concurrent.futures.as_completed(future_to_task):
                item, err, source = future.result()
                if item:
                    results.append(item)
                    stats['success'] += 1
                if err:
                    failures.append(err)
                    stats['failed'] += 1
                if source in stats:
                    stats[source] += 1

    return results, failures, stats

