import json
import logging
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime

import pandas as pd

LOGGER = logging.getLogger("capmap")

PROVIDER_CONFIG_FILE = "data/provider_config.json"
DEFAULT_PROVIDER_ORDER = ["biying", "akshare"]
BIYING_BASE_URL = os.environ.get("BIYING_BASE_URL", "https://api.biyingapi.com")
BIYING_QUOTA_FILE = "data/biying_quota.json"
BIYING_DAILY_LIMIT = 99999999999

def _check_and_consume_quota():
    # 商用版不限次数，直接返回 True
    return True, 0
    
    # try:

    #    today_str = datetime.now().strftime("%Y-%m-%d")
    #   data = {"date": today_str, "count": 0}
        
    #    if os.path.exists(BIYING_QUOTA_FILE):
    #          with open(BIYING_QUOTA_FILE, "r", encoding="utf-8") as f:
    #                loaded = json.load(f)
    #                if isinstance(loaded, dict) and loaded.get("date") == today_str:
    #                    data = loaded
    #       except Exception:
    #            pass

    #    if data["count"] >= BIYING_DAILY_LIMIT:
    #         return False, data["count"]

    #   data["count"] += 1
        
    #  try:
    #     os.makedirs(os.path.dirname(BIYING_QUOTA_FILE), exist_ok=True)
    #    with open(BIYING_QUOTA_FILE, "w", encoding="utf-8") as f:
    #            json.dump(data, f)
    #   except Exception:
    #       pass
    #       
    #   return True, data["count"]
    #except Exception as e:
    #   LOGGER.warning(f"Quota check failed: {e}")
        # If quota check fails, assume allowed but don't crash
    #    return True, 0


def _read_json_file(path):
    if not path or not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception as exc:
        LOGGER.warning("provider config read failed: %s", exc)
        return {}


def _normalize_provider(value):
    return str(value).strip().lower()


def _split_order(value):
    if not value:
        return []
    if isinstance(value, (list, tuple)):
        items = value
    else:
        items = str(value).split(",")
    order = []
    for item in items:
        name = _normalize_provider(item)
        if name and name not in order:
            order.append(name)
    return order


def load_provider_config():
    cfg = _read_json_file(PROVIDER_CONFIG_FILE)

    licence_env = os.environ.get("BIYING_LICENCE") or os.environ.get("BIYING_LICENSE")
    if licence_env:
        cfg["biying_licence"] = licence_env.strip()

    order_env = os.environ.get("CAPMAP_PROVIDER_ORDER")
    if order_env:
        cfg["provider_order"] = _split_order(order_env)

    provider_env = os.environ.get("CAPMAP_PROVIDER")
    if provider_env:
        cfg["provider_order"] = _split_order(provider_env)

    return cfg


def get_provider_order(cfg=None):
    if cfg is None:
        cfg = load_provider_config()
    order = _split_order(cfg.get("provider_order"))
    if not order:
        return DEFAULT_PROVIDER_ORDER[:]
    if "akshare" not in order:
        order.append("akshare")
    return order


def get_biying_licence(cfg=None):
    if cfg is None:
        cfg = load_provider_config()
    for key in ("biying_licence", "licence", "license"):
        value = cfg.get(key)
        if value:
            return str(value).strip()
    return ""


def _normalize_time_param(value):
    """
    Format time parameter for Biying API.
    Supports YYYYMMDD or YYYYMMDDHHMMSS.
    """
    if value is None:
        return ""
    try:
        dt = pd.to_datetime(value)
        # Check if we have non-zero time component
        if dt.hour == 0 and dt.minute == 0 and dt.second == 0:
             return dt.strftime("%Y%m%d")
        else:
             return dt.strftime("%Y%m%d%H%M%S")
    except Exception:
        text = str(value).replace("-", "").replace(":", "").replace(" ", "")
        return text


def _biying_market(symbol, is_index=False):
    code = str(symbol)
    if is_index:
        if code.startswith("399"):
            return "SZ"
        return "SH"
    if code.startswith("6"):
        return "SH"
    return "SZ"


def _build_biying_url(path, params=None):
    base = BIYING_BASE_URL.rstrip("/")
    url = f"{base}/{path.lstrip('/')}"
    if params:
        url = f"{url}?{urllib.parse.urlencode(params)}"
    return url


def _fetch_biying_json(url, timeout=20):
    # Check daily quota
    allowed, count = _check_and_consume_quota()
    if not allowed:
        LOGGER.warning(f"Biying daily quota reached ({count}/{BIYING_DAILY_LIMIT}). Switching to fallback.")
        return None

    retries = 3
    for i in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "capmap/1.0"})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                payload = resp.read()
            try:
                return json.loads(payload.decode("utf-8"))
            except Exception:
                try:
                    return json.loads(payload)
                except Exception:
                    # JSON parse error (suppress payload logging for privacy)
                    return None
        except urllib.error.HTTPError as e:
            if e.code == 429:
                LOGGER.warning(f"Biying API 429 Limit reached, retrying {i+1}/{retries} after sleep...")
                time.sleep(2 * (i + 1))
                continue
            # Log error code but avoid full URL which may contain licence
            LOGGER.warning(f"Biying API HTTP Error {e.code}: {e.reason}")
            return None
        except Exception as e:
            LOGGER.warning(f"Biying API request failed: {e}")
            return None
    return None


def _extract_biying_rows(payload):
    if payload is None:
        return []
    data = payload
    if isinstance(data, dict):
        for key in ("data", "result", "rows", "list"):
            if key in data:
                data = data[key]
                break
    if isinstance(data, dict):
        for key in ("data", "list", "rows"):
            if key in data:
                data = data[key]
                break
    if isinstance(data, list):
        return data
    return []


def fetch_biying_stock_list(licence):
    if not licence:
        return {}
    # 尝试多个可能的 stock list 接口
    # 1. /hslt/list/ (A股列表)
    url = _build_biying_url(f"/hslt/list/{urllib.parse.quote(licence)}")
    payload = _fetch_biying_json(url)
    rows = _extract_biying_rows(payload)
    
    if not rows:
        return {}

    df = pd.DataFrame(rows)
    code_col = None
    name_col = None
    for c in ("dm", "code", "symbol", "ts_code", "证券代码", "代码"):
        if c in df.columns:
            code_col = c
            break
            
    # Refined Name Column Search Strategy (2026-01-29)
    # 1. 优先匹配明确的中文简称字段 (jc)
    # 2. 其次匹配明确的中文全称字段 (mc, 名称)
    # 3. 最后才匹配模糊的字段 (name), 因为 'name' 有时可能是代码或英文
    candidate_cols = ["jc", "简称", "证券简称", "mc", "名称", "股票名称", "name"]
    for c in candidate_cols:
        if c in df.columns:
            # 增加一步校验：如果该列的值全是数字，或者和 code 列完全一样，说明不是合法的名字列，跳过
            sample_val = str(df[c].iloc[0]) if not df.empty else ""
            if sample_val.isdigit() and len(sample_val) >= 6:
                continue
            if code_col and code_col != c:
                 # 检查是否和 code 列完全重复 (这通常意味着 name 其实是 code)
                 if df[c].equals(df[code_col]):
                     continue
            
            name_col = c
            break
            
    if not code_col or not name_col:
        return {}
    df[code_col] = df[code_col].astype(str).str.strip()
    df[code_col] = df[code_col].str.replace(r"\\..*$", "", regex=True) # remove suffix like .SH
    df[name_col] = df[name_col].astype(str)
    return dict(zip(df[code_col], df[name_col]))


def fetch_biying_all_realtime(licence):
    """
    获取全市场实时行情快照 (替代 AkShare stock_zh_a_spot_em)
    API: https://all.biyingapi.com/hsrl/real/all/{licence}
    """
    if not licence:
        return pd.DataFrame()
        
    # 注意: all.biyingapi.com 可能不同于普通的 api.biyingapi.com
    # 但为了简单，如果 BIYING_BASE_URL 是默认的，我们手动构造
    base = "https://all.biyingapi.com"
    path = f"/hsrl/real/all/{urllib.parse.quote(licence)}"
    url = f"{base}{path}"
    
    payload = _fetch_biying_json(url, timeout=30)
    rows = _extract_biying_rows(payload)
    if not rows:
        return pd.DataFrame()
        
    df = pd.DataFrame(rows)
    # 按需重命名列以匹配 AkShare 或内部格式
    # Biying columns: dm(代码), mc(名称), p(现价), zf(涨幅), cje(成交额)...
    rename_map = {
        "dm": "code", "mc": "name", 
        "p": "close", "zf": "pct_chg",
        "cje": "amount", "cjl": "volume",
        "o": "open", "h": "high", "l": "low", 
        "z": "prev_close"
    }
    # 过滤掉不存在的
    rename_map = {k: v for k, v in rename_map.items() if k in df.columns}
    df = df.rename(columns=rename_map)
    return df

def fetch_biying_index_list(licence):
    """获取所有指数列表"""
    if not licence:
        return pd.DataFrame()
    url = _build_biying_url(f"/hsindex/list/{urllib.parse.quote(licence)}")
    payload = _fetch_biying_json(url)
    rows = _extract_biying_rows(payload)
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)

def fetch_biying_stock_info(code, licence):
    """获取个股资料"""
    if not licence: return None
    url = _build_biying_url(f"/hscp/gsjj/{code}/{urllib.parse.quote(licence)}")
    payload = _fetch_biying_json(url)
    # Payload likely a dict directly or list of 1
    if isinstance(payload, list) and len(payload) > 0:
        return payload[0]
    return payload


def fetch_biying_index_cons(index_code, licence):
    """获取指数成分股列表
    
    API Doc: /hszg/gg/{code}/{licence}
    Ref: https://www.biyingapi.com/doc_hs
    Need to map standard index code to 'concept tree' code.
    Samples:
      000300 -> hs300
      000905 -> zhishu_000905
      399001 -> zhishu_399001
      000016 -> zhishu_000016 (Sz50)
      000688 -> zhishu_000688 (KC50)
    """
    if not licence:
        return []
        
    # Standardize input code (remove sh/sz prefix)
    raw_code = str(index_code).lower().replace("sh", "").replace("sz", "").split(".")[0]
    
    # Mapping logic
    if raw_code == "000300":
        target_code = "hs300"
    else:
        # Default pattern seems to be zhishu_{code}
        target_code = f"zhishu_{raw_code}"

    # Try specific codes first, then generic
    candidates = [target_code]
    # In case 399001 was passed as sz399001
    
def fetch_biying_index_cons(index_code, licence):
    """获取指数成分股列表"""
    if not licence:
        return []
        
    # 清理代码
    raw_code = str(index_code).split(".")[0]
    
    # 映射表: 常用指数 -> Biying 内部代码
    # 000300 -> hs300
    # 000905 -> zhishu_000905
    # 000852 -> zhishu_000852
    candidates = []

    if raw_code == "000300":
        candidates.append("hs300")
    elif raw_code == "399001":
        candidates.append("zhishu_399001")
    elif raw_code == "000001":
        candidates.append("zhishu_000001")
    else:
        # 默认尝试 zhishu_ 前缀
        candidates.append(f"zhishu_{raw_code}")

    # 兜底：尝试原始代码 (某些旧接口可能还活着)
    candidates.append(raw_code)

    # 路径：/hszg/gg/{code}/{licence}
    path_template = "/hszg/gg/{code}/{licence}"
    
    rows = []
    
    for c in candidates:
        try:
            path = path_template.format(code=c, licence=urllib.parse.quote(licence))
            url = _build_biying_url(path)
            payload = _fetch_biying_json(url)
            rows = _extract_biying_rows(payload)
            if rows:
                break
        except Exception:
            pass

    # 如果还为空，尝试旧接口路径 (万一)
    if not rows:
         try:
            # 尝试 /zscons/
            url = _build_biying_url(f"/zscons/{raw_code}/{urllib.parse.quote(licence)}")
            payload = _fetch_biying_json(url)
            rows = _extract_biying_rows(payload)
         except Exception:
            pass

    cons = []
    for row in rows:
        # 寻找代码字段 (Biying 通常返回 'dm')
        code = None
        for k in ["dm", "code", "symbol", "ts_code"]:
            if k in row:
                code = str(row[k])
                break
        if code:
            # 清理后缀
            if "." in code:
                code = code.split(".")[0]
            cons.append(code)
            
    if cons:
        LOGGER.info(f"Biying Index Cons success: {raw_code} -> {len(cons)} stocks")
        
    return cons




def _fetch_biying_history_raw(symbol, start_date, end_date, period, licence, is_index=False, adj="n"):
    if not licence:
        return []
    
    # Map Params to Biying API format
    # Period: daily -> d
    # Adjust: qfq -> f, hfq -> b
    periods_str = str(period).lower()
    if periods_str in ["daily", "1day"]:
        period = "d"
    
    # Force adj='n' for minute level data (5, 15, 30, 60)
    # Biying Doc: "Minute level without adjustment data, corresponding parameter is n"
    if str(period) in ["1", "5", "15", "30", "60"]:
        adj = "n"
    else:
        # Only map adjust for non-minute periods
        if adj == "qfq":
            adj = "f"
        elif adj == "hfq":
            adj = "b"
        
    market = _biying_market(symbol, is_index=is_index)
    start_key = _normalize_time_param(start_date)
    end_key = _normalize_time_param(end_date)
    code = f"{symbol}.{market}"
    if is_index:
        path = f"/hsindex/history/{code}/{period}/{licence}"
    else:
        path = f"/hsstock/history/{code}/{period}/{adj}/{licence}"
    url = _build_biying_url(path, params={"st": start_key, "et": end_key})
    payload = _fetch_biying_json(url)
    return _extract_biying_rows(payload)


def _parse_biying_time(value):
    if value is None:
        return pd.NaT
    if isinstance(value, (int, float)):
        ts = int(value)
        if ts > 10**12:
            return pd.to_datetime(ts, unit="ms", errors="coerce")
        if ts > 10**9:
            return pd.to_datetime(ts, unit="s", errors="coerce")
    text = str(value).strip()
    if text.isdigit():
        ts = int(text)
        if ts > 10**12:
            return pd.to_datetime(ts, unit="ms", errors="coerce")
        if ts > 10**9:
            return pd.to_datetime(ts, unit="s", errors="coerce")
    return pd.to_datetime(text, errors="coerce")


def fetch_biying_intraday(symbol, date_str, period, licence, is_index=False):
    rows = _fetch_biying_history_raw(
        symbol,
        date_str,
        date_str,
        period,
        licence,
        is_index=is_index,
        adj="n",
    )
    if not rows:
        return None
    df = pd.DataFrame(rows)
    if df.empty:
        return None
    time_col = None
    for c in ("t", "time", "datetime", "日期", "时间"):
        if c in df.columns:
            time_col = c
            break
    close_col = None
    for c in ("c", "close", "收盘"):
        if c in df.columns:
            close_col = c
            break
    open_col = None
    for c in ("o", "open", "开盘"):
        if c in df.columns:
            open_col = c
            break
    high_col = None
    for c in ("h", "high", "最高"):
        if c in df.columns:
            high_col = c
            break
    low_col = None
    for c in ("l", "low", "最低"):
        if c in df.columns:
            low_col = c
            break
    vol_col = None
    for c in ("v", "vol", "volume", "成交量"):
        if c in df.columns:
            vol_col = c
            break

    if not time_col or not close_col:
        return None
    
    df["time"] = df[time_col].apply(_parse_biying_time)
    df["close"] = pd.to_numeric(df[close_col], errors="coerce")
    
    if open_col and open_col in df.columns:
        df["open"] = pd.to_numeric(df[open_col], errors="coerce")
    else:
        df["open"] = df["close"]
        
    if high_col and high_col in df.columns:
        df["high"] = pd.to_numeric(df[high_col], errors="coerce")
    else:
        df["high"] = df[["open", "close"]].max(axis=1)
        
    if low_col and low_col in df.columns:
        df["low"] = pd.to_numeric(df[low_col], errors="coerce")
    else:
        df["low"] = df[["open", "close"]].min(axis=1)

    if vol_col and vol_col in df.columns:
        df["volume"] = pd.to_numeric(df[vol_col], errors="coerce")
    else:
        df["volume"] = 0

    base = df["open"].iloc[0]
    if pd.isna(base) or base == 0:
        base = df["close"].iloc[0]
        
    df["pct_chg"] = (df["close"] - base) / base * 100
    
    return df[["time", "pct_chg", "open", "high", "low", "close", "volume"]].copy()


def fetch_biying_daily(symbol, start_date, end_date, licence, is_index=False, period="d"):
    rows = _fetch_biying_history_raw(
        symbol,
        start_date,
        end_date,
        period,
        licence,
        is_index=is_index,
        adj="n",
    )
    if not rows:
        return None
    df = pd.DataFrame(rows)
    if df.empty:
        return None
    date_col = None
    for c in ("t", "date", "日期", "时间"):
        if c in df.columns:
            date_col = c
            break
    close_col = None
    for c in ("c", "close", "收盘"):
        if c in df.columns:
            close_col = c
            break
    amount_col = None
    for c in ("a", "amount", "成交额"):
        if c in df.columns:
            amount_col = c
            break
    prev_close_col = None
    for c in ("pc", "preclose", "prev_close", "前收盘"):
        if c in df.columns:
            prev_close_col = c
            break
    if not date_col or not close_col:
        return None
    df["日期"] = df[date_col].apply(_parse_biying_time)
    df["收盘"] = pd.to_numeric(df[close_col], errors="coerce")
    if amount_col:
        df["成交额"] = pd.to_numeric(df[amount_col], errors="coerce")
    else:
        df["成交额"] = pd.NA
    if prev_close_col:
        prev = pd.to_numeric(df[prev_close_col], errors="coerce")
        df["涨跌幅"] = (df["收盘"] - prev) / prev * 100
    else:
        df["涨跌幅"] = df["收盘"].pct_change() * 100
    return df[["日期", "收盘", "涨跌幅", "成交额"]].copy()
