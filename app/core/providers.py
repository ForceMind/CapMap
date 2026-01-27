import json
import logging
import os
import urllib.parse
import urllib.request

import pandas as pd

LOGGER = logging.getLogger("capmap")

PROVIDER_CONFIG_FILE = "data/provider_config.json"
DEFAULT_PROVIDER_ORDER = ["biying", "akshare"]
BIYING_BASE_URL = os.environ.get("BIYING_BASE_URL", "https://api.biyingapi.com")


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


def _normalize_yyyymmdd(value):
    if value is None:
        return ""
    try:
        dt = pd.to_datetime(value)
        return dt.strftime("%Y%m%d")
    except Exception:
        text = str(value).replace("-", "")
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
    req = urllib.request.Request(url, headers={"User-Agent": "capmap/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        payload = resp.read()
    try:
        return json.loads(payload.decode("utf-8"))
    except Exception:
        try:
            return json.loads(payload)
        except Exception:
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
    url = _build_biying_url(f"/hslt/list/{urllib.parse.quote(licence)}")
    payload = _fetch_biying_json(url)
    rows = _extract_biying_rows(payload)
    if not rows:
        return {}
    df = pd.DataFrame(rows)
    code_col = None
    name_col = None
    for c in ("code", "symbol", "ts_code", "证券代码", "代码"):
        if c in df.columns:
            code_col = c
            break
    for c in ("name", "名称", "证券简称", "股票名称"):
        if c in df.columns:
            name_col = c
            break
    if not code_col or not name_col:
        return {}
    df[code_col] = df[code_col].astype(str)
    df[name_col] = df[name_col].astype(str)
    return dict(zip(df[code_col], df[name_col]))


def _fetch_biying_history_raw(symbol, start_date, end_date, period, licence, is_index=False, adj="n"):
    if not licence:
        return []
    market = _biying_market(symbol, is_index=is_index)
    start_key = _normalize_yyyymmdd(start_date)
    end_key = _normalize_yyyymmdd(end_date)
    code = f"{symbol}.{market}"
    if is_index:
        path = f"/hsindex/history/{code}/{period}/{licence}"
    else:
        path = f"/hsstock/history/{code}/{period}/{adj}/{licence}"
    url = _build_biying_url(path, params={"st": start_key, "et": end_key})
    payload = _fetch_biying_json(url)
    return _extract_biying_rows(payload)


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
    if not time_col or not close_col:
        return None
    df["time"] = pd.to_datetime(df[time_col])
    df["close"] = pd.to_numeric(df[close_col], errors="coerce")
    if open_col and open_col in df.columns:
        base = pd.to_numeric(df[open_col], errors="coerce").iloc[0]
    else:
        base = df["close"].iloc[0]
    if pd.isna(base) or base == 0:
        base = df["close"].iloc[0]
    df["pct_chg"] = (df["close"] - base) / base * 100
    return df[["time", "pct_chg", "close"]].copy()


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
    df["日期"] = pd.to_datetime(df[date_col])
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
