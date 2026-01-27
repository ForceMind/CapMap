import argparse
import time
from datetime import datetime
import traceback
from pathlib import Path
import sys

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from modules import data_loader
from modules.config import STOCK_POOLS


def _safe_print(msg: str) -> None:
    try:
        print(msg)
    except UnicodeEncodeError:
        enc = sys.stdout.encoding or "utf-8"
        print(msg.encode(enc, errors="replace").decode(enc, errors="replace"))


class DummyPlaceholder:
    def __init__(self, label):
        self.label = label

    def text(self, msg):
        _safe_print(f"[ui:{self.label}] {msg}")
        return self

    def progress(self, value):
        try:
            pct = int(value * 100)
        except Exception:
            pct = value
        _safe_print(f"[progress:{self.label}] {pct}")
        return self

    def empty(self):
        return None


class DummyStreamlit:
    def __init__(self):
        self.session_state = {}

    def empty(self):
        return DummyPlaceholder("empty")

    def progress(self, value=0):
        return DummyPlaceholder("progress").progress(value)

    def toast(self, msg):
        _safe_print(f"[toast] {msg}")

    def warning(self, msg):
        _safe_print(f"[warn] {msg}")

    def error(self, msg):
        _safe_print(f"[error] {msg}")

    def success(self, msg):
        _safe_print(f"[ok] {msg}")

    def info(self, msg):
        _safe_print(f"[info] {msg}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Debug fetch_history_data with minimal UI stubs.")
    parser.add_argument("--pool", default="中证500 (中盘)", help="pool name in modules/config.py")
    parser.add_argument("--max-workers", type=int, default=1)
    parser.add_argument("--delay", type=float, default=0.5)
    parser.add_argument("--no-spot", action="store_true")
    args = parser.parse_args()

    if args.pool not in STOCK_POOLS:
        print(f"[error] pool not found: {args.pool}")
        print(f"[info] available pools: {list(STOCK_POOLS.keys())}")
        return 2

    # Patch Streamlit usage inside data_loader to avoid UI dependency in CLI.
    data_loader.st = DummyStreamlit()

    print(f"[info] start: {datetime.now().isoformat(timespec='seconds')}")
    print(f"[info] pool: {args.pool}")
    print(f"[info] workers: {args.max_workers}, delay: {args.delay}, spot: {not args.no_spot}")

    try:
        t0 = time.time()
        df = data_loader.fetch_history_data(
            pool_name=args.pool,
            allow_download=True,
            max_workers=args.max_workers,
            request_delay=args.delay,
            fetch_spot=(not args.no_spot),
        )
        elapsed = time.time() - t0
    except Exception:
        print("[error] fetch_history_data raised an exception:")
        print(traceback.format_exc())
        return 3

    if df is None or df.empty:
        print("[result] empty dataframe")
        return 1

    if "日期" in df.columns:
        date_min = pd.to_datetime(df["日期"]).min()
        date_max = pd.to_datetime(df["日期"]).max()
        print(f"[result] rows: {len(df)}, date range: {date_min} .. {date_max}")
    else:
        print(f"[result] rows: {len(df)}")

    print(f"[info] elapsed: {elapsed:.1f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
