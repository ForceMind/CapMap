import argparse
from datetime import datetime, timedelta
import sys

import akshare as ak
import pandas as pd


def _pick_code_cols(df):
    if "variety" in df.columns:
        return "variety", "name"
    if "品种代码" in df.columns:
        return "品种代码", "品种名称"
    return df.columns[0], df.columns[1]


def _normalize_codes(series: pd.Series) -> pd.Series:
    s = series.astype(str)
    s = s.str.extract(r"(\d{6})", expand=False).fillna(s)
    return s.str.zfill(6)


def main() -> int:
    parser = argparse.ArgumentParser(description="Manual fetch sanity check for CSI index pools.")
    parser.add_argument("--index", default="000905", help="index code, e.g. 000905 for CSI500")
    parser.add_argument("--days", type=int, default=10, help="days of history to pull for sample stocks")
    parser.add_argument("--sample", type=int, default=5, help="sample size of stocks to test")
    parser.add_argument("--spot", action="store_true", help="also test stock_zh_a_spot_em")
    args = parser.parse_args()

    print(f"[info] akshare version: {getattr(ak, '__version__', 'unknown')}")
    print(f"[info] target index: {args.index}")

    try:
        cons_df = ak.index_stock_cons(symbol=args.index)
    except Exception as exc:
        print(f"[error] index_stock_cons failed: {exc}")
        return 2

    if cons_df is None or cons_df.empty:
        print("[error] index_stock_cons returned empty data")
        return 3

    code_col, name_col = _pick_code_cols(cons_df)
    codes = _normalize_codes(cons_df[code_col])
    names = cons_df[name_col].astype(str).tolist() if name_col in cons_df.columns else []
    total = len(codes)
    unique_total = len(set(codes.tolist()))
    print(f"[info] constituents: {total}, unique codes: {unique_total}")

    sample_codes = list(dict.fromkeys(codes.tolist()))[: max(1, args.sample)]
    today = datetime.now().date()
    start_date = (today - timedelta(days=args.days)).strftime("%Y%m%d")
    end_date = today.strftime("%Y%m%d")
    print(f"[info] sample date range: {start_date} - {end_date}")

    for idx, code in enumerate(sample_codes, start=1):
        name = names[codes.tolist().index(code)] if names else ""
        try:
            df_hist = ak.stock_zh_a_hist(
                symbol=code,
                start_date=start_date,
                end_date=end_date,
                adjust="qfq",
            )
        except Exception as exc:
            print(f"[error] {idx}/{len(sample_codes)} {code} {name} -> hist error: {exc}")
            continue

        if df_hist is None or df_hist.empty:
            print(f"[warn] {idx}/{len(sample_codes)} {code} {name} -> empty hist")
            continue

        try:
            date_col = df_hist["日期"].astype(str)
            min_d = date_col.min()
            max_d = date_col.max()
        except Exception:
            min_d = "unknown"
            max_d = "unknown"
        print(f"[ok] {idx}/{len(sample_codes)} {code} {name} -> rows {len(df_hist)} ({min_d} .. {max_d})")

    if args.spot:
        try:
            spot_df = ak.stock_zh_a_spot_em()
            if spot_df is None or spot_df.empty:
                print("[warn] stock_zh_a_spot_em returned empty data")
            else:
                print(f"[info] spot rows: {len(spot_df)}, cols: {list(spot_df.columns)[:8]}")
        except Exception as exc:
            print(f"[error] stock_zh_a_spot_em failed: {exc}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
