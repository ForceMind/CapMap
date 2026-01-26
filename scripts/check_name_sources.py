#!/usr/bin/env python3
import argparse

import akshare as ak


def _print_table(df):
    if df is None or df.empty:
        print("(not found)")
        return
    print(df.to_string(index=False))


def main():
    parser = argparse.ArgumentParser(
        description="Compare name sources for given stock codes."
    )
    parser.add_argument(
        "codes",
        nargs="*",
        default=["601888", "600111"],
        help="Stock codes to check (default: 601888 600111)",
    )
    args = parser.parse_args()

    codes = [str(c) for c in args.codes]
    print("Codes:", ", ".join(codes))

    print("\nindex_stock_cons (000300):")
    try:
        cons = ak.index_stock_cons(symbol="000300")
        if cons is None or cons.empty:
            print("(empty)")
        else:
            if "variety" in cons.columns:
                code_col, name_col = "variety", "name"
            elif "\u54c1\u79cd\u4ee3\u7801" in cons.columns:
                code_col, name_col = "\u54c1\u79cd\u4ee3\u7801", "\u54c1\u79cd\u540d\u79f0"
            else:
                code_col, name_col = cons.columns[0], cons.columns[1]
            cons[code_col] = cons[code_col].astype(str)
            subset = cons[cons[code_col].isin(codes)][[code_col, name_col]]
            _print_table(subset)
    except Exception as exc:
        print("ERROR:", exc)

    print("\nstock_zh_a_spot_em:")
    try:
        spot = ak.stock_zh_a_spot_em()
        if spot is None or spot.empty:
            print("(empty)")
        else:
            code_col, name_col = "\u4ee3\u7801", "\u540d\u79f0"
            spot[code_col] = spot[code_col].astype(str)
            subset = spot[spot[code_col].isin(codes)][[code_col, name_col]]
            _print_table(subset)
    except Exception as exc:
        print("ERROR:", exc)


if __name__ == "__main__":
    main()
