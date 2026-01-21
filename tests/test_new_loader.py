import sys
import os
sys.path.append(os.getcwd())

from modules.data_loader import fetch_history_data, fetch_cached_min_data
from modules.config import STOCK_POOLS

import tushare as ts
from modules.config import TUSHARE_TOKEN

def debug_tushare_api():
    print(f"Debug Token: {TUSHARE_TOKEN[:6]}...")
    ts.set_token(TUSHARE_TOKEN)
    pro = ts.pro_api()
    
    codes_to_try = ['000300.SH', '399300.SZ', '000300', '399300']
    
    for c in codes_to_try:
        try:
            print(f"Testing index_member for {c}...")
            df = pro.index_member(index_code=c)
            if df is not None and not df.empty:
                print(f"Success with {c}!")
                print("Columns:", df.columns.tolist())
                print(df.head(2))
            else:
                print(f"Empty result for {c}")
        except Exception as e:
            print(f"Index Member Error ({c}): {e}")
            
    try:
        print("Testing legacy ts.get_hs300s()...")
        df = ts.get_hs300s()
        if df is not None and not df.empty:
            print("Legacy Success!")
            print(df.head(2))
        else:
            print("Legacy Empty/None")
    except Exception as e:
        print(f"Legacy Error: {e}")



def test_history():
    print("Testing fetch_history_data...")
    # This might fail without token, but let's see the error message
    df = fetch_history_data("沪深300 (大盘)")
    if df.empty:
        print("Result is empty (Expected if invalid token)")
    else:
        print(f"Got {len(df)} rows")
        print(df.head())

def test_min_data():
    print("Testing fetch_cached_min_data...")
    # Get a recent trading date (e.g. yesterday)
    import datetime
    yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    print(f"Testing for date: {yesterday}")
    
    # 000001
    df = fetch_cached_min_data("000001", yesterday, is_index=False)
    if df.empty:
        print("Min data empty")
    else:
        print(f"Got {len(df)} min rows")
        print(df.head())

if __name__ == "__main__":
    # debug_tushare_api()
    # test_history()
    test_min_data()
