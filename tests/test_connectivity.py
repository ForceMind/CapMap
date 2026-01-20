import akshare as ak
import pandas as pd
import traceback
import os

# FORCE NO PROXY
os.environ['NO_PROXY'] = '*'
os.environ['http_proxy'] = ''
os.environ['https_proxy'] = ''
os.environ['HTTP_PROXY'] = ''
os.environ['HTTPS_PROXY'] = ''

from modules.config import STOCK_POOLS

def test_fetch():
    print("Testing AkShare connection...")
    pool_name = "沪深300 (大盘)"
    config = STOCK_POOLS.get(pool_name)
    index_code = config["code"]
    print(f"Fetching cons for {pool_name} ({index_code})...")
    
    try:
        cons_df = ak.index_stock_cons(symbol=index_code)
        if cons_df is None or cons_df.empty:
            print("❌ Result is Empty or None")
        else:
            print(f"✅ Success! Got {len(cons_df)} stocks.")
            print(cons_df.head(1))
            
            # Test Hist
            code = "000001"
            print(f"Testing History Fetch for {code}...")
            df = ak.stock_zh_a_hist(symbol=code, start_date="20240101", end_date="20240105", adjust="qfq")
            if df is not None and not df.empty:
                print(f"✅ History Success: {len(df)} rows")
            else:
                print("❌ History Failed")

    except Exception as e:

        print("❌ Error fetching cons:")
        traceback.print_exc()

if __name__ == "__main__":
    test_fetch()
