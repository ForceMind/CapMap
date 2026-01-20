import akshare as ak
import pandas as pd
from datetime import datetime, timedelta

def test_date(date_str):
    print(f"Testing {date_str}...")
    try:
        # 平安银行 000001
        start = f"{date_str} 09:30:00"
        end = f"{date_str} 15:00:00"
        df = ak.stock_zh_a_hist_min_em(symbol="000001", period="1", adjust="qfq", start_date=start, end_date=end)
        if df is not None:
            print(f"  Result length: {len(df)}")
            if not df.empty: print("  Date in df:", df['时间'].iloc[0])
        else:
            print("  Result: None")
    except Exception as e:
        print(f"  Error: {e}")

# 今天是？
today = datetime.now()
# 昨天
test_date((today - timedelta(days=1)).strftime("%Y-%m-%d"))
# 10天前
test_date((today - timedelta(days=10)).strftime("%Y-%m-%d"))
# 30天前
test_date((today - timedelta(days=30)).strftime("%Y-%m-%d"))
# 2024-01-08 (1 year ago?) well, 2026 now
# 2025-01-01
test_date("2025-01-01")
