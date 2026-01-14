import akshare as ak
import pandas as pd
from datetime import datetime, timedelta

def test_date(date_str, p='1'):
    try:
        start = f"{date_str} 09:30:00"
        end = f"{date_str} 15:00:00"
        df = ak.stock_zh_a_hist_min_em(symbol="000001", period=p, adjust="qfq", start_date=start, end_date=end)
        if df is not None and not df.empty:
            return True
        else:
            return False
    except Exception:
        return False

today = datetime.now()
print(f"Today is {today.strftime('%Y-%m-%d')}")

found_1min = 0
found_5min = 0

print("Scanning...")
for i in range(1, 30):
    d_str = (today - timedelta(days=i)).strftime("%Y-%m-%d")
    
    # Check 1 min
    if test_date(d_str, '1'):
        found_1min += 1
    
    # Check 5 min
    if test_date(d_str, '5'):
        found_5min += 1

print(f"In last 30 days: 1-min data days={found_1min}, 5-min data days={found_5min}")
