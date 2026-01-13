import akshare as ak
import pandas as pd
from datetime import datetime

try:
    # 尝试获取 平安银行 (000001) 在 2024-01-08 的分钟数据
    df = ak.stock_zh_a_hist_min_em(symbol="000001", start_date="2024-01-08 09:30:00", end_date="2024-01-08 15:00:00", period="1", adjust="qfq")
    print("Empty?", df.empty if df is not None else "None")
    if df is not None and not df.empty:
        print(df.head())
        print(df.columns)
except Exception as e:
    print("Error:", e)
