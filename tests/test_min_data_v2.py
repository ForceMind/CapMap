import akshare as ak
import pandas as pd
from datetime import datetime

print("Testing stock_zh_a_hist_min_em...")
try:
	df = ak.stock_zh_a_hist_min_em(symbol="000001", start_date="2023-12-01 09:30:00", end_date="2023-12-01 15:00:00", period="1", adjust="qfq")
	if df is not None and not df.empty:
		print("Success with full time string!")
		print(df.head(1))
	else:
		print("Fail full time string. Trying only date...")
		df = ak.stock_zh_a_hist_min_em(symbol="000001", start_date="2023-12-01", end_date="2023-12-01", period="1", adjust="qfq")
		if df is not None and not df.empty:
			 print("Success with date only!")
			 print(df.head(1))
		else:
			 print("Fan failed.")

except Exception as e:
	print("Error:", e)