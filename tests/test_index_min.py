import akshare as ak
import pandas as pd

print("Testing index min data...")
try:
	df_sh = ak.stock_zh_a_hist_min_em(symbol="sh000001", period="1", adjust="qfq", start_date="2024-01-08 09:30:00", end_date="2024-01-08 15:00:00")
	if df_sh is not None and not df_sh.empty:
		print("SH000001 found:", len(df_sh))
	else:
		print("SH000001 not found")

	df_300 = ak.stock_zh_a_hist_min_em(symbol="sh000300", period="1", adjust="qfq", start_date="2024-01-08 09:30:00", end_date="2024-01-08 15:00:00")
	if df_300 is not None and not df_300.empty:
		print("SH000300 found:", len(df_300))
	else:
		 print("SH000300 not found")
         
except Exception as e:
	print("Error:", e)