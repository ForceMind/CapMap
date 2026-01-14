import akshare as ak
from datetime import datetime, timedelta

print("Testing index recent min...")
today_str = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
start = f"{today_str} 09:30:00"
end = f"{today_str} 15:00:00"

print(f"Target: {start} - {end}")

try:
	df = ak.index_zh_a_hist_min_em(symbol="000300", period="1", start_date=start, end_date=end)
	if df is not None:
		print(f"Index 000300 len: {len(df)}")
		if not df.empty: print(df.head(1))
	else:
		print("Index 000300: None")
except Exception as e:
	print(f"Error 000300: {e}")

try:
	df = ak.index_zh_a_hist_min_em(symbol="000001", period="1", start_date=start, end_date=end)
	if df is not None:
		print(f"Index 000001 len: {len(df)}")
	else:
		print("Index 000001: None")
except Exception as e:
	print(f"Error 000001: {e}")