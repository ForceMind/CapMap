import akshare as ak

print("Testing symbols...")
try:
	print("Try 1: symbol='000300'")
	df = ak.stock_zh_a_hist_min_em(symbol="000300", period="1", adjust="qfq", start_date="2024-01-08 09:30:00", end_date="2024-01-08 15:00:00")
	if df is not None: print("Success 000300", len(df))
	else: print("Fail 000300")
except Exception as e:
	print("Error 000300:", e)

try:
	print("Try 2: symbol='sh000300'")
	df = ak.stock_zh_a_hist_min_em(symbol="sh000300", period="1", adjust="qfq", start_date="2024-01-08 09:30:00", end_date="2024-01-08 15:00:00")
	if df is not None: print("Success sh000300", len(df))
except Exception as e:
	print("Error sh000300:", e)
    
try:
	if hasattr(ak, 'index_zh_a_hist_min_em'):
		print("Found index_zh_a_hist_min_em function!")
		df = ak.index_zh_a_hist_min_em(symbol="000300", period="1", start_date="2024-01-08 09:30:00", end_date="2024-01-08 15:00:00")
		if df is not None: print("Success index func", len(df))
	else:
		print("No index_zh_a_hist_min_em function")
except Exception as e:
	print("Error index func:", e)