import akshare as ak
try:
    df = ak.index_stock_cons(symbol="000300")
    print("Columns:", df.columns.tolist())
    print("First row:", df.iloc[0].to_dict())
except Exception as e:
    print("Error:", e)
