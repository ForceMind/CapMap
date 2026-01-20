import akshare as ak
import pandas as pd

print("Testing index min data...")
try:
    # 尝试沪深300指数, 代码通常是 "sh000300" 或者 "sz399300"
    # 或者 akshare 的 symbol 映射。
    # 东方财富接口 stock_zh_a_hist_min_em 对指数的支持情况
    # 上证指数 000001 -> 'sh000001'
    
    print("Fetching SH000300 (CSI300)...")
    # 注意：stock_zh_a_hist_min_em 的 symbol 不需要前缀，如果是指数，可能需要特殊处理
    # 经过查询，akshare 获取指数分钟数据通常也是用 stock_zh_a_hist_min_em，但是 symbol 可能不同
    # 试试常用指数代码
    
    # 上证指数
    df_sh = ak.stock_zh_a_hist_min_em(symbol="sh000001", period="1", adjust="qfq", start_date="2024-01-08 09:30:00", end_date="2024-01-08 15:00:00")
    if df_sh is not None and not df_sh.empty:
        print("SH000001 found:", len(df_sh))
    else:
        print("SH000001 not found")

    # 沪深300
    df_300 = ak.stock_zh_a_hist_min_em(symbol="sh000300", period="1", adjust="qfq", start_date="2024-01-08 09:30:00", end_date="2024-01-08 15:00:00")
    if df_300 is not None and not df_300.empty:
        print("SH000300 found:", len(df_300))
    else:
         print("SH000300 not found")
         
except Exception as e:
    print("Error:", e)
