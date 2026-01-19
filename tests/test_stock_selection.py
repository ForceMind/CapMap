
import pandas as pd
import numpy as np

# Mock generic structure
def simulate_logic():
    # 1. Mock daily_df with 300 stocks (some SH 6xxxx, some SZ 0xxxx/3xxxx)
    # Ensure we have mixed turnover
    codes = []
    names = []
    turnovers = []
    
    # 200 SH stocks
    for i in range(200):
        codes.append(f"600{i:03d}")
        names.append(f"SH_Stock_{i}")
        turnovers.append(np.random.randint(100, 10000))
        
    # 100 SZ stocks
    for i in range(100):
        codes.append(f"000{i:03d}")
        names.append(f"SZ_Stock_{i}")
        turnovers.append(np.random.randint(100, 10000))
        
    daily_df = pd.DataFrame({
        '代码': codes,
        '名称': names,
        '成交额': turnovers,
        '涨跌幅': np.random.randn(300)
    })
    
    # User Input
    top_n = 20
    
    # -------------------------------------------------------------------------
    # Logic from app.py for "Turnover" mode (Original)
    # -------------------------------------------------------------------------
    print(f"--- Testing Turnover Mode (Original Logic, Total N={top_n}) ---")
    
    # Logic:
    top_stocks_df_orig = daily_df.sort_values('成交额', ascending=False).head(top_n)
    
    sh_pool_orig = top_stocks_df_orig[top_stocks_df_orig['代码'].astype(str).str.startswith('6')]
    sz_pool_orig = top_stocks_df_orig[~top_stocks_df_orig['代码'].astype(str).str.startswith('6')]
    
    print(f"Original -> SH: {len(sh_pool_orig)}, SZ: {len(sz_pool_orig)}, Total: {len(top_stocks_df_orig)}")

    # -------------------------------------------------------------------------
    # Logic for "Turnover" mode (Proposed fix: N per market)
    # -------------------------------------------------------------------------
    print(f"\n--- Testing Turnover Mode (Proposed Fix: N per Market) ---")
    
    sh_pool_all = daily_df[daily_df['代码'].astype(str).str.startswith('6')]
    sz_pool_all = daily_df[~daily_df['代码'].astype(str).str.startswith('6')]
    
    sh_top = sh_pool_all.sort_values('成交额', ascending=False).head(top_n)
    sz_top = sz_pool_all.sort_values('成交额', ascending=False).head(top_n)
    
    top_stocks_df_new = pd.concat([sh_top, sz_top], ignore_index=True)
    
    sh_count_new = len(top_stocks_df_new[top_stocks_df_new['代码'].astype(str).str.startswith('6')])
    sz_count_new = len(top_stocks_df_new[~top_stocks_df_new['代码'].astype(str).str.startswith('6')])
    
    print(f"Proposed -> SH: {sh_count_new}, SZ: {sz_count_new}, Total: {len(top_stocks_df_new)}")
    
    assert sh_count_new == top_n
    assert sz_count_new == top_n

    
    # -------------------------------------------------------------------------
    # Logic from app.py for "Index Contribution" mode
    # -------------------------------------------------------------------------
    print(f"\n--- Testing Index Contribution Mode (N={top_n} each) ---")
    
    daily_df['abs_impact'] = (daily_df['涨跌幅'] * daily_df['成交额']).abs()
    
    sh_pool_all = daily_df[daily_df['代码'].astype(str).str.startswith('6')].copy()
    sz_pool_all = daily_df[~daily_df['代码'].astype(str).str.startswith('6')].copy()
    
    sh_top = sh_pool_all.sort_values('abs_impact', ascending=False).head(top_n)
    sz_top = sz_pool_all.sort_values('abs_impact', ascending=False).head(top_n)
    
    top_stocks_df_idx = pd.concat([sh_top, sz_top], ignore_index=True)
    
    print(f"Index SH Count: {len(sh_top)}")
    print(f"Index SZ Count: {len(sz_top)}")
    print(f"Index Total Count: {len(top_stocks_df_idx)}")
    
    
if __name__ == "__main__":
    simulate_logic()
