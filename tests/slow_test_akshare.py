import akshare as ak
import pandas as pd
import time
import random
from datetime import datetime

# 测试目标列表：混合指数和个股，测试不同周期
test_symbols = [
    {"symbol": "sh000300", "type": "index", "period": "1"},  # 沪深300指数 1分钟
    {"symbol": "600519", "type": "stock", "period": "1"},    # 贵州茅台 1分钟
    {"symbol": "000001", "type": "stock", "period": "5"},    # 平安银行 5分钟
    {"symbol": "sh000001", "type": "index", "period": "5"},  # 上证指数 5分钟
    {"symbol": "399006", "type": "index", "period": "15"},  # 创业板指 15分钟
]

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def test_api():
    log("=== 开始 Akshare 接口慢速稳定性测试 ===")
    log("注意：本测试强制单线程，并在每次请求后强制等待 3-6 秒，以确稳定。")
    log(f"待测试标的数量: {len(test_symbols)}")
    
    success_count = 0
    fail_count = 0
    
    for i, item in enumerate(test_symbols):
        symbol = item['symbol']
        typ = item['type']
        period = item['period']
        
        log(f"\n--- 正在请求 [{i+1}/{len(test_symbols)}]: {symbol} ({typ}, {period}分钟) ---")
        
        start_time = time.time()
        try:
            df = None
            if typ == 'index':
                # 指数接口
                # 注意：部分指数代码 AKShare 可能需要特定前缀，如 sh/sz
                log(f"调用 ak.index_zh_a_hist_min_em(symbol='{symbol}', period='{period}')")
                df = ak.index_zh_a_hist_min_em(symbol=symbol, period=period)
            else:
                # 个股接口
                log(f"调用 ak.stock_zh_a_hist_min_em(symbol='{symbol}', period='{period}')")
                df = ak.stock_zh_a_hist_min_em(symbol=symbol, period=period, adjust='qfq')
            
            elapsed = time.time() - start_time
            
            if df is not None and not df.empty:
                rows = len(df)
                # 尝试获取最新时间，不同接口列名可能不同
                last_time = "Unknown"
                if '时间' in df.columns:
                    last_time = df.iloc[-1]['时间']
                elif 'day' in df.columns: # 有些日线接口
                     last_time = df.iloc[-1]['day']
                     
                log(f">>> SUCCESS: 获取到 {rows} 条数据, 最新时间: {last_time}, 耗时: {elapsed:.2f}s")
                success_count += 1
            else:
                log(f">>> WARNING: 接口返回空数据, 耗时: {elapsed:.2f}s")
                fail_count += 1
                
        except Exception as e:
            elapsed = time.time() - start_time
            log(f">>> ERROR: 请求异常 - {str(e)}")
            log(f"    耗时: {elapsed:.2f}s")
            fail_count += 1
        
        # 慢速等待，避免触发反爬
        if i < len(test_symbols) - 1: # 最后一个就不用等那么久了，或者也可以等
            wait_seconds = random.uniform(3, 6) # 3到6秒随机等待
            log(f"冷却中... 等待 {wait_seconds:.2f} 秒以避免反爬...")
            time.sleep(wait_seconds)

    log("\n=== 测试结束 ===")
    log(f"总计: {len(test_symbols)}, 成功: {success_count}, 失败: {fail_count}")

if __name__ == "__main__":
    try:
        test_api()
    except KeyboardInterrupt:
        log("测试被用户手动中断")
