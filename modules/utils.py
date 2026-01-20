import time
from datetime import datetime, timedelta
import threading

# 尝试导入 Streamlit 上下文管理器
try:
    from streamlit.runtime.scriptrunner import add_script_run_ctx, get_script_run_ctx
except ImportError:
    from streamlit.scriptrunner import add_script_run_ctx, get_script_run_ctx

def with_retry(func, retries=3, delay=1.0, default=None):
    """通用重试装饰器逻辑"""
    for i in range(retries):
        try:
            return func()
        except Exception as e:
            if i == retries - 1:
                print(f"Failed after {retries} retries: {e}")
                return default
            time.sleep(delay * (i + 1)) # 指数退避

def get_start_date(years_back=2):
    """计算 N 年前的日期，返回 YYYYMMDD 字符串"""
    target = datetime.now() - timedelta(days=365 * years_back)
    return target.strftime("%Y%m%d")
