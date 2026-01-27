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

def get_start_date(years_back=None, months_back=None, days_back=None):
    """计算起始日期，返回 YYYYMMDD 字符串。
    默认使用近 3 个月；可按年/月/天指定。
    """
    if days_back is not None:
        target = datetime.now() - timedelta(days=days_back)
    elif months_back is not None:
        target = datetime.now() - timedelta(days=30 * months_back)
    else:
        if years_back is None:
            years_back = 0
            months_back = 3
            target = datetime.now() - timedelta(days=30 * months_back)
        else:
            target = datetime.now() - timedelta(days=365 * years_back)
    return target.strftime("%Y%m%d")
