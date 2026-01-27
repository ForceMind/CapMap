# 配置支持的指数池
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"

STOCK_POOLS = {
    "沪深300 (大盘)": {"code": "000300", "cache": str(DATA_DIR / "csi300_history_cache.parquet")},
    "中证500 (中盘)": {"code": "000905", "cache": str(DATA_DIR / "csi500_history_cache.parquet")},
    "中证1000 (小盘)": {"code": "000852", "cache": str(DATA_DIR / "csi1000_history_cache.parquet")}
}
