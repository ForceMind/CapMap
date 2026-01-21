# Tushare Token (请在此处填入您的 Tushare Token)
TUSHARE_TOKEN = "8800f61eac66af3264e07b1e30dc0e6fb73c4bf27f3a00506fe133b2"

# 配置支持的指数池
STOCK_POOLS = {
    "沪深300 (大盘)": {"code": "399300.SZ", "ak_code": "000300", "cache": "data/csi300_history_cache.parquet"},
    "中证500 (中盘)": {"code": "000905.SH", "ak_code": "000905", "cache": "data/csi500_history_cache.parquet"},
    "中证1000 (小盘)": {"code": "000852.SH", "ak_code": "000852", "cache": "data/csi1000_history_cache.parquet"}
}
