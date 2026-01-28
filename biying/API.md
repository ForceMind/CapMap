# 必盈数据 API 接口清单整理报告

本文档旨在为用户提供必盈数据（BiyingAPI）官方网站上所有 API 接口的完整汇总。必盈数据提供涵盖沪深 A 股、指数、基金、京股、港股及科创板的全面金融数据服务。

## 1. 沪深 A 股 API (`doc_hs`)

沪深 A 股 API 是最核心的部分，涵盖了从基础列表到财务指标的多种数据。

| 分类 | 接口名称 | 接口地址示例 | 说明 |
| :--- | :--- | :--- | :--- |
| **股票列表** | 基础股票列表 | `http://api.biyingapi.com/hslt/list/您的licence` | 获取沪深两市所有股票代码及名称 |
| | 新股日历 | `http://api.biyingapi.com/hslt/new/您的licence` | 获取新股申购、上市及发行详情 |
| | 概念指数列表 | `http://api.biyingapi.com/hslt/sectorslist/您的licence` | 获取券商定义的各种概念指数 |
| | 一级市场板块 | `http://api.biyingapi.com/hslt/primarylist/您的licence` | 获取行业一级市场板块名称 |
| **指数行业** | 指数/行业/概念树 | `http://api.biyingapi.com/hszg/list/您的licence` | 获取完整的行业、概念树形结构 |
| | 某指数下股票 | `http://api.biyingapi.com/hszg/gg/代码/您的licence` | 获取指定板块或指数包含的股票 |
| | 股票所属板块 | `http://api.biyingapi.com/hszg/zg/代码/您的licence` | 查询某只股票所属的所有板块 |
| **涨跌股池** | 涨停股池 | `http://api.biyingapi.com/hslt/ztgc/日期/您的licence` | 获取指定日期的涨停股票详情 |
| | 跌停股池 | `http://api.biyingapi.com/hslt/dtgc/日期/您的licence` | 获取指定日期的跌停股票详情 |
| | 强势股/次新股 | `http://api.biyingapi.com/hslt/qsgc/您的licence` | 获取强势股、次新股及炸板股池 |
| **实时交易** | 实时分时数据 | `https://api.biyingapi.com/hsstock/real/time/代码/您的licence` | 获取股票的实时成交价格、成交量等 |
| | 五档盘口 | `https://api.biyingapi.com/hsstock/real/five/代码/您的licence` | 获取股票的实时买卖五档挂单数据 |
| | 全量实时数据 | `https://all.biyingapi.com/hsrl/real/all/您的licence` | 一次性获取两市所有股票的最新快照 |
| **行情数据** | 最新分时K线 | `https://api.biyingapi.com/hsstock/latest/代码/级别/除权/您的licence` | 获取最新的 1/5/15/30/60 分钟及日线 K 线 |
| | 历史 K 线 | `https://api.biyingapi.com/hsstock/history/代码/级别/除权/您的licence` | 获取指定时间段的历史 K 线数据 |
| **公司详情** | 公司简介 | `http://api.biyingapi.com/hscp/gsjj/代码/您的licence` | 获取上市公司的基本背景信息 |
| | 股东信息 | `http://api.biyingapi.com/hscp/sdgd/代码/您的licence` | 获取十大股东、流通股东及变动情况 |
| | 业绩预告 | `http://api.biyingapi.com/hscp/yjyg/代码/您的licence` | 获取公司的业绩预告及快报数据 |
| **财务报表** | 资产负债表 | `http://api.biyingapi.com/hsstock/financial/balance/代码/您的licence` | 获取上市公司的资产负债详情 |
| | 利润/现金流表 | `http://api.biyingapi.com/hsstock/financial/income/代码/您的licence` | 获取利润表及现金流量表数据 |
| **技术指标** | MACD/MA/BOLL | `http://api.biyingapi.com/hsstock/history/指标/代码/您的licence` | 直接获取计算好的技术指标数据 |

## 2. 沪深指数 API (`doc_zs`)

专门针对沪深两市指数（如上证指数、深证成指、沪深 300 等）的接口。

| 接口名称 | 接口地址示例 | 说明 |
| :--- | :--- | :--- |
| 指数列表 | `http://api.biyingapi.com/hsindex/list/您的licence` | 获取沪深主要指数的代码及名称 |
| 指数实时交易 | `http://api.biyingapi.com/hsindex/real/time/代码/您的licence` | 获取指数的实时点数、涨跌幅等 |
| 指数历史行情 | `http://api.biyingapi.com/hsindex/history/代码/级别/您的licence` | 获取指数的历史分时及日线行情 |
| 指数技术指标 | `http://api.biyingapi.com/hsindex/history/macd/代码/您的licence` | 获取指数的 MACD、MA、KDJ 等指标 |

## 3. 基金数据 API (`doc_jj`)

涵盖沪深两市的公募基金、ETF 基金数据。

| 接口名称 | 接口地址示例 | 说明 |
| :--- | :--- | :--- |
| 基金列表 | `http://api.biyingapi.com/fd/list/all/您的licence` | 获取所有基金的代码及名称 |
| ETF 列表 | `http://api.biyingapi.com/fd/list/etf/您的licence` | 专门获取 ETF 基金列表 |
| 基金实时数据 | `http://api.biyingapi.com/fd/real/time/代码/您的licence` | 获取基金的最新净值或交易价格 |

## 4. 京股数据 API (`doc_jg`)

北交所（北京证券交易所）相关股票及指数数据。

| 接口名称 | 接口地址示例 | 说明 |
| :--- | :--- | :--- |
| 京市股票列表 | `http://api.biyingapi.com/bj/list/all/您的licence` | 获取北交所所有股票代码 |
| 股票实时数据 | `http://api.biyingapi.com/bj/stock/real/time/代码/您的licence` | 北交所股票的最新成交快照 |
| 指数实时数据 | `http://api.biyingapi.com/bj/index/real/time/代码/您的licence` | 北证 50 等指数的实时行情 |

## 5. 港股数据 API (`doc_gg`)

香港联交所主流股票的数据接口。

| 接口名称 | 接口地址示例 | 说明 |
| :--- | :--- | :--- |
| 港股股票列表 | `http://api.biyingapi.com/hk/list/all/您的licence` | 获取主流港股代码（约 850 只） |
| 港股实时数据 | `http://api.biyingapi.com/hk/stock/real/time/代码/您的licence` | 港股最新价、涨跌幅及五档盘口 |

## 6. 科创板数据 API (`doc_kc`)

上证科创板专项数据接口。

| 接口名称 | 接口地址示例 | 说明 |
| :--- | :--- | :--- |
| 科创板列表 | `http://api.biyingapi.com/kc/list/all/您的licence` | 获取所有科创板股票代码 |
| 科创板实时行情 | `http://api.biyingapi.com/kc/real/time/代码/您的licence` | 科创板股票的实时交易快照 |

---

### 通用说明
- **数据格式**: 所有接口均返回标准 **JSON** 格式。
- **请求方式**: 支持 **HTTP/HTTPS GET** 请求。
- **请求频率**: 
  - 免费/基础版：1 分钟 300 次
  - 包年版：1 分钟 3000 次
  - 白金版：1 分钟 6000 次
- **数据更新**: 实时数据在盘中秒级更新；列表及基础信息通常在每日收盘后（16:20 左右）更新。
