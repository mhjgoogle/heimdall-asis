# Heimdall-Asis 代码地图

---

## 当前数据库状态（2025-12-30）

### 数据量统计

| 表名 | 记录数 | 用途 |
|------|--------|------|
| **data_catalog** | 61 | 资产注册表（9个FRED + 24个yfinance + 28个NEWS） |
| **raw_ingestion_cache** | 175 | Bronze Layer 原始报文 |
| **sync_watermarks** | 61 | 每个catalog的清洗进度 |
| **timeseries_macro** | 24,169 | Silver Layer 宏观时间序列 |
| **timeseries_micro** | 269,861 | Silver Layer 价格/汇率时间序列 |
| **news_intel_pool** | 10 | Silver Layer 完整新闻（含全文） |
| **总计** | **294,347** | - |

### Catalog 分类

| 角色 | 数量 | 说明 |
|------|------|------|
| **J** (Judgment) | 30 | FRED宏观 + yfinance价格 |
| **V** (Validation) | 31 | NewsAPI新闻 |

### 数据来源

| 来源 | 数量 | 数据类型 |
|------|------|--------|
| **FRED** | 9 | 宏观指标 → timeseries_macro |
| **yfinance** | 24 | OHLCV价格 → timeseries_micro |
| **NewsAPI** | 28 | 新闻 → news_intel_pool |

### 数据库表Schema

#### 1. data_catalog（资产注册表，61条）

```
catalog_key      TEXT PRIMARY KEY  资产ID (METRIC_US_10Y_YIELD, ^GSPC, NEWS_US_TECH_SECTOR)
country          TEXT              国家代码 (US, JP)
scope            TEXT              MACRO / MICRO
role             TEXT              J (判断) / V (验证)
entity_name      TEXT              资产名称
source_api       TEXT              FRED / yfinance / NewsAPI
update_frequency TEXT              Daily / Monthly / Quarterly
config_params    JSON              API配置 ({"series": "DGS10"} 等)
search_keywords  TEXT              搜索词（NewsAPI）
is_active        INTEGER           激活状态 (0/1)
asset_class      TEXT              MICRO / Macroeconomic
```

#### 2. raw_ingestion_cache（原始采集，175条）

```
request_hash     TEXT PRIMARY KEY  md5幂等性hash
catalog_key      TEXT              指向data_catalog
source_api       TEXT              FRED / yfinance / NewsAPI
raw_payload      JSON              API原始响应（observations/articles）
inserted_at      TIMESTAMP         采集时间
```

**FRED数据示例**：
```json
{"observations": [{"date": "2025-12-30", "value": "4.23"}]}
```

**NewsAPI数据示例**（多数429限制）：
```json
{
  "fetched_at": "2025-12-30T08:39:21",
  "error": "API rate limited (429)",
  "articles": []
}
```

#### 3. sync_watermarks（清洗进度，61条）

```
catalog_key        TEXT PRIMARY KEY
last_ingested_at   TIMESTAMP         最后采集
last_cleaned_at    TIMESTAMP         最后清洗 ← 差分清洗基准
last_synced_at     TIMESTAMP         保留字段
last_meta_synced_at TIMESTAMP        保留字段
checksum           TEXT              保留字段
```

#### 4. timeseries_macro（宏观时间序列，24,169条）

```
id          INTEGER PRIMARY KEY
catalog_key TEXT    FRED catalog
date        DATE    数据日期
value       REAL    数值
```

**示例**：METRIC_US_10Y_YIELD, METRIC_US_ISM_PMI 等FRED指标

#### 5. timeseries_micro（价格时间序列，269,861条）

```
id        INTEGER PRIMARY KEY
catalog_key TEXT    yfinance ticker
date      DATE    交易日
val_open  REAL    开盘
val_high  REAL    最高
val_low   REAL    最低
val_close REAL    收盘
val_volume INTEGER 成交量
```

**资产类型**：
- 指数：^GSPC, ^N225, ^FTSE, ^GDAXI
- 金属：GC=F, SI=F, HG=F, PL=F, PA=F
- 外汇：USDJPY=X, EURUSD=X, GBPUSD=X, AUDJPY=X, NZDJPY=X
- 个股：NVDA, MSFT, TSLA, 8035.T, 4063.T, 9432.T, 7203.T, 7267.T

#### 6. news_intel_pool（新闻池，10条）

```
fingerprint  TEXT PRIMARY KEY  url md5指纹
catalog_key  TEXT             NewsAPI catalog
published_at TIMESTAMP        发布时间
title        TEXT             标题
url          TEXT             原始URL
body         TEXT             完整正文 ← Newspaper3k提取 1813-2172字
author       TEXT             作者
source_name  TEXT             媒体 (CNBC, Reuters等)
```

**现状**：NEWS_US_TECH_SECTOR 和 NEWS_US_FINANCIAL_SECTOR 各5篇完整新闻，其他因429限制暂无

---

## 代码与需求对应清单

### Bronze Layer (原始采集) - 0_Data Ingestion § 4.1-4.2

| 代码 | 位置 | 需求 | 说明 |
|------|------|------|------|
| **FredAdapter** | `local/src/adapters/fred.py` | § 4.1 种子注入 | FRED API 集成，支持 21 个宏观指标 |
| **YFinanceAdapter** | `local/src/adapters/yfinance_adapter.py` | § 4.1 种子注入 | 价格 OHLCV，支持指数/外汇/个股 |
| **RSSAdapter** | `local/src/adapters/rss_adapter.py` | § 4.1 种子注入 | Google News RSS，28 个新闻catalog |
| **RetryableHTTPClient** | `local/src/adapters/http_client.py` | § 4.2 指数退避 | 3 次重试，1s/2s/4s backoff，429/5xx 处理 |
| **BaseAdapter** | `local/src/adapters/base.py` | § 4.2 容错接口 | 统一的 `fetch_latest()` 和 `validate_config()` |
| **AdapterFactory/Manager** | `local/src/adapters/adapter_*.py` | § 2 Logic Sinking | 数据源与脚本隔离 |

### Bronze Layer 存储 - 0_Data Ingestion § 4.1

| 代码 | 位置 | 需求 | 说明 |
|------|------|------|------|
| **DatabaseCore** | `local/src/database/database_core.py` | § 2 Logic Sinking | WAL 模式，事务管理，幂等性（request_hash） |
| **init_db.py** | `scripts/init_db.py` + `local/src/database/init_db.py` | § 4.1 初始化 | 创建 6 个表，59 个 catalog 种子 |
| **raw_ingestion_cache** | DB | § 4.1 初始化 | Bronze Layer，存储原始 JSON 报文 |
| **sync_watermarks** | DB | § 4.4 差分同步 | 追踪 last_ingested_at、last_cleaned_at |

### Silver Layer (清洗转换) - 1_Data Clean 及 DATA_PIPELINE_ARCHITECTURE § 4-5

| 代码 | 位置 | 需求 | 说明 |
|------|------|------|------|
| **CleaningPipeline** | `local/src/pipeline/cleaning_pipeline.py` | § 4.4 差分清洗 | 基于 watermark 的增量清洗，原子事务 |
| **FredCleaner** | `local/src/cleaners/fred_cleaner.py` | 1_Data Clean | JSON → (date, value)，时间序列规范化 |
| **YFinanceCleaner** | `local/src/cleaners/yfinance_cleaner.py` | 1_Data Clean | OHLCV 验证、NaN 处理 |
| **NewsAPICleaner** | `local/src/cleaners/rss_cleaner.py` | DATA_PIPELINE § 5.1 | 调用 Newspaper3k 提取全文（4000-6000 字） |
| **web_scraper.py** | `local/src/adapters/web_scraper.py` | DATA_PIPELINE § 5.1 | Newspaper3k 集成，全文提取实现 |
| **timeseries_macro** | DB | 1_Data Clean | Silver Layer 宏观数据 |
| **timeseries_price** | DB | 1_Data Clean | Silver Layer 价格数据 |
| **news_intel_pool** | DB | 1_Data Clean | Silver Layer 新闻池（含完整文本） |

### 批处理编排 - 0_Data Ingestion § 4.4

| 代码 | 位置 | 需求 | 说明 |
|------|------|------|------|
| **IncrementalIngestionEngine** | `local/src/pipeline/incremental_ingestion.py` | § 4.4 差分同步 | 频率过滤，adapter 调用，hash 幂等性 |
| **batch_orchestrator.py** | `local/src/pipeline/batch_orchestrator.py` | § 4.4 编排 | ingestion + cleaning 两阶段编排 |
| **batch_ingestion.sh** | `scripts/batch_ingestion.sh` | § 4.4 流水线入口 | 支持 Daily/Monthly/Quarterly 参数 |
| **background_scheduler.py** | `scripts/background_scheduler.py` | § 4.4 后台调度 | 每日 00:05/00:10/00:15 自动运行（实际实现） |

### 资产激活 - 0_Data Ingestion § 4.3

| 代码 | 位置 | 需求 | 说明 |
|------|------|------|------|
| **confirm_all_assets.py** | `scripts/confirm_all_assets.py` | § 4.3 激活验证 | HTTP 200 + 非空数据 → is_active=1，局部失败隔离 |

---

## 实现与计划的差异

| 需求条目 | 计划 | 实际实现 | 调整原因 |
|--------|------|--------|--------|
| **批处理入口脚本** | scripts/run_daily_batch.py | scripts/batch_ingestion.sh + Python 内核 | Shell 提供更好的通用性和频率参数支持 |
| **后台调度** | 依赖系统 cron | background_scheduler.py（Python daemon） | 跨平台独立，无系统依赖 |
| **差分清洗机制** | 提及但无详细设计 | Watermark 表（last_cleaned_at） | 精确化实现，防止重复清洗 |
| **新闻获取** | 仅标题+摘要 | 全文提取（Newspaper3k） | 提升数据价值，完整文本分析 |
| **FRED 流动性指标** | 单个 METRIC_US_NET_LIQUIDITY | 拆分为 3 个独立 catalog | 符合规范化设计，METRIC_US_FED_BALANCE_SHEET / METRIC_US_REPO_INJECTIONS / METRIC_US_REVERSE_REPO |
| **NEWS catalog role** | 统一标记 | 全部设为 'V'（Validation） | 明确语义分类 |
| **MACRO catalog role** | 统一标记 | 全部设为 'J'（Judgment） | 明确语义分类 |
| **API 限额管理** | 未考虑 | NEWS 从 HOURLY→Daily（28 req/day） | NewsAPI Free Tier 100 req/day 限制，优化调度 |

---

## 关键设计模式

### 1. Watermark 差分清洗（同步 1_Data Clean）

```python
# 清洗流程
last_cleaned_at = db.get_watermark(catalog_key)
new_data = db.query(
    "SELECT * FROM raw_ingestion_cache 
     WHERE catalog_key=? AND inserted_at > ?",
    (catalog_key, last_cleaned_at)
)

if not new_data:
    return  # 无新数据，跳过

# 清洗 + 原子更新
with db.transaction():
    cleaner.clean(new_data)
    db.update_watermark(catalog_key, now())  # 原子同步进度
```

### 2. 幂等性保证（同步 0_Data Ingestion § 4.4）

```python
# request_hash = md5(source_api + config_params + date)
# INSERT OR REPLACE：重复执行不会产生重复记录
db.execute(
    "INSERT OR REPLACE INTO raw_ingestion_cache 
     (request_hash, catalog_key, raw_payload, inserted_at)
     VALUES (?, ?, ?, ?)",
    (hash, key, payload, now())
)
```

### 3. 局部失败隔离（同步 0_Data Ingestion § 4.4）

```python
for catalog in catalogs:
    try:
        ingest_single(catalog)  # 若失败只影响本 catalog
    except Exception as e:
        logger.error(f"Failed: {catalog} - {e}")
        continue  # 立即处理下一个，不阻塞全流程
```

---

## 代码数量统计

| 分层 | 文件数 | 代码行数（估） | 核心职责 |
|------|--------|------------|--------|
| Scripts | 5 | 500 | 初始化、流水线入口、后台调度 |
| Pipeline | 3 | 800 | 编排、采集、清洗 |
| Adapter | 8 | 1200 | 4 个数据源、工厂、管理、HTTP 客户端 |
| Cleaner | 4 | 600 | 4 种清洗逻辑 |
| Database | 5 | 700 | 连接、CRUD、幂等性、事务管理 |
| Tests | 4 | 400 | 单元测试 |
| **总计** | **29** | **~4200** | - |

---

## 执行流程图

```
background_scheduler.py (Daily 00:05)
  ↓
batch_ingestion.sh Daily
  ↓
├─ IncrementalIngestionEngine.ingest()
│  ├─ FRED 21 catalogs → FredAdapter.fetch_latest()
│  ├─ YFINANCE 10 catalogs → YFinanceAdapter.fetch_latest()
│  └─ RSS 28 catalogs → RSSAdapter.fetch_latest()
│  ↓
│  raw_ingestion_cache (request_hash 幂等性)
│  ↓
│  sync_watermarks.last_ingested_at ← UPDATE
│
└─ CleaningPipeline.clean_all()  [仅处理 inserted_at > last_cleaned_at]
   ├─ FredCleaner → timeseries_macro
   ├─ YFinanceCleaner → timeseries_price
   ├─ NewsAPICleaner → news_intel_pool (Newspaper3k 全文)
   ↓
   sync_watermarks.last_cleaned_at ← ATOMIC UPDATE
```

