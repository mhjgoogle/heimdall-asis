Heimdall-Asis Implementation: Data Ingestion (Bronze Layer)

1. 任务背景 (Context)

你正在辅助开发 Heimdall-Asis 投资辅助系统。
目前处于 Phase 4 Implementation。你需要构建 Bronze Layer (原始摄取层)，负责从 FRED、yfinance 和 Google News RSS 抓取原始报文，并将其原子化封存在本地 SQLite 数据库中。该阶段是整个系统“事实判断”的物理起点。

2. 核心架构约束 (Architectural Constraints)

单一信源 (SSOT)：docs/Phase4_DataModel.md 是数据库结构的唯一真理来源。

核心逻辑下沉 (Logic Sinking)：

数据库连接与 CRUD 封装在 local/src/database/。

抓取适配器封装在 local/src/adapters/。

编排流水线逻辑封装在 local/src/pipeline/。

scripts/ 下的脚本仅作为入口，通过调用 src 模块完成任务。

物理路径：

数据库：/local/data/heimdall.db (启用 WAL 模式)。

适配器：/local/src/adapters/。

测试：/local/src/tests/。

幂等性：使用 request_hash (URL + Params 的 MD5) 作为主键，防止重复采集，确保数据审计的唯一性。

3. 技术栈要求 (Tech Stack)

Runtime: Python 3.11+

Http Client: httpx (必须支持异步与重试逻辑)

Finance Lib: yfinance (仅用于 OHLCV 历史价格)

Configuration: python-dotenv (API Keys 必须从 .env 加载，严禁硬编码)

Testing: pytest (用于 src/tests/ 下的单元验证)

4. 实施步骤细节 (Implementation Specs)

4.1 步骤一：数据库初始化与全量种子注入 (Initialization & Registry)

指令：编写 scripts/init_db.py。

DDL 执行：解析 Phase4_DataModel.md 中的 SQL，创建核心表结构。

种子注入 (Upsert Logic)：使用 INSERT OR REPLACE 注入以下配置。所有资产默认 is_active=0。

[Registry Seeds - 当前清单]

Macro (J) - 宏观事实判断层:

METRIC_US_NET_LIQUIDITY: FRED, {"series": ["WALCL", "WTREGEN", "RRPONTSYD"]}

METRIC_US_ISM_PMI: FRED, {"series": "NAPM"}

METRIC_US_UNRATE: FRED, {"series": "UNRATE"} (就业基准)

METRIC_US_RETAIL: FRED, {"series": "RSAFS"} (消费基准)

METRIC_JP_TANKAN: FRED, {"series": "JPNBS6000S"} (日本短观)

Micro (J) - 微观资产判断层:

指数: ^GSPC (S&P 500), ^N225 (Nikkei 225)

金属: GC=F, SI=F, HG=F, PL=F, PA=F

外汇: USDJPY=X, EURUSD=X, GBPUSD=X, AUDJPY=X, NZDJPY=X, CNY=X

个股: NVDA, MSFT, TSLA, 8035.T, 4063.T, 4188.T, 7203.T, 7267.T

Validation (V) - 情绪验证层:

情绪指标: ^VIX, T10Y2Y (美债利差)

情报舆情: 统一使用 Google News RSS，Query 模板: https://news.google.com/rss/search?q={ASSET}+news&hl=en-US

4.2 步骤二：容错适配器开发与单元测试 (Fault-Tolerant Adapters & Testing)

在 /local/src/adapters/ 下实现类，并在 /local/src/tests/ 下编写对应单元测试：

指数退避重试 (Exponential Backoff)：针对 5xx 错误或连接超时，重试 3 次 (1s, 2s, 4s)。

空数据校验：若响应成功但内容为空（如 observations 列表为空），必须抛出 EmptyResultSetError。

职责分明：YFinanceAdapter 仅允许获取价格序列，新闻流必须通过 RSSAdapter 统一获取。

4.3 步骤三：资产激活与非空校验 (Asset Activation & Audit)

指令：实现 scripts/confirm_all_assets.py。

连通性与内容校验：对 is_active=0 的资产调用对应的 Adapter 进行 fetch_latest(limit=1)。

成功判定标准：

连通性：HTTP 200 且无协议错误。

非空性：获取到的原始数据集长度必须 > 0。

状态翻转：仅在上述条件全部满足时，执行 UPDATE data_catalog SET is_active=1。

日志输出：详细记录激活失败的原因，并跳过该资产继续验证下一个，确保脚本不中断。

4.4 步骤四：批处理流水线与局部隔离 (Batch Ingestion Pipeline)

在 /local/src/pipeline/ingestion.py 中实现核心编排逻辑：

编排逻辑：由 scripts/batch_ingestion.sh 调用本模块（实际实现调整：使用 Shell 脚本作为入口，支持频率参数 FREQUENCY）。

局部失败隔离 (Isolation)：遍历 data_catalog 时，若某个资产抓取失败，应捕获异常并记录结构化日志，随后立即处理下一个资产，严禁阻塞全局任务。

差分同步 (Differential Sync)：通过对比 sync_watermarks 中的水位，仅抓取增量部分。

结构化日志 (Logging)：每次抓取必须记录 catalog_key, status (SUCCESS/FAILED), duration, count 以及 request_hash。

---

## 实际实现与计划差异补充 (Implementation Divergence)

### 4.5 Watermark 机制深度说明

**设计目标**：实现三层数据流 Bronze → Silver → Watermark Update 的原子化清洗。

**表结构**：

```sql
CREATE TABLE sync_watermarks (
  catalog_key TEXT PRIMARY KEY,
  last_ingested_at TIMESTAMP,      -- 最后一次采集时间
  last_cleaned_at TIMESTAMP,       -- 最后一次清洗时间（关键）
  last_synced_at TIMESTAMP,        -- 保留字段
  last_meta_synced_at TIMESTAMP,   -- 保留字段
  checksum TEXT                    -- 保留字段
);
```

**核心逻辑**：

1. **采集阶段**：IncrementalIngestionEngine 更新 `last_ingested_at`
2. **清洗阶段**：CleaningPipeline 查询 `WHERE inserted_at > last_cleaned_at`，仅处理新数据
3. **原子更新**：清洗成功后在事务中更新 `last_cleaned_at`
4. **差分检测**：batch_ingestion.sh 在清洗前检查是否有新数据（COUNT 从 last_cleaned_at 后）

**实际部署**：当前 59 个 catalog 均已初始化 watermark，支持 Daily/Monthly/Quarterly 三种频率。

### 4.6 Database Schema 与原计划差异

**计划中的表**：

- `data_catalog`（仅用于资产激活）
- `raw_ingestion_cache`（存储原始报文）

**实际实现扩展**：

| 表名 | 用途 | 变更说明 |
|------|------|--------|
| data_catalog | 资产注册表 | ✅ 已实现，字段扩展：added `update_frequency` (Daily/Monthly/Quarterly), `scope` (MACRO/MICRO), `role` (J/V) |
| raw_ingestion_cache | Bronze Layer | ✅ 已实现，用于临时存储原始 JSON 报文 |
| sync_watermarks | 清洗进度跟踪 | ✅ 已实现（计划未提及），关键：差分清洗的基础 |
| timeseries_macro | Silver Layer (宏观) | ✅ 已实现，存储清洗后的时间序列数据 |
| timeseries_price | Silver Layer (价格) | ✅ 已实现，存储 OHLCV 和外汇价格 |
| news_intel_pool | Silver Layer (新闻) | ✅ 已实现，存储含全文的新闻记录 |

### 4.7 适配器实现现状

**已实现的 4 个适配器**（在 `/local/src/adapters/` 下）：

1. **FredAdapter** (`fred.py`)
   - FRED API 集成，支持多个 series ID
   - 指数退避重试：最多 3 次
   - 空数据校验：若无 observations 则抛异常

2. **YFinanceAdapter** (`yfinance_adapter.py`)
   - 历史 OHLCV 数据获取
   - 支持多个 ticker 并行请求

3. **NewsAPIAdapter** (via RSSAdapter `rss_adapter.py`)
   - Google News RSS 源解析
   - **关键补充**：已实现全文提取！
     - 使用 Newspaper3k 库从 URL 提取完整文章内容
     - 支持多语言（英文主力）
     - 落地到 `news_intel_pool` 表中

4. **HttpClientAdapter** (`http_client.py`)
   - 通用 HTTP 重试客户端
   - 连接超时：30s
   - 重试策略：Exponential backoff (1s, 2s, 4s)

**实际部署状态**：

- 59 个 catalog 已配置
- 28 个 NEWS catalog（各种关键词）
- 21 个 FRED 宏观指标
- 10 个 YFinance 价格资产

### 4.8 步骤三改进：自动激活机制

**原计划**：scripts/confirm_all_assets.py 手动验证并激活

**实际现状**：

- ✅ confirm_all_assets.py 已实现
- 🔧 激活流程已集成到 batch_ingestion.sh
- 🔄 首次采集时自动激活（若连通性正常）

### 4.9 批处理调度与后台执行

**原计划**：scripts/run_daily_batch.py

**实际实现**：

```
scripts/background_scheduler.py （Python daemon，PID 持久化）
  ├─ Daily 00:05 → bash scripts/batch_ingestion.sh Daily
  ├─ Monthly 00:10 → bash scripts/batch_ingestion.sh Monthly
  └─ Quarterly 00:15 → bash scripts/batch_ingestion.sh Quarterly
```

**优势**：

1. 不依赖 cron，跨平台兼容（Windows/Linux/macOS）
2. 内存驻留，避免每次启动开销
3. 自动异常恢复（schedule 库内置重试）
4. 日志集中：scheduler.log + batch_ingestion.log

### 4.10 API 限额管理（新增）

**NewsAPI 限制**：Free Tier = 100 requests/day

**初期问题**：28 NEWS × hourly = 672 req/day (超限 6.7倍)

**解决方案**：2025-12-30 迁移至 Daily 频率 → 28 req/day (安全范围)

| 数据源 | 请求方式 | 日均请求量 | 状态 |
|--------|---------|----------|------|
| FRED (Daily) | 21 个 catalog × 1 次 | ~21 | ✅ 无限制 |
| NewsAPI (Daily) | 28 个 catalog × 1 次 | ~28 | ✅ 在 100 限额内 |
| YFinance (Daily) | 10 个 ticker × 1 次 | ~10 | ✅ 无限制 |
| **总计** | - | **~59** | ✅ 安全 |

### 4.11 当前运行状态（截至 2025-12-30）

**Background Scheduler**：
- PID: 665450
- Status: Running
- Schedule: Daily 00:05, Monthly 00:10, Quarterly 00:15

**数据统计**：
- 活跃 catalog: 59
- Bronze 记录: 5,482（含 NewsAPI 原始 JSON）
- Silver 记录: 12,540+（含完整新闻文章）
- Watermark 同步: 100%

**关键改进**：

| 需求 | 计划方案 | 实际方案 | 改进点 |
|-----|--------|--------|-------|
| 批处理入口 | scripts/run_daily_batch.py | scripts/batch_ingestion.sh + background_scheduler.py | Shell 通用性强，支持频率参数 |
| 差分清洗 | 提及但无详细方案 | Watermark 机制（last_cleaned_at） | 精确化，防止重复清洗 |
| 新闻全文 | 仅获取标题、摘要 | Newspaper3k 提取 4000-6000 字 | 完整内容，提升价值 |
| 调度管理 | 依赖系统 cron | Python daemon（内存驻留） | 跨平台，无依赖 |
| 容错策略 | 单个适配器重试 | 全局隔离 + 结构化日志 | 一个 catalog 失败不阻塞全流程 |

5. 验收标准 (Definition of Accuracy)

[ ] 需求闭环：数据库内已包含 Phase 1 定义的所有宏观五大支柱及微观核心资产。

[ ] 架构合规：scripts 下不包含任何具体的抓取逻辑，仅作为 src 模块的 Caller。

[ ] 质量保证：运行 confirm_all_assets.py 后，仅 API 响应正常且内容非空的资产变为激活状态。

[ ] 性能幂等：连续运行两次流水线，第二次应通过 request_hash 自动跳过，实现零重复存储。

