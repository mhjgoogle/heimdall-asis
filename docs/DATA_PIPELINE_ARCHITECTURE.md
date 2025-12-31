# 数据采集-清洗完整架构说明

**最后更新：2025-12-30**

本文档描述 Heimdall 的数据采集、存储、清洗的完整流程。**请勿随意修改核心代码**。

---

## 1. 系统架构全景

```
┌─────────────────────────────────────────────────────────────────┐
│                    Background Scheduler (Python)                 │
│                    运行在后台 (PID: 661769)                      │
│                     定时触发摄入任务                             │
└──────────────────────┬──────────────────────────────────────────┘
                       │
         ┌─────────────┼─────────────┐
         │             │             │
    每小时第5分钟   每天00:05   每月1号00:10
         │             │             │
    HOURLY         Daily        Monthly/Quarterly
         │             │             │
         └─────────────┼─────────────┘
                       │
         ┌─────────────▼──────────────────────────┐
         │  batch_ingestion.sh (主摄入脚本)       │
         │                                        │
         │  1️⃣  从 API 获取原始数据               │
         │  2️⃣  存入 Bronze 层 (raw_ingestion_cache)
         │  3️⃣  检测是否有新数据                │
         │  4️⃣  如有新数据 → 执行清洗           │
         └─────────────┬──────────────────────────┘
                       │
         ┌─────────────▼──────────────────────────┐
         │       清洗管道 (Cleaning Pipeline)      │
         │                                        │
         │  1️⃣  查询 Watermark (last_cleaned_at) │
         │  2️⃣  获取增量数据 (inserted_at > watermark)
         │  3️⃣  Cleaner 转换数据                │
         │  4️⃣  插入 Silver 层表                │
         │  5️⃣  原子性更新 Watermark            │
         └─────────────┬──────────────────────────┘
                       │
    ┌──────────────────┼──────────────────────┐
    │                  │                      │
    ▼                  ▼                      ▼
timeseries_macro  timeseries_micro  news_intel_pool
(FRED 宏指标)    (股票 OHLCV)      (完整新闻文章)
```

---

## 2. 数据层次说明

### 2.1 Bronze 层 - 原始数据

**表：`raw_ingestion_cache`**

存储从 API 获取的原始、未经处理的数据。

| 字段 | 含义 | 示例 |
|------|------|------|
| `request_hash` | 查询去重 key | `sha256(catalog_key + query + time_window)` |
| `catalog_key` | 资产编码 | `NEWS_US_TECH_SECTOR` |
| `source_api` | 数据源 | `NewsAPI`, `yfinance`, `FRED`, `RSS` |
| `raw_payload` | 原始 JSON | `{"articles": [...], "error": null}` |
| `inserted_at` | 摄入时间戳 | `2025-12-30 09:05:30` |

**特点：**
- 存储完整原始数据，无任何处理
- 去重机制防止重复存储
- 保留错误信息（如 API 限流 429）

### 2.2 Silver 层 - 清洗数据

**表：**
- `timeseries_macro` - FRED 宏经济指标
- `timeseries_micro` - yfinance 股票 OHLCV
- `news_intel_pool` - 完整新闻文章

**特点：**
- 数据已清洗、验证、标准化
- NewsAPI 和 RSS：已用 Newspaper3k 提取完整文章内容（4000-6000 字符）
- 去重：基于 fingerprint（URL MD5）
- 带有 source、author、publish_date 等元数据

### 2.3 Watermarks 表 - 追踪表

**表：`sync_watermarks`**

记录每个 catalog 的摄入和清洗进度，用于**差分处理**。

| 字段 | 含义 | 用途 |
|------|------|------|
| `catalog_key` | 资产编码 | 唯一标识 |
| `last_ingested_at` | 最后摄入时间 | 追踪摄入进度 |
| `last_cleaned_at` | 最后清洗时间 | **决定增量清洗的起点** |

**核心机制：** 清洗时查询 `last_cleaned_at`，只处理 `inserted_at > last_cleaned_at` 的数据。

---

## 3. 采集流程详解

### 3.1 触发方式

**后台调度器** (`background_scheduler.py`) 定时触发：

```
每小时第5分钟   → bash batch_ingestion.sh HOURLY
每天 00:05      → bash batch_ingestion.sh Daily
每月 1 号 00:10 → bash batch_ingestion.sh Monthly
每季度 1 号 00:15 → bash batch_ingestion.sh Quarterly
```

### 3.2 摄入步骤（batch_ingestion.sh）

```bash
#!/bin/bash
1. 从 data_catalog 查询指定频率的所有 catalogs
2. 对每个 catalog 调用 IncrementalIngestionEngine.ingest_by_asset_key()
3. 每个 adapter 通过 API 获取数据
4. 检查 request_hash 去重
5. 存入 raw_ingestion_cache (Bronze 层)
6. 更新 sync_watermarks.last_ingested_at
```

### 3.3 数据源配置

**`data_catalog` 表**

每个资产的配置：

| 列 | 含义 | 示例 |
|----|------|------|
| `catalog_key` | 唯一标识 | `NEWS_US_TECH_SECTOR` |
| `source_api` | 数据源类型 | `NewsAPI`, `yfinance`, `FRED`, `RSS` |
| `update_frequency` | 更新频率 | `HOURLY`, `Daily`, `Monthly`, `Quarterly` |
| `search_keywords` | 搜索词 | `"Apple OR Microsoft, technology earnings"` |
| `is_active` | 是否激活 | `1` 或 `0` |

---

## 4. 清洗流程详解

### 4.1 清洗三阶段

```
┌─────────────────────────────────────────────────┐
│ 第 1 阶段：获取 Watermark（查询上次清洗点）    │
│ SELECT MIN(last_cleaned_at) FROM sync_watermarks│
│ WHERE catalog_key IN (NEWS_*, FRED, yfinance)  │
│ 结果：2025-12-30 08:05:00                      │
└─────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────┐
│ 第 2 阶段：获取增量数据（查询新数据）          │
│ SELECT * FROM raw_ingestion_cache              │
│ WHERE inserted_at > '2025-12-30 08:05:00'      │
│ 结果：找到 28 条新数据                         │
└─────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────┐
│ 第 3 阶段：数据转换 + 原子性提交                │
│ BEGIN TRANSACTION                              │
│   INSERT INTO silver_table (...)  /* 28 条 */ │
│   UPDATE sync_watermarks           /* 更新 */ │
│ COMMIT                                         │
│ 结果：全部成功或全部回滚                      │
└─────────────────────────────────────────────────┘
```

### 4.2 Cleaner 的作用

每个数据源有对应的 Cleaner：

| 源 | Cleaner | 动作 |
|----|---------|------|
| FRED | FredCleaner | 验证字段，直接入库 |
| yfinance | YFinanceCleaner | 验证 OHLCV 格式 |
| NewsAPI | NewsAPICleaner | 用 Newspaper3k 提取完整文章 |
| RSS | RssCleaner | 用 Newspaper3k 提取完整文章 |

**关键：** NewsAPI 和 RSS 的 body 提取：

```python
# 从 URL 提取完整文章内容（4000-6000 字符）
newspaper.Article(url).download().parse()
# 备选方案：trafilatura、直接返回 description
```

### 4.3 去重机制

**Fingerprint（指纹）：**

```python
fingerprint = MD5(url)  # 基于 URL 的唯一哈希
```

**数据库约束：**

```sql
INSERT OR REPLACE INTO news_intel_pool (fingerprint, ...)
-- 同一个 URL 只保存最新的版本
```

---

## 5. Watermark 机制详解

### 5.1 核心概念

Watermark 是一个"检查点"，标记上一次清洗完成的时刻。

```
时间线：
08:00 ────────── 09:00 ────────── 10:00
      摄入        │    摄入        │
      清洗        │    清洗        │
      watermark   │    watermark   │
      = 08:55     │    = 09:55     │
                  │               │
              inserted_at        last_cleaned_at
              >= 09:05           = 08:55
                                 ↓
                            查询增量数据的起点
```

### 5.2 工作流程

**首次清洗（last_cleaned_at = NULL）：**
```sql
SELECT * FROM raw_ingestion_cache
-- 查询所有数据
```

**增量清洗（last_cleaned_at = 08:55）：**
```sql
SELECT * FROM raw_ingestion_cache
WHERE inserted_at > '2025-12-30 08:55:00'
-- 只查新数据，避免重复处理
```

**更新 Watermark（清洗完成）：**
```sql
UPDATE sync_watermarks
SET last_cleaned_at = '2025-12-30 09:05:00'
WHERE catalog_key = 'NEWS_US_TECH_SECTOR'
```

---

## 6. 核心文件说明

### 6.1 必须保留的文件

**脚本：**

| 文件 | 路径 | 作用 | 不要改 |
|------|------|------|--------|
| `batch_ingestion.sh` | `scripts/` | 摄入+清洗流水线 | ⚠️ 核心逻辑 |
| `background_scheduler.py` | `scripts/` | 后台定时调度器 | ⚠️ 调度时间 |

**代码库：**

| 文件/目录 | 作用 | 不要改 |
|-----------|------|--------|
| `local/src/pipeline/incremental_ingestion.py` | 增量摄入引擎 | ⚠️ Watermark 逻辑 |
| `local/src/pipeline/cleaning_pipeline.py` | 清洗管道 | ⚠️ 差分清洗逻辑 |
| `local/src/adapters/` | 数据源适配器 | ✓ 可优化关键词 |
| `local/src/cleaners/` | 数据清洗器 | ✓ 可优化提取方法 |

**日志：**

| 文件 | 作用 | 保留 |
|------|------|------|
| `batch_ingestion.log` | 摄入+清洗日志 | ✅ 保留 |
| `scheduler.log` | 调度器日志 | ✅ 保留 |

### 6.2 可修改的配置

**可以改的：**

```sql
-- 调整更新频率
UPDATE data_catalog 
SET update_frequency = '2_HOURLY'  -- 从 HOURLY 改为 2 小时一次
WHERE catalog_key LIKE 'NEWS%';

-- 调整搜索词（AND/OR 逻辑）
UPDATE data_catalog 
SET search_keywords = 'Apple OR Microsoft OR NVIDIA, earnings'
WHERE catalog_key = 'NEWS_US_TECH_SECTOR';

-- 激活/停用数据源
UPDATE data_catalog 
SET is_active = 0  -- 临时停用
WHERE catalog_key = 'NEWS_US_TECH_SECTOR';
```

**千万不要改的：**

```python
# ❌ 不要改 Watermark 查询逻辑
# ❌ 不要改差分清洗条件 (inserted_at > last_cleaned_at)
# ❌ 不要改原子性事务逻辑
# ❌ 不要改 Cleaner 的 fingerprint 生成
# ⚠️ 谨慎改调度时间（可能影响现有任务）
```

---

## 7. 当前状态（2025-12-30）

### 7.1 已实现功能

| 功能 | 状态 | 备注 |
|------|------|------|
| ✅ NewsAPI 摄入 | 完成 | 支持 AND/OR 查询、域名过滤 |
| ✅ 全文提取 | 完成 | Newspaper3k + trafilatura |
| ✅ 差分清洗 | 完成 | Watermark 机制自动防重复 |
| ✅ 后台定时 | 完成 | background_scheduler.py 运行 |
| ✅ 多频率支持 | 完成 | HOURLY/Daily/Monthly/Quarterly |
| ✅ 自动检测新数据 | 完成 | 有新数据才执行清洗 |

### 7.2 已知限制与 API 配额

**NewsAPI 免费版限制：**
```
100 请求/天
24 小时数据延迟
最多搜索 1 个月的文章
```

**当前配置问题：**
```
28 个 NEWS catalogs × 1 小时/次 = 28 请求/小时
28 请求/小时 × 24 小时 = 672 请求/天
❌ 远超 100 请求/天的限制
```

**必要的配置修改：**

| 频率 | 请求数/天 | 是否可行 | 建议 |
|------|----------|--------|------|
| 每小时 | 672 | ❌ 不可行 | 每天限流 |
| 每 4 小时 | 168 | ❌ 不可行 | 仍超限 |
| 每 6 小时 | 112 | ⚠️ 勉强 | 偶发限流 |
| **每 8 小时** | **84** | ✅ **推荐** | 稳定运行 |
| 每 12 小时 | 56 | ✅ 最稳定 | 更新频率低 |

**建议设置：**
```sql
UPDATE data_catalog 
SET update_frequency = 'DAILY'
WHERE catalog_key LIKE 'NEWS%';
```

每天 1 次 × 28 catalogs = **28 请求/天** ✅

---

| 问题 | 原因 | 解决方案 |
|------|------|---------|
| 🔴 频繁 429 限流 | NewsAPI 100 请求/天限制 | 改成每 8 小时运行一次 |
| 🟡 冷启动时全量清洗 | 无历史 watermark | 正常，首次会处理所有数据 |
| 🟡 URL 无法提取 | 网站反爬 | Newspaper3k 有重试机制 |

### 7.3 数据统计

```
raw_ingestion_cache (Bronze):
├─ 总记录：33 条 NewsAPI 请求
├─ 成功：2 条（5 + 5 = 10 篇文章）
└─ 限流：28 条 (429 error)

news_intel_pool (Silver):
├─ 总记录：10 条完整新闻
├─ 带 body：10 条（已提取）
└─ 最后更新：2025-12-30 09:00
```

---

## 8. 常见操作

### 8.1 查看摄入进度

```bash
# 查看最新日志
tail -50 /home/mo/heimdall-asis/local/logs/batch_ingestion.log

# 查看调度器状态
tail -20 /home/mo/heimdall-asis/local/logs/scheduler.log

# 查看数据统计
sqlite3 /home/mo/heimdall-asis/local/data/heimdall.db << 'EOF'
SELECT source_api, COUNT(*) as records, MAX(inserted_at) as latest
FROM raw_ingestion_cache
GROUP BY source_api
ORDER BY latest DESC;
EOF
```

### 8.2 检查清洗状态

```bash
sqlite3 /home/mo/heimdall-asis/local/data/heimdall.db << 'EOF'
SELECT 
    catalog_key,
    last_ingested_at,
    last_cleaned_at,
    CASE 
        WHEN last_cleaned_at >= last_ingested_at THEN '✓ 已清洗'
        ELSE '⚠ 有未清洗数据'
    END as status
FROM sync_watermarks
WHERE catalog_key LIKE 'NEWS%'
LIMIT 5;
EOF
```

### 8.3 手动执行摄入

```bash
# 立即执行 HOURLY 摄入
bash /home/mo/heimdall-asis/scripts/batch_ingestion.sh HOURLY

# 立即执行 Daily 摄入
bash /home/mo/heimdall-asis/scripts/batch_ingestion.sh Daily
```

### 8.4 停止后台调度器

```bash
# 查找进程 ID
ps aux | grep background_scheduler.py | grep -v grep

# 停止（优雅关闭）
kill -TERM <PID>

# 重启
cd /home/mo/heimdall-asis && nohup python3 scripts/background_scheduler.py > /dev/null 2>&1 &
```

---

## 9. 下一步可能的改进

1. **API 限流优化**
   - 实现指数退避重试
   - 针对不同源的速率限制

2. **扩展数据源**
   - 添加更多 RSS 源
   - 集成 Twitter API

3. **清洗优化**
   - 支持自定义提取规则
   - 多语言内容标准化

4. **监控告警**
   - 摄入失败告警
   - 清洗错误率告警

---

## 10. 联系和问题

- **调度问题**：检查 `scheduler.log`
- **摄入失败**：检查 `batch_ingestion.log` 和 API 额度
- **清洗问题**：检查 watermarks 是否正确更新
- **数据质量**：检查对应的 Cleaner 实现

---

**最后更新：2025-12-30 09:05**
**系统状态：✅ 正常运行（后台调度器 PID: 661769）**
