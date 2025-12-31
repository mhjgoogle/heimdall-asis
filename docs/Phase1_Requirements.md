Status: Production-Signed

File: Phase1_Requirements.md
Phase: 1 (Baseline)
Benchmark Context: Decision Support System (DSS) / Second-Order Thinking
Integrity Check: Judgment vs Validation Matrix Refined
Last Updated: 2025-12-28 17:50 JST

$$Phase 1$$

 需求定义与可行性分析 (Requirements Definition)

1. 核心愿景 (Strategic Vision)

Heimdall-Asis 是一款专为 NISA (日本个人储蓄账户) 投资者设计的“二阶思考”辅助系统。它通过对美日市场的**判断层（事实）与验证层（情绪）**进行交叉审计，辅助投资者在复杂的宏观噪音中找到确定性。

1.1 双层决策逻辑 (The Dual-Layer Logic)

Layer 1: Judgment (事实判断)

逻辑：由硬指标驱动，建立系统的“主航向”。

输出：基准评分 (Base Score) 与 宏观色块 (Red/Green)。

Layer 2: Validation (情绪验证)

逻辑：通过市场定价与舆情检测“背离”。

输出：置信度等级 (High/Low) 与 警报消息 (Divergence Alerts)。

2. 宏观指标分类矩阵 (Macro Indicators)

我们将所有宏观变量严格划分为以下两类。禁止交叉混用。

|

| 维度 | 指标名称 (US/JP) | 作用分类 (Role) | 逻辑说明 |
| 政策 (Policy) | 利率决议 / FOMC 声明 | 判断 (Judgment) | 规则是铁打的事实，决定了流动的边界。 |
| 资金 (Liquidity) | 净流动性 (Net Liq) / 央行总资产 | 判断 (Judgment) | 股市的“燃料”，有钱没钱是物理事实。 |
| 生产 (Production) | ISM PMI / 短观 (Tankan) | 判断 (Judgment) | 经济运行的物理状态（工厂是否在开工）。 |
| 就业/消费 | 失业率 / 零售销售 (RSAFS) | 判断 (Judgment) | 经济增长的最终驱动力。 |
| 通胀 (Inflation) | Core PCE / Core CPI | 判断 (Judgment) | 央行决策的锚点。 |
| 情绪 (Sentiment) | VIX / Nikkei VI | 验证 (Validation) | 市场是否在恐慌？验证判断层结论是否已“见顶/见底”。 |
| 定价 (Pricing) | 10Y-2Y Spread (美债利差) | 验证 (Validation) | 债市对未来衰退的投票，用于验证 PMI 是否有误。 |
| 舆情 (News) | NLP 新闻情感流 | 验证 (Validation) | 媒体在吹什么风？用于检测“利好出尽”或“情绪错杀”。 |

3. 微观指标分类矩阵 (Micro Indicators)

针对个股与特定资产（如黄金），同样的分类逻辑必须贯彻到底。

| 维度 | 核心指标 | 作用分类 (Role) | 验证目的 |
| 基本面 | ROE, 营收增速, FCF | 判断 (Judgment) | 决定公司是否有钱赚钱。 |
| 动量 | MA200 趋势, Mom_12_1 | 判断 (Judgment) | 决定目前趋势的物理方向。 |
| 估值 | Forward P/E (Sector Z-Score) | 判断 (Judgment) | 决定价格是否在合理区间。 |
| 视觉验证 | MA200 乖离率 (Bias) | 验证 (Validation) | 虽然评分很高，但如果乖离率过大，验证为“过热”。 |
| 审计验证 | AI 公司新闻综述 | 验证 (Validation) | 虽然数据很好，但如果有“管理层造假”新闻，置信度降为 0。 |
| 风险验证 | 个股隐含波动率 (IV) | 验证 (Validation) | 验证财报前后的市场预期是否过于拥挤。 |

4. 关键交互逻辑：背离预警 (The Divergence Logic)

系统在 Phase 2 (UI) 必须能够根据下表输出预警：

| 判断层 (J) | 验证层 (V) | 系统输出 (UI Feedback) | 业务逻辑 (The "Why") |
| BULLISH | CONFIRMED | 🟢 RISK ON (High Confidence) | 数据好，市场也相信，趋势最稳。 |
| BULLISH | PANIC (Fear) | 🟡 OPPORTUNITY? (Low Confidence) | 数据好但市场在怕，可能是“情绪错杀”。 |
| BEARISH | EUPHORIA (Greed) | 🟠 DANGER (Bubble) | 数据坏但市场在疯，典型的泡沫末期。 |
| BEARISH | CONFIRMED | 🔴 RISK OFF (High Confidence) | 数据坏，市场也认栽，坚决离场。 |

5. 成功指标 (Success Metrics - Refined)

$$P0$$

 审计覆盖率：100% 的 Gold 层结论必须包含对应的 V 字段数据（例如：如果不展示 VIX，就不能给出置信度）。

$$P0$$

 极端值处理：所有判断层数据必须经过 Winsorize 去噪，验证层数据（如 VIX）必须保留尖峰。