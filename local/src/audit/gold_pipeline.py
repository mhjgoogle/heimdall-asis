#!/usr/bin/env python3
# filepath: local/src/audit/gold_pipeline.py

"""Gold layer audit pipeline: LLM analysis and scoring."""

import sys
from pathlib import Path
import sqlite3
import json
import logging
from datetime import datetime
from typing import Dict, List

# 添加 src 路径
local_dir = Path(__file__).parent.parent.parent
sys.path.insert(0, str(local_dir / 'src'))

from audit.llm_auditor import LLMAuditor, AuditOptimizer

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

DB_PATH = local_dir / 'data' / 'heimdall.db'


class GoldLayerPipeline:
    """Gold 层处理管道"""
    
    def __init__(self, db_path: str, audit_model: str = "claude-3-5-sonnet"):
        self.db_path = db_path
        self.auditor = LLMAuditor(str(db_path), model=audit_model)
    
    def run(self, 
            audit_news: bool = True,
            audit_limit: int = 100,
            dry_run: bool = False,
            verbose: bool = False) -> Dict:
        """
        运行 Gold 层处理
        
        Args:
            audit_news: 是否执行 LLM 审计新闻
            audit_limit: 最多审计多少条新闻
            dry_run: 不写入数据库
            verbose: 显示详细日志
        
        Returns:
            执行结果统计
        """
        results = {
            'start_time': datetime.now(),
            'audit_stats': {},
            'score_stats': {},
            'warning_stats': {},
            'dry_run': dry_run
        }
        
        logger.info("=" * 60)
        logger.info("Gold Layer Pipeline Started")
        logger.info("=" * 60)
        
        # 1. 审计新闻
        if audit_news:
            logger.info(f"\nPhase 1: Auditing News (limit={audit_limit})")
            audit_results = self.auditor.audit_news_batch(limit=audit_limit)
            results['audit_stats'] = audit_results
            
            logger.info(f"  ✓ Processed: {audit_results['processed']}")
            logger.info(f"  ✓ Cached: {audit_results['cached']}")
            logger.info(f"  ✓ Failed: {audit_results['failed']}")
            logger.info(f"  ✓ Tokens used: {audit_results['total_tokens']}")
            logger.info(f"  ✓ Estimated cost: ${audit_results['estimated_cost']:.4f}")
        
        # 2. 计算资产评分
        logger.info(f"\nPhase 2: Computing Asset Scores")
        score_results = self._compute_asset_scores(dry_run=dry_run)
        results['score_stats'] = score_results
        
        logger.info(f"  ✓ Scores computed: {score_results['computed']}")
        logger.info(f"  ✓ Skipped: {score_results['skipped']}")
        
        # 3. 检测背离预警
        logger.info(f"\nPhase 3: Detecting Divergence Warnings")
        warning_results = self._detect_warnings(dry_run=dry_run)
        results['warning_stats'] = warning_results
        
        logger.info(f"  ✓ Warnings issued: {warning_results['warnings']}")
        logger.info(f"  ✓ Divergence detected: {warning_results['divergence']}")
        
        results['end_time'] = datetime.now()
        logger.info(f"\nGold Layer Pipeline Completed in {(results['end_time'] - results['start_time']).total_seconds():.1f}s")
        
        return results
    
    def _compute_asset_scores(self, dry_run: bool = False) -> Dict:
        """
        计算资产评分
        
        逻辑：
        1. Base Score (基于 JUDGMENT 数据，如价格、利率、经济数据)
        2. Sentiment Adjustment (基于 VALIDATION 数据和 LLM 分析)
        3. Divergence Score (价格 vs 情绪的背离程度)
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            
            # 获取所有活跃资产
            assets = conn.execute("""
                SELECT DISTINCT catalog_key 
                FROM data_catalog
                WHERE is_active = 1
            """).fetchall()
            
            stats = {'computed': 0, 'skipped': 0}
            
            for asset in assets:
                catalog_key = asset['catalog_key']
                
                # 获取最新的价格/数值数据
                if 'STOCK' in catalog_key or 'ASSET' in catalog_key:
                    # 微观数据（个股/外汇）
                    latest = conn.execute("""
                        SELECT * FROM timeseries_micro
                        WHERE catalog_key = ?
                        ORDER BY date DESC
                        LIMIT 1
                    """, (catalog_key,)).fetchone()
                    
                    if latest:
                        base_score = self._calc_micro_base_score(latest)
                    else:
                        stats['skipped'] += 1
                        continue
                        
                elif 'FRED' in catalog_key or 'MACRO' in catalog_key:
                    # 宏观数据（单值）
                    latest = conn.execute("""
                        SELECT * FROM timeseries_macro
                        WHERE catalog_key = ?
                        ORDER BY date DESC
                        LIMIT 1
                    """, (catalog_key,)).fetchone()
                    
                    if latest:
                        base_score = self._calc_macro_base_score(latest)
                    else:
                        stats['skipped'] += 1
                        continue
                
                # 获取情绪调整
                sentiment_adjustment = self._get_sentiment_adjustment(catalog_key, conn)
                
                # 综合分数
                final_score = base_score + (sentiment_adjustment * 0.3)  # 情绪权重 30%
                final_score = max(-1.0, min(1.0, final_score))  # 归一化到 [-1, 1]
                
                # 创建输出表（如果不存在）
                if not dry_run:
                    conn.execute("""
                        CREATE TABLE IF NOT EXISTS asset_scores_local (
                            catalog_key TEXT PRIMARY KEY,
                            base_score REAL,
                            sentiment_adjustment REAL,
                            final_score REAL,
                            divergence_score REAL,
                            confidence REAL,
                            scored_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    """)
                    
                    conn.execute("""
                        INSERT OR REPLACE INTO asset_scores_local
                        (catalog_key, base_score, sentiment_adjustment, final_score, confidence)
                        VALUES (?, ?, ?, ?, ?)
                    """, (catalog_key, base_score, sentiment_adjustment, final_score, 0.75))
                    
                    conn.commit()
                
                stats['computed'] += 1
            
            return stats
    
    def _calc_micro_base_score(self, data: sqlite3.Row) -> float:
        """
        计算个股基础分
        
        逻辑：
        - 当前价格相对过去 30 天的动量
        - Close vs SMA30
        """
        current_close = data['val_close']
        return 0.5  # 简化示例，实际需要更复杂的计算
    
    def _calc_macro_base_score(self, data: sqlite3.Row) -> float:
        """
        计算宏观基础分
        
        逻辑：
        - 基于指标的历史分位数
        - 如 GDP 增速、失业率、利率
        """
        return 0.0  # 简化示例
    
    def _get_sentiment_adjustment(self, catalog_key: str, conn) -> float:
        """
        从新闻情绪获取调整因子
        
        逻辑：
        - 查询最近的相关新闻
        - 平均它们的 sentiment_score
        - 返回 [-1, 1] 的调整值
        """
        news = conn.execute("""
            SELECT AVG(sentiment_score) as avg_sentiment
            FROM news_intel_pool
            WHERE catalog_key = ?
            AND published_at > datetime('now', '-7 days')
        """, (catalog_key,)).fetchone()
        
        if news and news['avg_sentiment']:
            return news['avg_sentiment']
        return 0.0
    
    def _detect_warnings(self, dry_run: bool = False) -> Dict:
        """
        检测背离预警
        
        当价格和情绪不一致时：
        - 价格上升 + 情绪负面 = 反弹机会
        - 价格下降 + 情绪正面 = 熊市陷阱
        """
        warnings = []
        stats = {'warnings': 0, 'divergence': 0}
        
        with sqlite3.connect(self.db_path) as conn:
            # 简化示例
            conn.execute("""
                CREATE TABLE IF NOT EXISTS divergence_warnings (
                    id INTEGER PRIMARY KEY,
                    catalog_key TEXT,
                    divergence_type TEXT,
                    price_direction TEXT,
                    sentiment_direction TEXT,
                    confidence REAL,
                    triggered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            if not dry_run:
                conn.commit()
        
        return stats


def cost_analysis():
    """成本分析示例"""
    print("\n" + "="*60)
    print("LLM 审计成本分析")
    print("="*60)
    
    news_counts = [10, 100, 500, 1000]
    
    for count in news_counts:
        print(f"\n分析 {count} 条新闻：")
        for model in ["ollama-local", "gpt-3.5-turbo", "claude-3-5-sonnet"]:
            cost = AuditOptimizer.estimate_cost(count, model)
            print(f"  {model:20} ${cost['estimated_cost']:8.4f}  (质量: {cost['quality']:6})")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Gold Layer Pipeline")
    parser.add_argument("--audit", action="store_true", help="Run LLM audit")
    parser.add_argument("--audit-limit", type=int, default=50, help="Max news to audit")
    parser.add_argument("--model", default="claude-3-5-sonnet", help="LLM model to use")
    parser.add_argument("--dry-run", action="store_true", help="Don't write to DB")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    parser.add_argument("--cost-analysis", action="store_true", help="Show cost analysis")
    
    args = parser.parse_args()
    
    if args.cost_analysis:
        cost_analysis()
    else:
        pipeline = GoldLayerPipeline(str(DB_PATH), audit_model=args.model)
        results = pipeline.run(
            audit_news=args.audit,
            audit_limit=args.audit_limit,
            dry_run=args.dry_run,
            verbose=args.verbose
        )
        
        print("\n" + "="*60)
        print(json.dumps(results, indent=2, default=str))
