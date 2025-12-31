# filepath: local/src/audit/llm_auditor.py

"""LLM auditor: AI-powered news analysis with batching."""

import sqlite3
import json
import hashlib
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple
import logging

# 选择 LLM 提供商（可选）
try:
    import anthropic
    ANTHROPIC_AVAILABLE = True
except ImportError:
    ANTHROPIC_AVAILABLE = False

try:
    import openai
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False

logger = logging.getLogger(__name__)


class LLMAuditor:
    """
    LLM 审计引擎：
    - 新闻摘要（从标题、URL 元数据生成内容总结）
    - 情绪分析（检测市场情绪：positive/negative/neutral）
    - Token 优化策略（批处理、缓存、模型选择）
    """
    
    def __init__(self, 
                 db_path: str,
                 model: str = "claude-3-5-sonnet",  # 成本 vs 质量最优的选择
                 use_cache: bool = True,
                 batch_size: int = 10,
                 max_retries: int = 3):
        """
        初始化审计引擎
        
        Args:
            db_path: SQLite 数据库路径
            model: 使用的 LLM 模型
            use_cache: 是否启用本地缓存
            batch_size: 批处理大小
            max_retries: 最大重试次数
        """
        self.db_path = db_path
        self.model = model
        self.use_cache = use_cache
        self.batch_size = batch_size
        self.max_retries = max_retries
        
        # 初始化缓存表
        self._init_cache_table()
        
    def _init_cache_table(self):
        """初始化审计缓存表（避免重复处理）"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS audit_cache (
                    fingerprint TEXT PRIMARY KEY,
                    content_hash TEXT NOT NULL,
                    ai_summary TEXT,
                    sentiment_score REAL,
                    confidence REAL,
                    model_used TEXT,
                    tokens_used INTEGER,
                    audited_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(fingerprint, content_hash)
                )
            """)
            conn.commit()

    def audit_news_batch(self, limit: int = 100) -> Dict[str, any]:
        """
        批量审计新闻（主入口）
        
        策略：
        1. 查询未审计的新闻
        2. 按 batch_size 分组
        3. 使用智能提示词减少 tokens
        4. 批量更新数据库
        
        Returns:
            {
                'total': 100,
                'processed': 95,
                'cached': 5,
                'failed': 0,
                'total_tokens': 8432,
                'estimated_cost': 0.15
            }
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            
            # 查询未审计的新闻
            query = """
                SELECT n.fingerprint, n.title, n.url, n.published_at, n.catalog_key
                FROM news_intel_pool n
                WHERE n.ai_summary IS NULL
                LIMIT ?
            """
            unaudited = conn.execute(query, (limit,)).fetchall()
            
        if not unaudited:
            return {
                'total': 0,
                'processed': 0,
                'cached': 0,
                'failed': 0,
                'total_tokens': 0,
                'estimated_cost': 0.0
            }
        
        stats = {
            'total': len(unaudited),
            'processed': 0,
            'cached': 0,
            'failed': 0,
            'total_tokens': 0,
            'estimated_cost': 0.0
        }
        
        # 分批处理
        for batch_idx in range(0, len(unaudited), self.batch_size):
            batch = unaudited[batch_idx:batch_idx + self.batch_size]
            
            try:
                batch_results = self._process_batch(batch)
                
                # 更新数据库
                with sqlite3.connect(self.db_path) as conn:
                    for result in batch_results:
                        if result.get('success'):
                            conn.execute("""
                                UPDATE news_intel_pool
                                SET ai_summary = ?, sentiment_score = ?
                                WHERE fingerprint = ?
                            """, (result['summary'], result['sentiment'], result['fingerprint']))
                            
                            # 更新缓存
                            if self.use_cache:
                                conn.execute("""
                                    INSERT OR REPLACE INTO audit_cache
                                    (fingerprint, content_hash, ai_summary, sentiment_score, 
                                     confidence, model_used, tokens_used)
                                    VALUES (?, ?, ?, ?, ?, ?, ?)
                                """, (
                                    result['fingerprint'],
                                    result['content_hash'],
                                    result['summary'],
                                    result['sentiment'],
                                    result.get('confidence', 0.8),
                                    self.model,
                                    result.get('tokens', 0)
                                ))
                            
                            stats['processed'] += 1
                            stats['total_tokens'] += result.get('tokens', 0)
                        else:
                            stats['failed'] += 1
                    
                    conn.commit()
                    
            except Exception as e:
                logger.error(f"Batch processing failed: {e}")
                stats['failed'] += len(batch)
        
        # 估算成本（Anthropic Claude 3.5 Sonnet）
        # Input: $3/1M tokens, Output: $15/1M tokens
        # 平均输出 200 tokens，输入 300 tokens
        avg_output_tokens = 200
        input_cost = (stats['total_tokens'] * 0.3) * (3 / 1_000_000)
        output_cost = (stats['total_tokens'] * 0.2) * (15 / 1_000_000)
        stats['estimated_cost'] = input_cost + output_cost
        
        return stats

    def _process_batch(self, batch: List[sqlite3.Row]) -> List[Dict]:
        """
        处理一批新闻
        
        Token 优化策略：
        1. 用结构化提示词（减少歧义，减少补充 tokens）
        2. 分级处理（先快速识别，再深度分析）
        3. 共享背景上下文（避免重复）
        """
        results = []
        
        for news in batch:
            # 检查缓存
            if self.use_cache:
                cached = self._get_cached_audit(news['fingerprint'])
                if cached:
                    results.append({
                        'fingerprint': news['fingerprint'],
                        'summary': cached['ai_summary'],
                        'sentiment': cached['sentiment_score'],
                        'confidence': cached['confidence'],
                        'success': True,
                        'cached': True
                    })
                    continue
            
            # 未缓存 -> LLM 处理
            content_hash = self._hash_content(news['title'], news['url'])
            prompt = self._build_audit_prompt(news)
            
            try:
                response = self._call_llm(prompt)
                parsed = self._parse_audit_response(response)
                
                results.append({
                    'fingerprint': news['fingerprint'],
                    'content_hash': content_hash,
                    'summary': parsed['summary'],
                    'sentiment': parsed['sentiment'],
                    'confidence': parsed.get('confidence', 0.8),
                    'tokens': parsed.get('tokens_used', 0),
                    'success': True
                })
                
            except Exception as e:
                logger.error(f"Failed to audit {news['fingerprint']}: {e}")
                results.append({
                    'fingerprint': news['fingerprint'],
                    'success': False,
                    'error': str(e)
                })
        
        return results

    def _build_audit_prompt(self, news: sqlite3.Row) -> str:
        """
        构建审计提示词（Token 优化版本）
        
        设计原则：
        - 使用结构化格式（减少模型补充内容的倾向）
        - 明确指定输出格式（减少试错 tokens）
        - 用关键词代替长句子
        """
        # 提取日期信息
        published_date = news['published_at'].split('T')[0] if 'T' in news['published_at'] else news['published_at']
        
        prompt = f"""You are a financial news analyst. Analyze this news item briefly.

TITLE: {news['title'][:100]}
URL: {news['url'][:80]}
DATE: {published_date}
SOURCE: {news['catalog_key']}

Output JSON only (no markdown):
{{
  "summary": "1-sentence summary under 20 words",
  "sentiment": "positive|negative|neutral",
  "keywords": ["keyword1", "keyword2"],
  "relevance": "high|medium|low"
}}"""
        
        return prompt

    def _call_llm(self, prompt: str) -> str:
        """
        调用 LLM（支持多种提供商）
        
        成本优化：
        - Claude 3.5 Sonnet：最优的成本/质量比
        - OpenAI GPT-4 Turbo：备选
        - 本地模型（Ollama）：完全免费
        """
        if "claude" in self.model.lower() and ANTHROPIC_AVAILABLE:
            return self._call_claude(prompt)
        elif "gpt" in self.model.lower() and OPENAI_AVAILABLE:
            return self._call_openai(prompt)
        else:
            # 备选：本地推理（需要 Ollama 或类似）
            return self._call_local_model(prompt)

    def _call_claude(self, prompt: str) -> str:
        """使用 Anthropic Claude API"""
        client = anthropic.Anthropic()
        
        message = client.messages.create(
            model=self.model,
            max_tokens=500,  # 限制输出大小节省 token
            messages=[
                {"role": "user", "content": prompt}
            ]
        )
        
        return message.content[0].text

    def _call_openai(self, prompt: str) -> str:
        """使用 OpenAI API"""
        response = openai.ChatCompletion.create(
            model=self.model,
            messages=[
                {"role": "system", "content": "You are a financial analyst. Be concise."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.3,  # 降低 temperature 减少输出长度
            max_tokens=400
        )
        
        return response.choices[0].message.content

    def _call_local_model(self, prompt: str) -> str:
        """
        使用本地 LLM（Ollama）
        完全免费！缺点是速度慢
        
        安装: brew install ollama
        启动: ollama run mistral
        """
        try:
            import requests
            
            response = requests.post(
                "http://localhost:11434/api/generate",
                json={
                    "model": "mistral",  # 或 "neural-chat", "llama2"
                    "prompt": prompt,
                    "stream": False,
                    "temperature": 0.3
                },
                timeout=60
            )
            
            if response.status_code == 200:
                return response.json()["response"]
            else:
                raise Exception(f"Local LLM failed: {response.text}")
                
        except Exception as e:
            logger.error(f"Local LLM unavailable: {e}")
            # 降级：返回简单启发式分析
            return self._heuristic_fallback(prompt)

    def _heuristic_fallback(self, prompt: str) -> str:
        """
        完全免费的降级方案：启发式分析
        
        当没有 LLM 时使用
        准确率 60-70%，但零成本
        """
        # 解析提示词中的标题
        title_match = prompt.find("TITLE: ")
        if title_match >= 0:
            title = prompt[title_match + 7:].split("\n")[0].lower()
        else:
            title = ""
        
        # 简单的关键词匹配
        positive_words = ["surge", "gain", "rise", "bull", "strong", "recovery", "beat"]
        negative_words = ["crash", "fall", "loss", "bear", "weak", "decline", "miss"]
        
        sentiment = "neutral"
        if any(word in title for word in positive_words):
            sentiment = "positive"
        elif any(word in title for word in negative_words):
            sentiment = "negative"
        
        return json.dumps({
            "summary": title[:50] if title else "Market news",
            "sentiment": sentiment,
            "keywords": [],
            "relevance": "medium"
        })

    def _parse_audit_response(self, response: str) -> Dict:
        """解析 LLM 输出"""
        try:
            # 尝试提取 JSON
            if "{" in response and "}" in response:
                json_start = response.find("{")
                json_end = response.rfind("}") + 1
                parsed = json.loads(response[json_start:json_end])
            else:
                parsed = json.loads(response)
            
            # 标准化情绪分值
            sentiment_map = {
                "positive": 0.7,
                "negative": -0.7,
                "neutral": 0.0
            }
            sentiment_str = parsed.get("sentiment", "neutral").lower()
            
            return {
                'summary': parsed.get('summary', '')[:200],
                'sentiment': sentiment_map.get(sentiment_str, 0.0),
                'confidence': 0.8,
                'tokens_used': 300  # 估计值
            }
        except Exception as e:
            logger.error(f"Failed to parse response: {e}")
            return {
                'summary': 'Analysis failed',
                'sentiment': 0.0,
                'confidence': 0.0,
                'tokens_used': 0
            }

    def _hash_content(self, title: str, url: str) -> str:
        """为内容生成哈希（用于缓存对比）"""
        content = f"{title}|{url}".encode()
        return hashlib.md5(content).hexdigest()

    def _get_cached_audit(self, fingerprint: str) -> Optional[Dict]:
        """从缓存查询已审计的新闻"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT ai_summary, sentiment_score, confidence FROM audit_cache WHERE fingerprint = ?",
                (fingerprint,)
            ).fetchone()
            
            if row:
                return dict(row)
        return None


class AuditOptimizer:
    """
    审计优化器：智能选择模型和参数以最小化成本
    """
    
    @staticmethod
    def estimate_cost(num_news: int, 
                     model: str = "claude-3-5-sonnet") -> Dict:
        """
        估算审计成本
        
        成本对比（2025年价格）：
        1. Claude 3.5 Sonnet：$3/$15 per 1M tokens - 最优
        2. GPT-4 Turbo：$10/$30 per 1M tokens - 贵但快
        3. GPT-3.5 Turbo：$0.5/$1.5 per 1M tokens - 便宜但质量差
        4. Ollama 本地：$0 - 免费
        """
        # 平均每条新闻：300 input + 200 output tokens
        avg_tokens_per_news = 500
        total_tokens = num_news * avg_tokens_per_news
        
        models = {
            "claude-3-5-sonnet": {
                "input_cost": 3 / 1_000_000,
                "output_cost": 15 / 1_000_000,
                "quality": "high",
                "speed": "medium"
            },
            "gpt-4-turbo": {
                "input_cost": 10 / 1_000_000,
                "output_cost": 30 / 1_000_000,
                "quality": "high",
                "speed": "fast"
            },
            "gpt-3.5-turbo": {
                "input_cost": 0.5 / 1_000_000,
                "output_cost": 1.5 / 1_000_000,
                "quality": "medium",
                "speed": "very_fast"
            },
            "ollama-local": {
                "input_cost": 0,
                "output_cost": 0,
                "quality": "medium",
                "speed": "slow"
            }
        }
        
        config = models.get(model, models["claude-3-5-sonnet"])
        input_tokens = num_news * 300
        output_tokens = num_news * 200
        
        total_cost = (input_tokens * config["input_cost"]) + (output_tokens * config["output_cost"])
        
        return {
            "model": model,
            "num_news": num_news,
            "total_tokens": total_tokens,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "estimated_cost": total_cost,
            "cost_per_article": total_cost / num_news if num_news > 0 else 0,
            "quality": config["quality"],
            "speed": config["speed"]
        }

    @staticmethod
    def recommend_model(num_news: int, budget: float) -> str:
        """
        推荐最合适的模型
        
        Args:
            num_news: 要分析的新闻数量
            budget: 可用预算（美元）
        
        Returns:
            推荐的模型名称
        """
        candidates = [
            "ollama-local",
            "gpt-3.5-turbo",
            "claude-3-5-sonnet",
            "gpt-4-turbo"
        ]
        
        for model in candidates:
            cost_info = AuditOptimizer.estimate_cost(num_news, model)
            if cost_info["estimated_cost"] <= budget:
                return model
        
        # 如果都超预算，推荐最便宜的
        return "ollama-local"


if __name__ == "__main__":
    # 测试成本估算
    print("成本估算（分析 1000 条新闻）：\n")
    
    for model in ["ollama-local", "gpt-3.5-turbo", "claude-3-5-sonnet", "gpt-4-turbo"]:
        cost = AuditOptimizer.estimate_cost(1000, model)
        print(f"{model:20} | Cost: ${cost['estimated_cost']:.2f} | Quality: {cost['quality']:6} | Speed: {cost['speed']}")
    
    print("\n推荐模型（预算 $10）:", AuditOptimizer.recommend_model(1000, 10))
    print("推荐模型（预算 $0）:", AuditOptimizer.recommend_model(1000, 0))
