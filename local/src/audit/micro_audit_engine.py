#!/usr/bin/env python3
"""
Micro Audit Engine - 版本化的 Vic 趋势线和技术指标计算引擎

支持 Experimental 和 Production 两种模式，允许版本对比和逻辑迭代。
"""

import argparse
import json
import sqlite3
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any, Generator
import pandas as pd
import numpy as np
from abc import ABC, abstractmethod


# ==================== 核心路径配置 ====================
PROJECT_ROOT = Path(__file__).parent.parent.parent
DB_PATH = PROJECT_ROOT / "data" / "heimdall.db"
CACHE_DIR = PROJECT_ROOT / "data" / "cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)


# ==================== 策略版本控制 ====================
class StrategyVersion:
    """策略版本管理"""

    # 生产版本（确定的 TradeVic36 逻辑）
    PRODUCTION = "v1_tradervic36"

    # 实验版本（待验证的新逻辑）
    EXPERIMENTAL = "v2_beta"

    @staticmethod
    def get_all_versions() -> List[str]:
        return [StrategyVersion.PRODUCTION, StrategyVersion.EXPERIMENTAL]


# ==================== 策略注册表 ====================
class StrategyRegistry:
    """策略插件注册表 - 支持动态添加新的计算逻辑"""

    _strategies = {}

    @classmethod
    def register(cls, name: str, strategy_class: type):
        """注册新策略"""
        cls._strategies[name] = strategy_class

    @classmethod
    def get_strategy(cls, name: str, version: str = StrategyVersion.PRODUCTION):
        """获取策略实例"""
        if name not in cls._strategies:
            raise ValueError(f"Strategy '{name}' not registered")
        return cls._strategies[name](version=version)

    @classmethod
    def list_strategies(cls) -> List[str]:
        """列出所有注册的策略"""
        return list(cls._strategies.keys())


# ==================== 技术指标策略基类 ====================
class IndicatorStrategy(ABC):
    """技术指标计算策略基类"""
    
    def __init__(self, version: str = StrategyVersion.PRODUCTION):
        self.version = version
        self.config = self._load_config()
    
    def _load_config(self) -> Dict:
        """加载配置参数 - 支持版本特定的配置"""
        base_config = {
            'atr_period': 14,
            'atr_multiplier': 1.5,
            'sma_periods': [20, 60, 200],
            'bias_period': 200,
            'consolidation_window': 20,
            'consolidation_threshold': 0.02,
            'window_2month': 60,
            'window_1year': 250,
            'window_3year': 750,
            'min_span_short': 3,
            'min_span_mid': 15,
            'min_span_long': 30,
            'atr_multiplier_touch': 0.5,
            'fallback_tolerance_pct': 0.005,
            'recent_days_threshold': 125,
            'group_threshold_short': 10,
            'group_threshold_long': 60,
            'edge_window': 30,
        }

        # 实验版本配置调整
        if self.version == StrategyVersion.EXPERIMENTAL:
            # v2实验版本：更严格的触点验证，减少假信号
            base_config.update({
                'atr_multiplier_touch': 0.3,  # 降低容差，更严格
                'min_span_short': 5,  # 要求更长的最小跨度
                'consolidation_threshold': 0.015,  # 更低的盘整阈值
            })

        return base_config
    
    @abstractmethod
    def calculate(self, df: pd.DataFrame) -> Dict[str, Any]:
        """执行计算，返回结果字典"""
        pass


# ==================== Vic 趋势线策略 ====================
class VicTrendStrategy(IndicatorStrategy):
    """Vic 趋势线识别策略"""
    
    def calculate(self, df: pd.DataFrame) -> Dict[str, Any]:
        """计算 Vic 趋势线 - 匹配TradeVic36"""
        df = df.copy()

        # 准备数据 - 基于Candle Body (Open/Close)
        df['BodyHigh'] = df[['Open', 'Close']].max(axis=1)
        df['BodyLow'] = df[['Open', 'Close']].min(axis=1)
        df['ATR'] = self._calculate_atr(df)

        # 识别锚点
        anchors = self._identify_anchors(df)

        # 生成趋势线
        trendlines = list(self._generate_lines(df, anchors))

        return {
            'version': self.version,
            'timestamp': datetime.now().isoformat(),
            'anchors': [{'date': str(a['date']), 'type': a['type'], 'period': a['period']} for a in anchors],
            'trendlines': trendlines,
            'consolidation': self._check_consolidation(df),
            'metadata': {
                'data_length': len(df),
                'date_range': f"{df.index[0].date()} ~ {df.index[-1].date()}",
                'atr_mean': df['ATR'].mean(),
            }
        }
    
    def _calculate_atr(self, df: pd.DataFrame) -> pd.Series:
        """计算 ATR（Average True Range）- 匹配TradeVic36"""
        period = self.config['atr_period']
        high = df['High']
        low = df['Low']
        close = df['Close']
        prev_close = close.shift(1)

        tr1 = high - low
        tr2 = (high - prev_close).abs()
        tr3 = (low - prev_close).abs()

        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr = tr.rolling(window=period).mean()

        # 填充NaN
        atr = atr.bfill()

        mask = atr.isna()
        if mask.any():
            atr[mask] = close[mask] * 0.01

        return atr
    
    def _check_consolidation(self, df: pd.DataFrame) -> Dict:
        """识别横盘整理"""
        window = self.config['consolidation_window']
        threshold = self.config['consolidation_threshold']
        
        if len(df) < window:
            return {'is_active': False, 'reason': 'insufficient_data'}
        
        recent = df.tail(window)
        high = recent['BodyHigh'].max()
        low = recent['BodyLow'].min()
        volatility = (high - low) / recent['Close'].mean() if recent['Close'].mean() != 0 else 0
        
        return {
            'is_active': volatility < threshold,
            'volatility': volatility,
            'threshold': threshold,
        }
    
    def _identify_anchors(self, df: pd.DataFrame) -> List[Dict]:
        """识别锚点 - 匹配TradeVic36逻辑"""
        anchors = []

        # 1. 全局极值
        global_max_idx = df['BodyHigh'].idxmax()
        global_min_idx = df['BodyLow'].idxmin()

        if pd.notna(global_max_idx):
            anchors.append({'date': global_max_idx, 'type': 'down', 'period': 'Global'})
        if pd.notna(global_min_idx):
            anchors.append({'date': global_min_idx, 'type': 'up', 'period': 'Global'})

        # 2. 滑动窗口
        windows = [50, 250, 750]
        for w in windows:
            if w >= 700:
                p_name = '3Year'
            elif w >= 200:
                p_name = '1Year'
            else:
                p_name = '2Month'

            rolling_max = df['BodyHigh'].rolling(window=w, center=True).max()
            high_points = df[df['BodyHigh'] == rolling_max]
            for date in high_points.index:
                anchors.append({'date': date, 'type': 'down', 'period': p_name})

            rolling_min = df['BodyLow'].rolling(window=w, center=True).min()
            low_points = df[df['BodyLow'] == rolling_min]
            for date in low_points.index:
                anchors.append({'date': date, 'type': 'up', 'period': p_name})

        # 3. 边缘补全
        last_window = self.config['edge_window']
        if len(df) > last_window:
            recent_df = df.iloc[-last_window:]

            recent_high_date = recent_df['BodyHigh'].idxmax()
            is_new = True
            for a in anchors:
                if a['date'] == recent_high_date and a['type'] == 'down':
                    is_new = False; break
            if is_new and pd.notna(recent_high_date):
                anchors.append({'date': recent_high_date, 'type': 'down', 'period': '2Month'})

            recent_low_date = recent_df['BodyLow'].idxmin()
            is_new = True
            for a in anchors:
                if a['date'] == recent_low_date and a['type'] == 'up':
                    is_new = False; break
            if is_new and pd.notna(recent_low_date):
                anchors.append({'date': recent_low_date, 'type': 'up', 'period': '2Month'})

        # 4. 10年时间过滤
        max_years = 10  # self.config.get('max_anchor_years', 10)
        cutoff_date = df.index[-1] - pd.Timedelta(days=max_years * 365)

        filtered_anchors = [a for a in anchors if a['date'] >= cutoff_date]

        # 5. 去重
        unique_anchors = {}
        priority = {'Global': 4, '3Year': 3, '1Year': 2, '2Month': 1}
        filtered_anchors.sort(key=lambda x: priority.get(x['period'], 0))

        for a in filtered_anchors:
            key = (a['date'], a['type'])
            unique_anchors[key] = a

        return sorted(list(unique_anchors.values()), key=lambda x: x['date'])
    
    def _generate_lines(self, df: pd.DataFrame, anchors: List[Dict]) -> Generator[Dict, None, None]:
        """生成趋势线 - 匹配TradeVic36的完整逻辑"""
        raw_lines = []

        rec_thresh_days = self.config.get('recent_days_threshold', 125)
        if len(df) > rec_thresh_days:
            recent_threshold = df.index[-rec_thresh_days]
        else:
            recent_threshold = df.index[0]

        priority_map = {'Global': 4, '3Year': 3, '1Year': 2, '2Month': 1}

        for i, anchor in enumerate(anchors):
            if anchor['period'] == '2Month' and anchor['date'] < recent_threshold:
                continue

            target_date = None
            current_priority = priority_map.get(anchor['period'], 0)

            # 查找下一个相反类型的锚点
            for j in range(i + 1, len(anchors)):
                next_anchor = anchors[j]
                if next_anchor['type'] != anchor['type']:
                    if anchor['period'] == '2Month' or \
                       priority_map.get(next_anchor['period'], 0) >= current_priority:
                        target_date = next_anchor['date']
                        break

            line = self._generate_segment(df, anchor, target_date)
            if line:
                raw_lines.append(line)

        if not raw_lines:
            return []

        raw_lines.sort(key=lambda x: x['p1'][0])
        curr_group = [raw_lines[0]]

        for l in raw_lines[1:]:
            prev = curr_group[-1]
            time_diff = (l['p1'][0] - prev['p1'][0]).days

            thresh = self.config['group_threshold_short'] if l['period'] == '2Month' else self.config['group_threshold_long']

            if time_diff < thresh and l['type'] == prev['type']:
                curr_group.append(l)
            else:
                best_line = self._select_best_line(curr_group)
                if best_line:
                    # 转换为输出格式
                    yield {
                        'start_date': str(best_line['p1'][0].date()),
                        'start_price': best_line['p1'][1],
                        'break_date': str(best_line['break_date'].date()),
                        'break_price': best_line['p2'][1],
                        'type': best_line['type'],
                        'period': best_line['period'],
                        'slope': best_line['slope'],
                        'touches': best_line['touch_count'],
                        'strength': 'strong' if best_line['touch_count'] >= 3 else 'weak',
                    }
                curr_group = [l]

        if curr_group:
            best_line = self._select_best_line(curr_group)
            if best_line:
                yield {
                    'start_date': str(best_line['p1'][0].date()),
                    'start_price': best_line['p1'][1],
                    'break_date': str(best_line['break_date'].date()),
                    'break_price': best_line['p2'][1],
                    'type': best_line['type'],
                    'period': best_line['period'],
                    'slope': best_line['slope'],
                    'touches': best_line['touch_count'],
                    'strength': 'strong' if best_line['touch_count'] >= 3 else 'weak',
                }

    def _generate_segment(self, df: pd.DataFrame, anchor: Dict, next_anchor_date=None) -> Optional[Dict]:
        """生成单条趋势线片段"""
        start_date = anchor['date']
        trend_type = anchor['type']
        anchor_period = anchor['period']

        if start_date not in df.index:
            return None

        sub_df = df.loc[start_date:].copy()
        if len(sub_df) < 5:
            return None

        dates = sub_df.index
        lows = sub_df['BodyLow'].values
        highs = sub_df['BodyHigh'].values
        closes = sub_df['Close'].values
        atrs = sub_df['ATR'].values

        if next_anchor_date and next_anchor_date in sub_df.index:
            target_idx = sub_df.index.get_loc(next_anchor_date)
        else:
            target_idx = np.argmax(highs) if trend_type == 'up' else np.argmin(lows)

        if target_idx < 2:
            target_idx = len(sub_df) - 1

        # 凸包扫描
        best_idx = -1
        scan_end = min(target_idx + 1, len(sub_df))
        scan_start = 1

        if trend_type == 'up':
            min_slope = np.inf
            for i in range(scan_start, scan_end):
                if i >= len(lows): break
                slope = (lows[i] - lows[0]) / i
                if slope < min_slope:
                    min_slope = slope
                    best_idx = i
            current_slope = min_slope
        else:
            max_slope = -np.inf
            for i in range(scan_start, scan_end):
                if i >= len(highs): break
                slope = (highs[i] - highs[0]) / i
                if slope > max_slope:
                    max_slope = slope
                    best_idx = i
            current_slope = max_slope

        if best_idx == -1 or np.isinf(current_slope):
            return None

        # Vic 准则验证
        if trend_type == 'up' and current_slope <= 0:
            return None
        if trend_type == 'down' and current_slope >= 0:
            return None

        # 最小跨度过滤
        min_span_req = self.config['min_span_short'] if anchor_period == '2Month' \
                      else (self.config['min_span_mid'] if anchor_period == '1Year' \
                           else self.config['min_span_long'])
        if best_idx < min_span_req:
            return None

        # 延长与突破
        final_slope = current_slope
        y_start = lows[0] if trend_type == 'up' else highs[0]
        break_idx = len(sub_df) - 1

        for i in range(best_idx + 1, len(sub_df)):
            line_price = y_start + final_slope * i
            price_check = closes[i]
            if (trend_type == 'up' and price_check < line_price) or \
               (trend_type == 'down' and price_check > line_price):
                break_idx = i
                break

        # 触点验证
        touch_count = 0
        atr_multiplier = self.config['atr_multiplier_touch']
        fallback_tol = self.config.get('fallback_tolerance_pct', 0.005)

        for i in range(break_idx + 1):
            line_p = y_start + final_slope * i
            bar_p = lows[i] if trend_type == 'up' else highs[i]

            current_atr = atrs[i]
            if pd.isna(current_atr) or current_atr == 0:
                tolerance_val = line_p * fallback_tol
            else:
                tolerance_val = current_atr * atr_multiplier

            dist = abs(bar_p - line_p)
            if dist <= tolerance_val:
                touch_count += 1

        return {
            'p1': (dates[0], y_start),
            'p2': (dates[break_idx], closes[break_idx]),  # 使用实际收盘价而不是外推价格
            'type': trend_type,
            'period': anchor_period,
            'slope': final_slope,
            'break_date': dates[break_idx],
            'touch_count': touch_count
        }

    def _select_best_line(self, group):
        """从组中选择最佳趋势线"""
        if not group:
            return None
        if group[0]['type'] == 'up':
            return min(group, key=lambda x: x['slope'])
        else:
            return max(group, key=lambda x: x['slope'])
    
    def _count_touches(self, df: pd.DataFrame, start_price: float, slope: float, atr: float) -> int:
        """计算趋势线触点数"""
        touches = 0
        tolerance = atr * self.config['atr_multiplier']
        
        for i, row in df.iterrows():
            line_price = start_price + slope * (i - df.index[0]).days
            bar_price = row['BodyLow'] if slope > 0 else row['BodyHigh']
            
            if abs(bar_price - line_price) <= tolerance:
                touches += 1
        
        return touches


# ==================== 技术指标策略 ====================
class TechnicalIndicatorStrategy(IndicatorStrategy):
    """SMA、Bias、动量等指标计算"""
    
    def calculate(self, df: pd.DataFrame) -> Dict[str, Any]:
        """计算所有技术指标"""
        df = df.copy()
        
        # 计算 SMA
        sma_results = {}
        for period in self.config['sma_periods']:
            sma_results[f'sma_{period}'] = df['Close'].rolling(period).mean().tolist()
        
        # 计算 Bias（与200日均线的乖离率）
        sma_200 = df['Close'].rolling(self.config['bias_period']).mean()
        bias = ((df['Close'] - sma_200) / sma_200 * 100).tolist()
        
        # 计算年化波动率
        daily_returns = df['Close'].pct_change()
        volatility = daily_returns.std() * np.sqrt(252)
        
        return {
            'version': self.version,
            'timestamp': datetime.now().isoformat(),
            'sma': sma_results,
            'bias': bias,
            'volatility': volatility,
            'bias_threshold_high': 20.0,
            'bias_threshold_low': -20.0,
            'metadata': {
                'data_length': len(df),
                'last_close': df['Close'].iloc[-1],
                'last_bias': bias[-1],
            }
        }


# ==================== 策略注册 ====================
StrategyRegistry.register('vic_trends', VicTrendStrategy)
StrategyRegistry.register('technical_indicators', TechnicalIndicatorStrategy)


# ==================== 微观审计引擎主类 ====================
class MicroAuditEngine:
    """微观资产审计和计算引擎"""
    
    def __init__(self):
        self.db_path = DB_PATH
        self.cache_dir = CACHE_DIR
    
    def load_asset_data(self, catalog_key: str) -> Optional[pd.DataFrame]:
        """从数据库加载资产数据"""
        try:
            conn = sqlite3.connect(self.db_path)
            query = f"""
            SELECT date, val_open, val_high, val_low, val_close, val_volume
            FROM timeseries_micro
            WHERE catalog_key = ?
            ORDER BY date
            """
            df = pd.read_sql_query(query, conn, params=(catalog_key,))
            conn.close()
            
            if df.empty:
                print(f"❌ 未找到资产数据: {catalog_key}")
                return None
            
            # 重命名列以便后续使用
            df.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
            df['Date'] = pd.to_datetime(df['Date'])
            df.set_index('Date', inplace=True)
            
            # 数据清洗
            df = df.bfill().ffill()
            
            return df
        except Exception as e:
            print(f"❌ 数据加载失败: {e}")
            return None
    
    def calculate_all_indicators(self, df: pd.DataFrame, version: str) -> Dict:
        """计算所有指标（Vic 趋势线 + 技术指标）"""

        results = {
            'catalog_key': None,
            'version': version,
            'timestamp': datetime.now().isoformat(),
            'logic_version': version,  # 记录计算逻辑版本
            'vic_trends': {},
            'technical_indicators': {},
        }

        # 使用策略注册表执行计算
        vic_strategy = StrategyRegistry.get_strategy('vic_trends', version=version)
        results['vic_trends'] = vic_strategy.calculate(df)

        tech_strategy = StrategyRegistry.get_strategy('technical_indicators', version=version)
        results['technical_indicators'] = tech_strategy.calculate(df)

        return results
    
    def save_results(self, catalog_key: str, results: Dict, mode: str = 'production'):
        """保存计算结果到缓存"""
        cache_file = self.cache_dir / f"{catalog_key}_{mode}.json"
        
        # 序列化结果（处理 numpy/datetime 对象）
        serializable = self._make_serializable(results)
        
        with open(cache_file, 'w') as f:
            json.dump(serializable, f, indent=2, default=str)
        
        print(f"✅ 结果已保存: {cache_file}")
        return cache_file
    
    def _make_serializable(self, obj):
        """将对象转换为 JSON 序列化格式"""
        if isinstance(obj, dict):
            return {k: self._make_serializable(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._make_serializable(v) for v in obj]
        elif isinstance(obj, (np.integer, np.floating)):
            return float(obj)
        elif isinstance(obj, (datetime, pd.Timestamp)):
            return str(obj)
        elif pd.isna(obj):
            return None
        else:
            return obj
    
    def process_asset(self, catalog_key: str, mode: str = 'production'):
        """处理单个资产"""
        print(f"\n📊 处理资产: {catalog_key} (模式: {mode})")
        
        # 加载数据
        df = self.load_asset_data(catalog_key)
        if df is None:
            return False
        
        print(f"  ✓ 数据加载完成: {len(df)} 条记录")
        
        # 确定版本
        version = StrategyVersion.EXPERIMENTAL if mode == 'experimental' else StrategyVersion.PRODUCTION
        
        # 计算指标
        results = self.calculate_all_indicators(df, version)
        results['catalog_key'] = catalog_key
        
        # 保存结果
        self.save_results(catalog_key, results, mode=mode)
        
        # 打印摘要
        print(f"  ✓ Vic 趋势线: {len(results['vic_trends'].get('trendlines', []))} 条")
        print(f"  ✓ 锚点数: {len(results['vic_trends'].get('anchors', []))}")
        print(f"  ✓ 版本: {version}")
        
        return True
    
    def batch_process(self, mode: str = 'production', ticker_filter: Optional[str] = None):
        """批量处理资产"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # 获取所有 Daily 频率的资产
            query = "SELECT DISTINCT catalog_key FROM data_catalog WHERE update_frequency = 'Daily'"
            if ticker_filter and ticker_filter != 'ALL':
                query += f" AND catalog_key = '{ticker_filter}'"

            cursor.execute(query)
            assets = [row[0] for row in cursor.fetchall()]
            conn.close()

            if not assets:
                print(f"❌ 未找到匹配的资产")
                return

            print(f"\n🔄 批量处理 {len(assets)} 个资产 (模式: {mode})")

            success, failed = 0, 0
            for asset in assets:
                try:
                    if self.process_asset(asset, mode=mode):
                        success += 1
                    else:
                        failed += 1
                except Exception as e:
                    print(f"❌ {asset}: {str(e)[:80]}")
                    failed += 1

            print(f"\n✅ 批量处理完成: {success} 成功, {failed} 失败")

        except Exception as e:
            print(f"❌ 批量处理失败: {e}")

    def batch_audit_differences(self, limit: Optional[int] = None) -> Dict:
        """批量回测差异报告 - 比较生产版和实验版结果"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # 获取所有 Daily 频率的资产
            query = "SELECT DISTINCT catalog_key FROM data_catalog WHERE update_frequency = 'Daily'"
            cursor.execute(query)
            assets = [row[0] for row in cursor.fetchall()]
            conn.close()

            if not assets:
                print(f"❌ 未找到资产")
                return {}

            if limit:
                assets = assets[:limit]

            print(f"\n🔄 生成差异报告 ({len(assets)} 个资产)")

            differences = []
            processed = 0

            for asset in assets:
                try:
                    prod_file = self.cache_dir / f"{asset}_production.json"
                    exp_file = self.cache_dir / f"{asset}_experimental.json"

                    if not prod_file.exists() or not exp_file.exists():
                        continue

                    with open(prod_file) as f:
                        prod_data = json.load(f)
                    with open(exp_file) as f:
                        exp_data = json.load(f)

                    # 比较趋势线数量
                    prod_lines = len(prod_data.get('vic_trends', {}).get('trendlines', []))
                    exp_lines = len(exp_data.get('vic_trends', {}).get('trendlines', []))

                    # 比较强趋势线数量
                    prod_strong = sum(1 for t in prod_data.get('vic_trends', {}).get('trendlines', [])
                                     if t.get('strength') == 'strong')
                    exp_strong = sum(1 for t in exp_data.get('vic_trends', {}).get('trendlines', [])
                                    if t.get('strength') == 'strong')

                    # 比较锚点数量
                    prod_anchors = len(prod_data.get('vic_trends', {}).get('anchors', []))
                    exp_anchors = len(exp_data.get('vic_trends', {}).get('anchors', []))

                    differences.append({
                        'asset': asset,
                        'prod_trendlines': prod_lines,
                        'exp_trendlines': exp_lines,
                        'trendlines_diff': exp_lines - prod_lines,
                        'prod_strong_lines': prod_strong,
                        'exp_strong_lines': exp_strong,
                        'strong_lines_diff': exp_strong - prod_strong,
                        'prod_anchors': prod_anchors,
                        'exp_anchors': exp_anchors,
                        'anchors_diff': exp_anchors - prod_anchors,
                    })

                    processed += 1
                    if processed % 10 == 0:
                        print(f"  ✓ 已处理 {processed}/{len(assets)} 个资产")

                except Exception as e:
                    print(f"⚠️ {asset} 处理失败: {str(e)[:50]}")
                    continue

            # 生成汇总统计
            summary = {
                'total_assets': len(differences),
                'avg_trendlines_diff': np.mean([d['trendlines_diff'] for d in differences]),
                'avg_strong_lines_diff': np.mean([d['strong_lines_diff'] for d in differences]),
                'avg_anchors_diff': np.mean([d['anchors_diff'] for d in differences]),
                'details': differences,
            }

            print("\n✅ 差异报告生成完成")
            print(f"   资产数量: {summary['total_assets']}")
            print(f"   平均趋势线差异: {summary['avg_trendlines_diff']:.2f}")
            print(f"   平均强趋势线差异: {summary['avg_strong_lines_diff']:.2f}")
            print(f"   平均锚点差异: {summary['avg_anchors_diff']:.2f}")

            return summary

        except Exception as e:
            print(f"❌ 生成差异报告失败: {e}")
            return {}


# ==================== 命令行入口 ====================
def main():
    parser = argparse.ArgumentParser(description='微观审计引擎 - Vic 趋势线和技术指标计算')
    parser.add_argument('--ticker', type=str, default='STOCK_PRICE_NVDA', 
                        help='资产代码 (支持 ALL 表示全部)')
    parser.add_argument('--mode', type=str, choices=['production', 'experimental'], 
                        default='production', help='运行模式')
    
    args = parser.parse_args()
    
    engine = MicroAuditEngine()
    
    if args.ticker == 'ALL':
        engine.batch_process(mode=args.mode)
    else:
        engine.process_asset(args.ticker, mode=args.mode)


if __name__ == '__main__':
    main()
