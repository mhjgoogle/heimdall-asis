"""
Catalog Synchronization Module
自动同步个股到新闻搜索词，确保新增个股自动获得新闻覆盖
"""

import sqlite3
import logging
from pathlib import Path
from typing import Dict, List, Tuple, Optional

logger = logging.getLogger(__name__)


class CatalogSyncManager:
    """
    Manages automatic synchronization of stock keywords to news catalogs.
    When a new stock is added, automatically add its keywords to relevant news sources.
    """
    
    # Stock -> News catalog mappings (based on company type/industry)
    STOCK_TO_NEWS_MAPPING = {
        # US Tech stocks
        'STOCK_PRICE_MSFT': ['NEWS_US_TECH_SECTOR'],
        'STOCK_PRICE_NVDA': ['NEWS_US_TECH_SECTOR'],
        'STOCK_PRICE_TSLA': ['NEWS_US_TECH_SECTOR'],
        
        # Japan automotive stocks
        'STOCK_PRICE_7203': ['NEWS_JP_MACRO_FUNDAMENTALS'],  # Toyota
        'STOCK_PRICE_7267': ['NEWS_JP_MACRO_FUNDAMENTALS'],  # Honda
        
        # Japan chemicals
        'STOCK_PRICE_4063': ['NEWS_US_MATERIALS_SECTOR'],  # Shin-Etsu
        
        # Japan materials
        'STOCK_PRICE_4188': ['NEWS_JP_MACRO_FUNDAMENTALS'],  # Mitsubishi
        
        # Japan semiconductors
        'STOCK_PRICE_8035': ['NEWS_JP_MACRO_FUNDAMENTALS', 'NEWS_US_COMMUNICATION_SECTOR'],  # TEL
    }
    
    # Industry type detection rules (keyword -> news catalog)
    INDUSTRY_RULES = {
        'technology': ['NEWS_US_TECH_SECTOR'],
        'semiconductor': ['NEWS_US_TECH_SECTOR'],
        'software': ['NEWS_US_TECH_SECTOR'],
        'cloud': ['NEWS_US_TECH_SECTOR'],
        'ai': ['NEWS_US_TECH_SECTOR'],
        'ev': ['NEWS_US_TECH_SECTOR'],
        'autonomous': ['NEWS_US_TECH_SECTOR'],
        
        'automotive': ['NEWS_US_INDUSTRIAL_SECTOR', 'NEWS_JP_MACRO_FUNDAMENTALS'],
        'chemical': ['NEWS_US_MATERIALS_SECTOR'],
        'materials': ['NEWS_US_MATERIALS_SECTOR'],
        'steel': ['NEWS_US_MATERIALS_SECTOR'],
        'mining': ['NEWS_US_MATERIALS_SECTOR'],
        
        'pharmaceutical': ['NEWS_US_HEALTHCARE_SECTOR'],
        'biotech': ['NEWS_US_HEALTHCARE_SECTOR'],
        'medical': ['NEWS_US_HEALTHCARE_SECTOR'],
        
        'finance': ['NEWS_US_FINANCIAL_SECTOR'],
        'banking': ['NEWS_US_FINANCIAL_SECTOR'],
        'insurance': ['NEWS_US_FINANCIAL_SECTOR'],
        
        'energy': ['NEWS_US_ENERGY_SECTOR'],
        'oil': ['NEWS_US_ENERGY_SECTOR'],
        'gas': ['NEWS_US_ENERGY_SECTOR'],
        
        'real estate': ['NEWS_US_REALESTATE_SECTOR'],
        'construction': ['NEWS_US_REALESTATE_SECTOR'],
        'property': ['NEWS_US_REALESTATE_SECTOR'],
    }
    
    def __init__(self, db_path: str):
        """Initialize with database path."""
        self.db_path = db_path
        self.db = None
    
    def connect(self):
        """Connect to database."""
        self.db = sqlite3.connect(self.db_path)
        self.db.row_factory = sqlite3.Row
    
    def close(self):
        """Close database connection."""
        if self.db:
            self.db.close()
    
    def get_stock_keywords(self, stock_key: str) -> List[str]:
        """Extract keywords from a stock's search_keywords field."""
        cursor = self.db.cursor()
        result = cursor.execute(
            'SELECT search_keywords FROM data_catalog WHERE catalog_key = ?',
            (stock_key,)
        ).fetchone()
        
        if result and result[0]:
            return [k.strip().lower() for k in result[0].split(',') if k.strip()]
        return []
    
    def detect_news_catalogs_for_stock(self, stock_key: str, stock_keywords: List[str]) -> List[str]:
        """
        Detect which news catalogs should include this stock's keywords.
        Uses predefined mapping + industry detection rules.
        """
        # 1. Check predefined mapping first
        if stock_key in self.STOCK_TO_NEWS_MAPPING:
            return self.STOCK_TO_NEWS_MAPPING[stock_key]
        
        # 2. Use industry detection rules
        news_catalogs = set()
        for keyword in stock_keywords:
            for industry_key, catalogs in self.INDUSTRY_RULES.items():
                if industry_key in keyword:
                    news_catalogs.update(catalogs)
        
        return list(news_catalogs) if news_catalogs else []
    
    def sync_stock_to_news(self, stock_key: str, verbose: bool = True) -> Dict:
        """
        Synchronize a stock's keywords to relevant news catalogs.
        
        Args:
            stock_key: The stock catalog key (e.g., 'STOCK_PRICE_MSFT')
            verbose: Print progress messages
        
        Returns:
            Dictionary with sync results
        """
        result = {
            'stock_key': stock_key,
            'status': 'pending',
            'stock_keywords': [],
            'target_news_catalogs': [],
            'updates': [],
            'errors': []
        }
        
        try:
            # Get stock keywords
            stock_keywords = self.get_stock_keywords(stock_key)
            result['stock_keywords'] = stock_keywords
            
            if not stock_keywords:
                result['status'] = 'no_keywords'
                if verbose:
                    logger.info(f"[{stock_key}] No keywords found")
                return result
            
            # Detect target news catalogs
            target_catalogs = self.detect_news_catalogs_for_stock(stock_key, stock_keywords)
            result['target_news_catalogs'] = target_catalogs
            
            if not target_catalogs:
                result['status'] = 'no_matching_news'
                if verbose:
                    logger.info(f"[{stock_key}] No matching news catalogs found")
                return result
            
            # Update each news catalog with stock keywords
            cursor = self.db.cursor()
            stock_name = stock_key.replace('STOCK_PRICE_', '')
            
            for news_catalog in target_catalogs:
                try:
                    # Get current keywords
                    current_kw = cursor.execute(
                        'SELECT search_keywords FROM data_catalog WHERE catalog_key = ?',
                        (news_catalog,)
                    ).fetchone()
                    
                    current_keywords_str = current_kw[0] if current_kw and current_kw[0] else ''
                    current_keywords_list = [k.strip() for k in current_keywords_str.split(',') if k.strip()]
                    
                    # Add stock keywords if not already present
                    new_keywords = []
                    added = []
                    
                    for kw in current_keywords_list:
                        new_keywords.append(kw)
                    
                    for stock_kw in stock_keywords:
                        # Check if any stock keyword already exists (case-insensitive)
                        exists = any(stock_kw.lower() == k.lower() for k in current_keywords_list)
                        if not exists:
                            new_keywords.append(stock_kw)
                            added.append(stock_kw)
                    
                    # Update if new keywords were added
                    if added:
                        new_keywords_str = ', '.join(new_keywords)
                        cursor.execute(
                            'UPDATE data_catalog SET search_keywords = ? WHERE catalog_key = ?',
                            (new_keywords_str, news_catalog)
                        )
                        self.db.commit()
                        
                        result['updates'].append({
                            'news_catalog': news_catalog,
                            'added_keywords': added,
                            'status': 'updated'
                        })
                        
                        if verbose:
                            logger.info(f"[{stock_key}] Updated {news_catalog}: added {added}")
                    else:
                        result['updates'].append({
                            'news_catalog': news_catalog,
                            'added_keywords': [],
                            'status': 'already_present'
                        })
                        
                        if verbose:
                            logger.info(f"[{stock_key}] {news_catalog}: keywords already present")
                
                except Exception as e:
                    error_msg = f"Failed to update {news_catalog}: {str(e)}"
                    result['errors'].append(error_msg)
                    logger.error(f"[{stock_key}] {error_msg}")
            
            result['status'] = 'success'
            
        except Exception as e:
            result['status'] = 'failed'
            result['errors'].append(str(e))
            logger.error(f"[{stock_key}] Sync failed: {e}")
        
        return result
    
    def sync_all_stocks(self, verbose: bool = True) -> Dict:
        """
        Synchronize all stocks to their relevant news catalogs.
        
        Returns:
            Summary of all sync operations
        """
        result = {
            'total_stocks': 0,
            'successful': 0,
            'failed': 0,
            'no_match': 0,
            'details': []
        }
        
        try:
            # Get all stock catalogs
            cursor = self.db.cursor()
            stocks = cursor.execute(
                "SELECT catalog_key FROM data_catalog WHERE catalog_key LIKE 'STOCK_PRICE_%'"
            ).fetchall()
            
            result['total_stocks'] = len(stocks)
            
            for stock_row in stocks:
                stock_key = stock_row[0]
                sync_result = self.sync_stock_to_news(stock_key, verbose=verbose)
                result['details'].append(sync_result)
                
                if sync_result['status'] == 'success':
                    result['successful'] += 1
                elif sync_result['status'] == 'no_matching_news':
                    result['no_match'] += 1
                elif sync_result['status'] == 'failed':
                    result['failed'] += 1
        
        except Exception as e:
            logger.error(f"Batch sync failed: {e}")
            result['error'] = str(e)
        
        return result
    
    def add_stock_to_catalog(self, stock_key: str, company_name: str, keywords: str) -> bool:
        """
        Add a new stock to the catalog and auto-sync to news.
        
        Args:
            stock_key: Unique key for the stock (e.g., 'STOCK_PRICE_AAPL')
            company_name: Human-readable company name
            keywords: Comma-separated keywords for the stock
        
        Returns:
            True if successful, False otherwise
        """
        try:
            cursor = self.db.cursor()
            
            # Check if already exists
            existing = cursor.execute(
                'SELECT catalog_key FROM data_catalog WHERE catalog_key = ?',
                (stock_key,)
            ).fetchone()
            
            if existing:
                logger.warning(f"Stock {stock_key} already exists in catalog")
                return False
            
            # Insert new stock
            cursor.execute('''
                INSERT INTO data_catalog (
                    catalog_key, country, scope, role, entity_name, 
                    source_api, update_frequency, config_params, 
                    search_keywords, is_active, asset_class
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                stock_key,
                'US',  # Default to US, can be overridden
                'EQUITY',
                'stock_price',
                company_name,
                'yfinance',
                'daily',
                '{}',
                keywords,
                1,  # is_active
                'stock'
            ))
            self.db.commit()
            
            logger.info(f"Added new stock {stock_key} to catalog")
            
            # Auto-sync to news catalogs
            sync_result = self.sync_stock_to_news(stock_key, verbose=True)
            
            return sync_result['status'] in ['success', 'already_present']
        
        except Exception as e:
            logger.error(f"Failed to add stock {stock_key}: {e}")
            return False


def sync_catalog():
    """Main function to synchronize all stocks to news catalogs."""
    import sys
    sys.path.append('.')
    
    from config import AppConfig
    
    manager = CatalogSyncManager(str(AppConfig.DB_PATH))
    manager.connect()
    
    try:
        print("\n" + "=" * 80)
        print("📰 CATALOG SYNCHRONIZATION: Stocks → News Keywords")
        print("=" * 80)
        
        result = manager.sync_all_stocks(verbose=True)
        
        print("\n" + "=" * 80)
        print("📊 SYNC SUMMARY")
        print("=" * 80)
        print(f"Total stocks: {result['total_stocks']}")
        print(f"Successfully synced: {result['successful']}")
        print(f"No matching news catalogs: {result['no_match']}")
        print(f"Failed: {result['failed']}")
        
        print("\n" + "=" * 80)
        print("Details:")
        print("=" * 80)
        for detail in result['details']:
            stock = detail['stock_key']
            status = detail['status']
            updates = len(detail['updates'])
            
            if status == 'success':
                print(f"✅ {stock:20} → {updates} news catalogs updated")
                for update in detail['updates']:
                    if update['status'] == 'updated':
                        kws = ', '.join(update['added_keywords'][:2])
                        print(f"   • {update['news_catalog']:35} + {kws}")
            elif status == 'already_present':
                print(f"⚪ {stock:20} → Already synced")
            elif status == 'no_matching_news':
                print(f"⚠️  {stock:20} → No matching news catalogs")
            else:
                print(f"❌ {stock:20} → {status}")
    
    finally:
        manager.close()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    sync_catalog()
