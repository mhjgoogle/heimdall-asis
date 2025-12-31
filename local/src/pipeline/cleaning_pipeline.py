# filepath: local/src/pipeline/cleaning_pipeline.py

"""Silver Layer cleaning pipeline.

Processes raw Bronze Layer data (raw_ingestion_cache) through appropriate cleaners
and outputs standardized records to Silver Layer tables:
- timeseries_macro (FRED economic data)
- timeseries_micro (yfinance OHLCV data)  
- news_intel_pool (NewsAPI news data with full article content)
"""

import sys
import os
import json
import sqlite3
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
from dataclasses import dataclass

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))

from local.config.config import AppConfig
from local.src.cleaners.fred_cleaner import FredCleaner
from local.src.cleaners.yfinance_cleaner import YFinanceCleaner
from local.src.cleaners.rss_cleaner import RssCleaner
from local.src.cleaners.newsapi_cleaner import NewsAPICleaner


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


@dataclass
class CleaningStats:
    """Track cleaning operation statistics."""
    source_api: str
    input_records: int = 0
    cleaned_records: int = 0
    failed_records: int = 0
    skipped_records: int = 0
    duration_seconds: float = 0.0
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate."""
        total = self.cleaned_records + self.failed_records
        return (self.cleaned_records / total * 100) if total > 0 else 0.0
    
    def __str__(self) -> str:
        """Format stats for display."""
        return (
            f"{self.source_api:10} | "
            f"In: {self.input_records:4d} | "
            f"Clean: {self.cleaned_records:4d} | "
            f"Fail: {self.failed_records:3d} | "
            f"Skip: {self.skipped_records:3d} | "
            f"Rate: {self.success_rate:5.1f}% | "
            f"Time: {self.duration_seconds:6.2f}s"
        )


class CleaningPipeline:
    """Main cleaning pipeline for Bronze -> Silver transformation."""
    
    def __init__(self, db_path: str = AppConfig.DB_PATH):
        """Initialize pipeline with database connection."""
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        
        # Cleaner registry
        self.cleaners = {
            'FRED': FredCleaner,
            'yfinance': YFinanceCleaner,
            'RSS': RssCleaner,
            'NewsAPI': NewsAPICleaner,
        }

    def run(self, source_api: Optional[str] = None, 
            dry_run: bool = False, limit: Optional[int] = None) -> Dict[str, CleaningStats]:
        """
        Run cleaning pipeline on Bronze Layer data.
        
        Args:
            source_api: Filter by specific source ('FRED', 'yfinance', 'RSS') or None for all
            dry_run: If True, don't insert into Silver Layer (only show what would be done)
            limit: Maximum number of records to process per source
            
        Returns:
            Dictionary of cleaning stats by source API
        """
        logger.info(f"Starting cleaning pipeline (dry_run={dry_run})")
        
        stats = {}
        sources = [source_api] if source_api else list(self.cleaners.keys())
        
        for source in sources:
            if source in self.cleaners:
                cleaner_class = self.cleaners[source]
                stats[source] = self._process_source(cleaner_class, source, dry_run, limit)
        
        self._print_summary(stats)
        return stats
    
    def _process_source(self, cleaner_class, source_api: str, 
                       dry_run: bool, limit: Optional[int]) -> CleaningStats:
        """
        Process records for a specific source API with differential cleaning.
        
        Implements differential cleaning logic:
        1. Get watermark: Query sync_watermarks for last_cleaned_at of this source
        2. Fetch delta: Query raw_ingestion_cache with inserted_at > last_cleaned_at
        3. Transform: Apply appropriate cleaner to each record
        4. Upsert: Insert cleaned records to Silver Layer (atomic transaction)
        5. Update watermark: Update sync_watermarks with new last_cleaned_at
        """
        start_time = datetime.now(timezone.utc)
        stats = CleaningStats(source_api=source_api)
        
        try:
            # Step 1: Get current watermark
            # If ANY catalog_key has NULL last_cleaned_at, we need to clean from the beginning of Bronze data
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_catalogs,
                    COUNT(CASE WHEN sw.last_cleaned_at IS NOT NULL THEN 1 END) as cleaned_count,
                    MIN(COALESCE(sw.last_cleaned_at, '1970-01-01')) as oldest_time
                FROM (
                    SELECT DISTINCT catalog_key FROM raw_ingestion_cache
                    WHERE source_api = ?
                )
                LEFT JOIN sync_watermarks sw USING (catalog_key)
            """, (source_api,))
            
            watermark_row = cursor.fetchone()
            total_catalogs = watermark_row[0]
            cleaned_count = watermark_row[1]
            
            # If some catalogs haven't been cleaned yet, clean all from the start
            # Otherwise use the minimum cleaned time
            if cleaned_count < total_catalogs:
                # Some catalogs never been cleaned - start from Bronze data's beginning
                cursor.execute("""
                    SELECT MIN(inserted_at) FROM raw_ingestion_cache WHERE source_api = ?
                """, (source_api,))
                result = cursor.fetchone()
                last_cleaned_at = result[0] if result[0] else None
            else:
                # All catalogs have been cleaned - use minimum watermark time
                last_cleaned_at = watermark_row[2] if watermark_row[2] != '1970-01-01' else None
            
            # Step 2: Fetch differential records
            # Strategy: Fetch records that are either:
            # 1. Newer than the earliest cleaned timestamp (for already-processed catalogs)
            # 2. ALL records from catalogs that have NEVER been cleaned (last_cleaned_at IS NULL)
            query = """
                SELECT request_hash, catalog_key, source_api, raw_payload, inserted_at
                FROM raw_ingestion_cache
                WHERE source_api = ?
                  AND (
                    -- Either this catalog has never been cleaned
                    catalog_key IN (
                        SELECT catalog_key FROM sync_watermarks 
                        WHERE last_cleaned_at IS NULL
                    )
                    -- Or the data is newer than the oldest cleaned timestamp
                    OR inserted_at > ?
                  )
            """
            params = [source_api]
            if last_cleaned_at:
                params.append(last_cleaned_at)
            else:
                params.append('1970-01-01')  # Safe default for NULL case
            
            query += " ORDER BY inserted_at ASC"
            
            if limit:
                query += " LIMIT ?"
                params.append(limit)
            
            cursor.execute(query, params)
            records = cursor.fetchall()
            stats.input_records = len(records)
            
            if not records:
                logger.info(f"No new {source_api} records to clean (last_cleaned_at: {last_cleaned_at})")
                stats.skipped_records = 0
                return stats
            
            logger.info(f"Processing {stats.input_records} new {source_api} records (delta from {last_cleaned_at})")
            
            # Step 3: Transform records through cleaner
            silver_records = []
            max_inserted_at = last_cleaned_at
            
            for record in records:
                try:
                    raw_payload = json.loads(record['raw_payload'])
                    
                    # Track the maximum inserted_at for watermark update
                    if record['inserted_at']:
                        if max_inserted_at is None or record['inserted_at'] > max_inserted_at:
                            max_inserted_at = record['inserted_at']
                    
                    # Instantiate cleaner for this catalog_key
                    cleaner = cleaner_class(record['catalog_key'])
                    
                    # Process based on source (enable body extraction for RSS and NewsAPI)
                    if source_api in ['RSS', 'NewsAPI']:
                        cleaned = cleaner.process(raw_payload, extract_body=True)
                    else:
                        cleaned = cleaner.process(raw_payload)
                    
                    if cleaned:
                        stats.cleaned_records += 1
                        silver_records.extend(cleaned)
                    else:
                        stats.skipped_records += 1
                        
                except json.JSONDecodeError as e:
                    logger.warning(f"Invalid JSON in {record.get('catalog_key', '?')}: {e}")
                    stats.failed_records += 1
                except Exception as e:
                    logger.warning(f"Error cleaning {record.get('catalog_key', '?')}: {e}")
                    stats.failed_records += 1
            
            # Step 4 & 5: Insert to Silver Layer + update watermark (atomic transaction)
            if not dry_run and silver_records:
                self._atomic_insert_and_update_watermark(
                    source_api, silver_records, max_inserted_at
                )
                logger.info(f"Inserted {len(silver_records)} records to Silver Layer")
                logger.info(f"Updated watermark: last_cleaned_at={max_inserted_at}")
            elif dry_run:
                logger.info(f"[DRY RUN] Would insert {len(silver_records)} records to Silver Layer")
                logger.info(f"[DRY RUN] Would update watermark to {max_inserted_at}")
            
        except Exception as e:
            logger.error(f"Error processing {source_api}: {e}")
            raise
        
        elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
        stats.duration_seconds = elapsed
        
        logger.info(f"✓ {stats}")
        
        return stats
    
    def _insert_silver_records(self, source_api: str, records: List[Dict]) -> int:
        """Insert cleaned records to appropriate Silver Layer table."""
        cursor = self.conn.cursor()
        inserted = 0
        
        try:
            if source_api == 'FRED':
                # timeseries_macro
                for record in records:
                    cursor.execute("""
                        INSERT OR REPLACE INTO timeseries_macro 
                        (catalog_key, date, value)
                        VALUES (?, ?, ?)
                    """, (
                        record.get('catalog_key'),
                        record.get('date'),
                        record.get('value'),
                    ))
                    inserted += 1
            
            elif source_api == 'yfinance':
                # timeseries_micro
                for record in records:
                    cursor.execute("""
                        INSERT OR REPLACE INTO timeseries_micro
                        (catalog_key, date, val_open, val_high, val_low, val_close, val_volume)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (
                        record.get('catalog_key'),
                        record.get('date'),
                        record.get('val_open'),
                        record.get('val_high'),
                        record.get('val_low'),
                        record.get('val_close'),
                        record.get('val_volume'),
                    ))
                    inserted += 1
            
            elif source_api == 'RSS':
                # news_intel_pool
                for record in records:
                    cursor.execute("""
                        INSERT OR REPLACE INTO news_intel_pool
                        (fingerprint, catalog_key, published_at, title, url, body)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (
                        record.get('fingerprint'),
                        record.get('catalog_key'),
                        record.get('published_at'),
                        record.get('title'),
                        record.get('url'),
                        record.get('body'),
                    ))
                    inserted += 1
            
            self.conn.commit()
            
        except Exception as e:
            logger.error(f"Error inserting records: {e}")
            self.conn.rollback()
        
        return inserted
    
    def _atomic_insert_and_update_watermark(self, source_api: str, 
                                            silver_records: List[Dict], 
                                            new_watermark: str):
        """
        Atomically insert Silver Layer records and update watermark.
        
        Uses database transaction to ensure consistency:
        1. Insert all cleaned records to appropriate Silver Layer table
        2. Update sync_watermarks.last_cleaned_at for ALL affected catalog_keys
        3. Commit transaction (all-or-nothing)
        
        This approach updates each affected catalog_key directly rather than
        using a system-level watermark, keeping watermarks aligned with actual data.
        """
        cursor = self.conn.cursor()
        
        try:
            # Begin transaction
            self.conn.execute("BEGIN TRANSACTION")
            
            # Collect unique catalog_keys being inserted
            catalog_keys = set()
            
            # Step 1: Insert records
            inserted = 0
            if source_api == 'FRED':
                for record in silver_records:
                    catalog_keys.add(record.get('catalog_key'))
                    cursor.execute("""
                        INSERT OR REPLACE INTO timeseries_macro 
                        (catalog_key, date, value)
                        VALUES (?, ?, ?)
                    """, (
                        record.get('catalog_key'),
                        record.get('date'),
                        record.get('value'),
                    ))
                    inserted += 1
            
            elif source_api == 'yfinance':
                for record in silver_records:
                    catalog_keys.add(record.get('catalog_key'))
                    cursor.execute("""
                        INSERT OR REPLACE INTO timeseries_micro
                        (catalog_key, date, val_open, val_high, val_low, val_close, val_volume)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (
                        record.get('catalog_key'),
                        record.get('date'),
                        record.get('val_open'),
                        record.get('val_high'),
                        record.get('val_low'),
                        record.get('val_close'),
                        record.get('val_volume'),
                    ))
                    inserted += 1
            
            elif source_api == 'RSS':
                for record in silver_records:
                    catalog_keys.add(record.get('catalog_key'))
                    cursor.execute("""
                        INSERT OR REPLACE INTO news_intel_pool
                        (fingerprint, catalog_key, published_at, title, url, body, author, source_name)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        record.get('fingerprint'),
                        record.get('catalog_key'),
                        record.get('published_at'),
                        record.get('title'),
                        record.get('url'),
                        record.get('body'),
                        record.get('author'),
                        record.get('source_name'),
                    ))
                    inserted += 1
            
            elif source_api == 'NewsAPI':
                for record in silver_records:
                    catalog_keys.add(record.get('catalog_key'))
                    cursor.execute("""
                        INSERT OR REPLACE INTO news_intel_pool
                        (fingerprint, catalog_key, published_at, title, url, body, author, source_name)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        record.get('fingerprint'),
                        record.get('catalog_key'),
                        record.get('published_at'),
                        record.get('title'),
                        record.get('url'),
                        record.get('body'),
                        record.get('author'),
                        record.get('source_name'),
                    ))
                    inserted += 1
            
            # Step 2: Update watermarks for all affected catalog_keys
            for catalog_key in catalog_keys:
                cursor.execute("""
                    INSERT OR IGNORE INTO sync_watermarks (catalog_key)
                    VALUES (?)
                """, (catalog_key,))
                
                cursor.execute("""
                    UPDATE sync_watermarks
                    SET last_cleaned_at = ?, last_ingested_at = ?
                    WHERE catalog_key = ?
                """, (new_watermark, new_watermark, catalog_key))
            
            # Step 3: Commit transaction
            self.conn.commit()
            logger.info(f"Atomic commit: {inserted} records inserted + {len(catalog_keys)} watermarks updated")
            
        except Exception as e:
            logger.error(f"Error in atomic insert: {e}, rolling back...")
            self.conn.rollback()
            raise
    
    def _print_summary(self, stats: Dict[str, CleaningStats]):
        """Print cleaning pipeline summary."""
        print("\n" + "=" * 100)
        print("CLEANING PIPELINE SUMMARY")
        print("=" * 100)
        print(f"{'Source':10} | {'Input':8} | {'Cleaned':8} | {'Failed':6} | {'Skipped':6} | {'Rate':8} | {'Duration':10}")
        print("-" * 100)
        
        total_input = 0
        total_cleaned = 0
        total_failed = 0
        total_skipped = 0
        total_time = 0.0
        
        for source, stat in stats.items():
            print(stat)
            total_input += stat.input_records
            total_cleaned += stat.cleaned_records
            total_failed += stat.failed_records
            total_skipped += stat.skipped_records
            total_time += stat.duration_seconds
        
        print("-" * 100)
        total_rate = (total_cleaned / (total_cleaned + total_failed) * 100) if (total_cleaned + total_failed) > 0 else 0
        print(f"{'TOTAL':10} | {total_input:8d} | {total_cleaned:8d} | {total_failed:6d} | {total_skipped:6d} | {total_rate:7.1f}% | {total_time:9.2f}s")
        print("=" * 100)
    
    def verify_silver_layer(self) -> Dict[str, int]:
        """Verify counts in Silver Layer tables."""
        cursor = self.conn.cursor()
        
        results = {}
        for table in ['timeseries_macro', 'timeseries_micro', 'news_intel_pool']:
            cursor.execute(f"SELECT COUNT(*) as cnt FROM {table}")
            count = cursor.fetchone()['cnt']
            results[table] = count
            logger.info(f"{table:20} {count:6d} records")
        
        return results
    
    def verify_cleaning_consistency(self, source_api: Optional[str] = None) -> Dict[str, Any]:
        """
        Three-phase verification of cleaning consistency.
        
        Phase 1: Data Completeness - Verify all input records were processed
        Phase 2: Deduplication - Check uniqueness of fingerprints/dates
        Phase 3: Watermark Alignment - Verify max(inserted_at) == watermark.last_cleaned_at
        
        Returns:
            Dict with verification results for each phase
        """
        cursor = self.conn.cursor()
        results = {}
        sources = [source_api] if source_api else ['FRED', 'yfinance', 'RSS']
        
        for source in sources:
            logger.info(f"\nVerifying {source} cleaning consistency...")
            source_results = {}
            
            # Phase 1: Data Completeness
            cursor.execute("""
                SELECT COUNT(*) as bronze_count FROM raw_ingestion_cache
                WHERE source_api = ?
            """, (source,))
            bronze_count = cursor.fetchone()['bronze_count']
            
            # Get expected silver count
            if source == 'FRED':
                cursor.execute("SELECT COUNT(*) as silver_count FROM timeseries_macro")
            elif source == 'yfinance':
                cursor.execute("SELECT COUNT(*) as silver_count FROM timeseries_micro")
            elif source == 'RSS':
                cursor.execute("SELECT COUNT(*) as silver_count FROM news_intel_pool")
            
            silver_count = cursor.fetchone()['silver_count']
            source_results['phase_1_completeness'] = {
                'bronze_records': bronze_count,
                'silver_records': silver_count,
                'status': 'OK' if silver_count > 0 else 'WARNING: No silver records'
            }
            logger.info(f"  Phase 1 [Completeness]: Bronze={bronze_count}, Silver={silver_count}")
            
            # Phase 2: Deduplication
            if source == 'RSS':
                # Check fingerprint uniqueness
                cursor.execute("""
                    SELECT COUNT(*) as total, COUNT(DISTINCT fingerprint) as unique_fps
                    FROM news_intel_pool
                """)
                row = cursor.fetchone()
                total_records = row['total']
                unique_fps = row['unique_fps']
                dedup_rate = (unique_fps / total_records * 100) if total_records > 0 else 0
                source_results['phase_2_deduplication'] = {
                    'total_records': total_records,
                    'unique_fingerprints': unique_fps,
                    'dedup_rate_percent': dedup_rate,
                    'status': f'OK ({dedup_rate:.1f}% unique)' if dedup_rate == 100 else f'WARNING ({dedup_rate:.1f}% unique)'
                }
                logger.info(f"  Phase 2 [Deduplication]: {unique_fps}/{total_records} unique fingerprints ({dedup_rate:.1f}%)")
            else:
                # Check date uniqueness for timeseries
                table = 'timeseries_macro' if source == 'FRED' else 'timeseries_micro'
                cursor.execute(f"""
                    SELECT COUNT(*) as total, COUNT(DISTINCT date) as unique_dates
                    FROM {table}
                """)
                row = cursor.fetchone()
                total_records = row['total']
                unique_dates = row['unique_dates']
                source_results['phase_2_deduplication'] = {
                    'total_records': total_records,
                    'unique_dates': unique_dates,
                    'status': 'OK (date-based dedup)'
                }
                logger.info(f"  Phase 2 [Deduplication]: {unique_dates}/{total_records} unique dates")
            
            # Phase 3: Watermark Alignment
            cursor.execute("""
                SELECT last_cleaned_at FROM sync_watermarks
                WHERE catalog_key = ?
            """, (f"SYSTEM_CLEANING_{source}",))
            watermark_row = cursor.fetchone()
            watermark_time = watermark_row['last_cleaned_at'] if watermark_row else None
            
            cursor.execute("""
                SELECT MAX(inserted_at) as max_inserted FROM raw_ingestion_cache
                WHERE source_api = ?
            """, (source,))
            max_inserted = cursor.fetchone()['max_inserted']
            
            alignment_ok = watermark_time == max_inserted if max_inserted else watermark_time is None
            source_results['phase_3_watermark_alignment'] = {
                'watermark_last_cleaned': watermark_time,
                'bronze_max_inserted': max_inserted,
                'alignment_ok': alignment_ok,
                'status': 'OK (aligned)' if alignment_ok else 'WARNING (misaligned)'
            }
            logger.info(f"  Phase 3 [Watermark]: {watermark_time} == {max_inserted} ? {alignment_ok}")
            
            results[source] = source_results
        
        return results
    
    def show_watermarks(self):
        """Display current cleaning watermarks."""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT catalog_key, last_cleaned_at
            FROM sync_watermarks
            WHERE catalog_key LIKE 'SYSTEM_CLEANING_%'
            ORDER BY catalog_key
        """)
        rows = cursor.fetchall()
        
        if not rows:
            logger.info("No cleaning watermarks found")
            return
        
        print("\n" + "=" * 80)
        print("DIFFERENTIAL CLEANING WATERMARKS (last_cleaned_at)")
        print("=" * 80)
        for row in rows:
            status = row['last_cleaned_at'] or "Never cleaned"
            print(f"  {row['catalog_key']:30} {status}")
        print("=" * 80 + "\n")
    
    def reset_watermark(self, source_api: Optional[str] = None):
        """
        Reset cleaning watermark to re-process all records.
        
        Resets last_cleaned_at for all catalog_keys from the specified source.
        
        Args:
            source_api: Reset specific source ('FRED', 'yfinance', 'RSS') or None for all
        """
        cursor = self.conn.cursor()
        
        if source_api:
            # Find all catalog_keys from this source in raw_ingestion_cache
            cursor.execute("""
                SELECT DISTINCT catalog_key FROM raw_ingestion_cache
                WHERE source_api = ?
            """, (source_api,))
            
            catalog_keys = [row['catalog_key'] for row in cursor.fetchall()]
            
            if catalog_keys:
                # Reset watermark for each affected catalog_key
                placeholders = ','.join('?' * len(catalog_keys))
                cursor.execute(f"""
                    UPDATE sync_watermarks 
                    SET last_cleaned_at = NULL 
                    WHERE catalog_key IN ({placeholders})
                """, catalog_keys)
                
                self.conn.commit()
                logger.info(f"Reset watermark for {source_api} ({len(catalog_keys)} catalog_keys)")
            else:
                logger.info(f"No catalog_keys found for source: {source_api}")
        else:
            # Reset all cleaning watermarks (all catalog_keys with data)
            cursor.execute("""
                UPDATE sync_watermarks 
                SET last_cleaned_at = NULL 
                WHERE catalog_key IN (
                    SELECT DISTINCT catalog_key FROM raw_ingestion_cache
                )
            """)
            self.conn.commit()
            logger.info("Reset all cleaning watermarks")
    
    def close(self):
        """Close database connection."""
        self.conn.close()

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Bronze -> Silver Layer cleaning pipeline with differential processing')
    parser.add_argument('--source', type=str, choices=['FRED', 'yfinance', 'RSS', 'NewsAPI'],
                       help='Filter by source API')
    parser.add_argument('--dry-run', action='store_true',
                       help='Show what would be done without inserting to Silver Layer')
    parser.add_argument('--limit', type=int,
                       help='Limit records processed per source')
    parser.add_argument('--verify', action='store_true',
                       help='Verify Silver Layer record counts')
    parser.add_argument('--full-verify', action='store_true',
                       help='Run three-phase verification (completeness, dedup, watermark alignment)')
    parser.add_argument('--show-watermarks', action='store_true',
                       help='Display current cleaning watermarks')
    parser.add_argument('--reset-watermark', type=str, nargs='?', const='ALL',
                       choices=['FRED', 'yfinance', 'RSS', 'ALL'],
                       help='Reset watermark to re-process records')
    
    args = parser.parse_args()
    
    pipeline = CleaningPipeline()
    
    try:
        # Handle watermark operations
        if args.show_watermarks:
            pipeline.show_watermarks()
        
        if args.reset_watermark:
            source = None if args.reset_watermark == 'ALL' else args.reset_watermark
            pipeline.reset_watermark(source)
            pipeline.show_watermarks()
        
        # Run cleaning (unless only watermark ops were requested)
        if not args.show_watermarks and not args.reset_watermark:
            stats = pipeline.run(
                source_api=args.source,
                dry_run=args.dry_run,
                limit=args.limit
            )
            
            # Verify if requested
            if args.verify:
                logger.info("\nVerifying Silver Layer contents:")
                pipeline.verify_silver_layer()
            
            # Full three-phase verification if requested
            if args.full_verify:
                logger.info("\n" + "=" * 80)
                logger.info("THREE-PHASE VERIFICATION")
                logger.info("=" * 80)
                verification_results = pipeline.verify_cleaning_consistency(args.source)
                
                # Print summary
                for source, results in verification_results.items():
                    print(f"\n{source}:")
                    for phase, phase_results in results.items():
                        print(f"  {phase}: {phase_results['status']}")
            
            # Show watermarks after successful run
            pipeline.show_watermarks()
        
    finally:
        pipeline.close()
