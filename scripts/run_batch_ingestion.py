#!/usr/bin/env python3
"""
Batch data ingestion script for Heimdall.

Usage:
    python3 run_batch_ingestion.py Daily
    python3 run_batch_ingestion.py HOURLY
    python3 run_batch_ingestion.py Monthly
"""

import sys
import sqlite3
from pathlib import Path
from datetime import datetime
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from local.src.pipeline.incremental_ingestion import IncrementalIngestionEngine
from local.src.pipeline.cleaning_pipeline import CleaningPipeline

DB_PATH = project_root / "local" / "data" / "heimdall.db"


def get_catalogs_by_frequency(frequency: str) -> list:
    """Get all catalog keys for specified frequency."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT catalog_key FROM data_catalog WHERE update_frequency = ? ORDER BY catalog_key",
        (frequency,)
    )
    catalogs = [row[0] for row in cursor.fetchall()]
    conn.close()
    return catalogs


def ingest_batch(frequency: str) -> dict:
    """Ingest all assets for specified frequency."""
    catalogs = get_catalogs_by_frequency(frequency)
    
    if not catalogs:
        logger.warning(f"No catalogs found for frequency: {frequency}")
        return {"success": 0, "failed": 0, "rate_limited": 0}
    
    logger.info(f"Starting ingestion for {len(catalogs)} {frequency} assets")
    
    engine = IncrementalIngestionEngine()
    stats = {"success": 0, "failed": 0, "rate_limited": 0}
    
    for i, catalog_key in enumerate(catalogs, 1):
        try:
            result = engine.ingest_by_asset_key(catalog_key)
            status = result.get('status', 'unknown')
            
            if status == 'success':
                stats["success"] += 1
                logger.debug(f"[{i:2d}/{len(catalogs)}] ✓ {catalog_key}")
            elif 'rate limit' in str(result.get('message', '')).lower() or '429' in str(result):
                stats["rate_limited"] += 1
                logger.info(f"[{i:2d}/{len(catalogs)}] ⊘ {catalog_key} (rate limited)")
            else:
                stats["failed"] += 1
                error_msg = result.get('error') or result.get('message')
                logger.warning(f"[{i:2d}/{len(catalogs)}] ✗ {catalog_key}: {error_msg}")
                
        except Exception as e:
            error_msg = str(e).lower()
            if '429' in error_msg or 'rate limit' in error_msg:
                stats["rate_limited"] += 1
                logger.info(f"[{i:2d}/{len(catalogs)}] ⊘ {catalog_key} (rate limited)")
            else:
                stats["failed"] += 1
                logger.error(f"[{i:2d}/{len(catalogs)}] ✗ {catalog_key}: {str(e)[:100]}")
    
    return stats


def check_and_clean(frequency: str) -> None:
    """Check for new data and run cleaning if needed."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Get last cleaning timestamp
    cursor.execute("""
        SELECT MAX(last_cleaned_at) as last_clean
        FROM sync_watermarks
        WHERE catalog_key IN (
            SELECT DISTINCT catalog_key FROM data_catalog 
            WHERE update_frequency = ?
        )
    """, (frequency,))
    
    result = cursor.fetchone()
    last_cleaned = result[0] if result else None
    
    # Count new data
    if last_cleaned:
        cursor.execute("""
            SELECT COUNT(*) FROM raw_ingestion_cache
            WHERE inserted_at > ?
        """, (last_cleaned,))
    else:
        cursor.execute("SELECT COUNT(*) FROM raw_ingestion_cache")
    
    new_count = cursor.fetchone()[0]
    conn.close()
    
    if new_count > 0:
        logger.info(f"Found {new_count} new records, running cleaning pipeline...")
        try:
            pipeline = CleaningPipeline()
            pipeline.run()
            logger.info("Cleaning pipeline completed successfully")
        except Exception as e:
            logger.error(f"Cleaning pipeline failed: {e}")
    else:
        logger.info("No new data, skipping cleaning")


def main():
    """Main entry point."""
    if len(sys.argv) < 2:
        print("Usage: python3 run_batch_ingestion.py <frequency>")
        print("Frequencies: HOURLY, Daily, Monthly, Quarterly")
        sys.exit(1)
    
    frequency = sys.argv[1]
    
    # Validate frequency
    valid_frequencies = ['HOURLY', 'Daily', 'Monthly', 'Quarterly']
    if frequency not in valid_frequencies:
        logger.error(f"Invalid frequency: {frequency}")
        logger.error(f"Valid options: {', '.join(valid_frequencies)}")
        sys.exit(1)
    
    logger.info(f"Starting batch ingestion for {frequency} frequency")
    
    try:
        # Ingest data
        stats = ingest_batch(frequency)
        
        # Log results
        logger.info(
            f"Ingestion complete: {stats['success']} success, "
            f"{stats['failed']} failed, {stats['rate_limited']} rate limited"
        )
        
        # Check and clean
        check_and_clean(frequency)
        
        logger.info(f"Batch ingestion for {frequency} completed successfully")
        
    except Exception as e:
        logger.error(f"Batch ingestion failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
