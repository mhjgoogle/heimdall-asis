# filepath: local/src/database/db_operations.py

"""Database operations for Heimdall-Asis."""

import sqlite3
import json
from datetime import datetime, timezone
from typing import Optional, List, Tuple
from local.config import AppConfig

def get_inactive_catalog_entries() -> List[Tuple]:
    """Get all inactive catalog entries."""
    db_path = AppConfig.DB_PATH
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT catalog_key, source_api, config_params, role, entity_name
            FROM data_catalog
            WHERE is_active = 0
            ORDER BY source_api, catalog_key
        """)
        return cursor.fetchall()

def get_active_catalog_entries() -> List[Tuple]:
    """Get all active catalog entries."""
    db_path = AppConfig.DB_PATH
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT catalog_key, source_api, config_params, update_frequency
            FROM data_catalog
            WHERE is_active = 1
            ORDER BY priority DESC, source_api, catalog_key
        """)
        return cursor.fetchall()

def ensure_sync_watermarks_entry(catalog_key: str):
    """Ensure sync_watermarks has an entry for the catalog_key."""
    db_path = AppConfig.DB_PATH
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR IGNORE INTO sync_watermarks (catalog_key)
            VALUES (?)
        """, (catalog_key,))
        conn.commit()

def activate_asset(catalog_key: str):
    """Set is_active=1 for the asset."""
    db_path = AppConfig.DB_PATH
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE data_catalog SET is_active = 1 WHERE catalog_key = ?
        """, (catalog_key,))
        conn.commit()

def get_catalog_info(catalog_key: str) -> Tuple[Optional[str], Optional[str]]:
    """Get frequency for the catalog entry."""
    db_path = AppConfig.DB_PATH
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT update_frequency FROM data_catalog
            WHERE catalog_key = ?
        """, (catalog_key,))
        result = cursor.fetchone()
        return (result[0] if result else None, None)

def get_catalog_role(catalog_key: str) -> str:
    """Get the role for the catalog entry."""
    db_path = AppConfig.DB_PATH
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT role FROM data_catalog
            WHERE catalog_key = ?
        """, (catalog_key,))
        result = cursor.fetchone()
        return result[0] if result else "JUDGMENT"

def get_last_ingested_at(catalog_key: str) -> Optional[datetime]:
    """Get the last ingested timestamp."""
    db_path = AppConfig.DB_PATH
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT last_ingested_at FROM sync_watermarks
            WHERE catalog_key = ?
        """, (catalog_key,))
        result = cursor.fetchone()
        if result and result[0]:
            dt = datetime.fromisoformat(result[0])
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        return None

def update_sync_watermarks_ingested(catalog_key: str):
    """Update last_ingested_at."""
    db_path = AppConfig.DB_PATH
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        # Use local time for consistency with logs, format: YYYY-MM-DD HH:MM:SS
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        cursor.execute("""
            UPDATE sync_watermarks
            SET last_ingested_at = ?
            WHERE catalog_key = ?
        """, (now, catalog_key))
        conn.commit()

def update_sync_watermarks_cleaned(catalog_key: str):
    """Update last_cleaned_at."""
    db_path = AppConfig.DB_PATH
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        # Use local time for consistency with logs, format: YYYY-MM-DD HH:MM:SS
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        cursor.execute("""
            UPDATE sync_watermarks SET last_cleaned_at = ?
            WHERE catalog_key = ?
        """, (now, catalog_key))
        conn.commit()

def get_pending_cleaning_tasks() -> List[Tuple]:
    """Get raw_ingestion_cache entries newer than last_cleaned_at."""
    db_path = AppConfig.DB_PATH
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT ric.request_hash, ric.catalog_key, ric.source_api, ric.raw_payload, ric.inserted_at
            FROM raw_ingestion_cache ric
            LEFT JOIN sync_watermarks sw ON ric.catalog_key = sw.catalog_key
            WHERE ric.inserted_at > COALESCE(sw.last_cleaned_at, '1970-01-01')
            ORDER BY ric.catalog_key, ric.inserted_at
        """)
        return cursor.fetchall()

def insert_timeseries_macro(data: dict):
    """Insert into timeseries_macro."""
    db_path = AppConfig.DB_PATH
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO timeseries_macro (catalog_key, date, value)
            VALUES (?, ?, ?)
        """, (data['catalog_key'], data['date'], data['value']))
        conn.commit()

def insert_timeseries_micro(data: dict):
    """Insert into timeseries_micro."""
    db_path = AppConfig.DB_PATH
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO timeseries_micro (catalog_key, date, val_open, val_high, val_low, val_close, val_volume)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (data['catalog_key'], data['date'], data['val_open'], data['val_high'], data['val_low'], data['val_close'], data['val_volume']))
        conn.commit()

def insert_news_intel_pool(data: dict):
    """Insert into news_intel_pool."""
    db_path = AppConfig.DB_PATH
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO news_intel_pool (fingerprint, title_hash, catalog_key, published_at, title, url, sentiment_score, ai_summary)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (data['fingerprint'], data['title_hash'], data['catalog_key'], data['published_at'], data['title'], data['url'], data.get('sentiment_score'), data.get('ai_summary')))
        conn.commit()

def insert_raw_ingestion_cache(request_hash: str, catalog_key: str, source_api: str, raw_payload: str):
    """Insert raw data into raw_ingestion_cache."""
    db_path = AppConfig.DB_PATH
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR IGNORE INTO raw_ingestion_cache (request_hash, catalog_key, source_api, raw_payload)
            VALUES (?, ?, ?, ?)
        """, (request_hash, catalog_key, source_api, raw_payload))
        conn.commit()