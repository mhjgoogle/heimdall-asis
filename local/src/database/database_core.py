# filepath: local/src/database/database_core.py

import sqlite3
import logging
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Optional, Dict, Any
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


class DatabaseSession:
    """Manages SQLite database connections with context manager support."""

    _local = threading.local()

    def __init__(self, db_path: Path):
        self.db_path = str(db_path)
        self._connection: Optional[sqlite3.Connection] = None

    def _get_connection(self) -> sqlite3.Connection:
        if not hasattr(self._local, 'conn') or self._local.conn is None:
            self._local.conn = sqlite3.connect(self.db_path)
            self._local.conn.row_factory = sqlite3.Row
            self._local.conn.execute("PRAGMA journal_mode=WAL")
        return self._local.conn

    def execute(self, query: str, params: tuple = ()):
        """Execute query and return cursor."""
        conn = self._get_connection()
        return conn.execute(query, params)

    def execute_many(self, query: str, params_list: list):
        """Execute multiple queries."""
        conn = self._get_connection()
        return conn.executemany(query, params_list)

    def commit(self):
        """Commit transaction."""
        if hasattr(self._local, 'conn') and self._local.conn:
            self._local.conn.commit()

    def rollback(self):
        """Rollback transaction."""
        if hasattr(self._local, 'conn') and self._local.conn:
            self._local.conn.rollback()

    def close(self):
        """Close connection."""
        if hasattr(self._local, 'conn') and self._local.conn:
            self._local.conn.close()
            self._local.conn = None

    @contextmanager
    def transaction(self):
        """Context manager for database transactions."""
        try:
            yield self
            self.commit()
        except Exception as e:
            self.rollback()
            logger.error(f"Transaction failed: {e}")
            raise


class DataCatalogOperations:
    """CRUD operations for data_catalog table."""

    def __init__(self, session: DatabaseSession):
        self.session = session

    def insert_or_update(self, catalog_key: str, data: Dict[str, Any]):
        """Insert or update catalog entry (idempotent)."""
        with self.session.transaction():
            self.session.execute("""
                INSERT OR REPLACE INTO data_catalog (
                    catalog_key, country, scope, role, entity_name,
                    source_api, update_frequency, config_params, search_keywords, is_active
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                catalog_key,
                data.get('country'),
                data.get('scope'),
                data.get('role'),
                data.get('entity_name'),
                data.get('source_api'),
                data.get('update_frequency'),
                data.get('config_params'),
                data.get('search_keywords'),
                data.get('is_active', 1)
            ))

    def get_active(self):
        """Get all active catalog entries ordered by priority."""
        cursor = self.session.execute("""
            SELECT catalog_key, source_api, config_params, update_frequency
            FROM data_catalog
            WHERE is_active = 1
            ORDER BY source_api, catalog_key
        """)
        return cursor.fetchall()

    def get_inactive(self):
        """Get all inactive catalog entries."""
        cursor = self.session.execute("""
            SELECT catalog_key, source_api, config_params, role, entity_name
            FROM data_catalog
            WHERE is_active = 0
            ORDER BY source_api, catalog_key
        """)
        return cursor.fetchall()

    def get_by_key(self, catalog_key: str):
        """Get specific catalog entry."""
        cursor = self.session.execute("""
            SELECT * FROM data_catalog WHERE catalog_key = ?
        """, (catalog_key,))
        return cursor.fetchone()

    def set_active(self, catalog_key: str, is_active: bool = True):
        """Set active status for catalog entry."""
        with self.session.transaction():
            self.session.execute("""
                UPDATE data_catalog SET is_active = ? WHERE catalog_key = ?
            """, (1 if is_active else 0, catalog_key))

    def get_role(self, catalog_key: str) -> Optional[str]:
        """Get role (JUDGMENT or VALIDATION) for catalog entry."""
        cursor = self.session.execute("""
            SELECT role FROM data_catalog WHERE catalog_key = ?
        """, (catalog_key,))
        result = cursor.fetchone()
        return result[0] if result else None


class SyncWatermarkOperations:
    """CRUD operations for sync_watermarks table."""

    def __init__(self, session: DatabaseSession):
        self.session = session

    def ensure_entry(self, catalog_key: str):
        """Ensure watermark entry exists (idempotent)."""
        with self.session.transaction():
            self.session.execute("""
                INSERT OR IGNORE INTO sync_watermarks (catalog_key)
                VALUES (?)
            """, (catalog_key,))

    def get(self, catalog_key: str):
        """Get watermark entry for catalog_key."""
        cursor = self.session.execute("""
            SELECT * FROM sync_watermarks WHERE catalog_key = ?
        """, (catalog_key,))
        return cursor.fetchone()

    def update_ingested(self, catalog_key: str):
        """Update last_ingested_at timestamp."""
        with self.session.transaction():
            # Use local time for consistency with logs, format: YYYY-MM-DD HH:MM:SS
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.session.execute("""
                UPDATE sync_watermarks
                SET last_ingested_at = ?
                WHERE catalog_key = ?
            """, (now, catalog_key))

    def update_cleaned(self, catalog_key: str):
        """Update last_cleaned_at timestamp."""
        with self.session.transaction():
            # Use local time for consistency with logs, format: YYYY-MM-DD HH:MM:SS
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.session.execute("""
                UPDATE sync_watermarks
                SET last_cleaned_at = ?
                WHERE catalog_key = ?
            """, (now, catalog_key))

    def update_synced(self, catalog_key: str):
        """Update last_synced_at timestamp."""
        with self.session.transaction():
            now = datetime.now(timezone.utc).isoformat()
            self.session.execute("""
                UPDATE sync_watermarks
                SET last_synced_at = ?
                WHERE catalog_key = ?
            """, (now, catalog_key))

    def get_last_ingested(self, catalog_key: str) -> Optional[datetime]:
        """Get last ingested timestamp."""
        cursor = self.session.execute("""
            SELECT last_ingested_at FROM sync_watermarks WHERE catalog_key = ?
        """, (catalog_key,))
        result = cursor.fetchone()
        if result and result[0]:
            try:
                dt = datetime.fromisoformat(result[0])
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except ValueError:
                return None
        return None


class RawIngestionOperations:
    """CRUD operations for raw_ingestion_cache table."""

    def __init__(self, session: DatabaseSession):
        self.session = session

    def insert_or_ignore(self, request_hash: str, catalog_key: str, source_api: str, raw_payload: str):
        """Insert raw payload with idempotent guarantee (INSERT OR IGNORE)."""
        with self.session.transaction():
            self.session.execute("""
                INSERT OR IGNORE INTO raw_ingestion_cache
                (request_hash, catalog_key, source_api, raw_payload)
                VALUES (?, ?, ?, ?)
            """, (request_hash, catalog_key, source_api, raw_payload))

    def exists(self, request_hash: str) -> bool:
        """Check if request_hash already exists."""
        cursor = self.session.execute("""
            SELECT 1 FROM raw_ingestion_cache WHERE request_hash = ?
        """, (request_hash,))
        return cursor.fetchone() is not None

    def get_pending_cleaning(self, limit: Optional[int] = None):
        """Get raw cache entries newer than last_cleaned_at."""
        query = """
            SELECT ric.request_hash, ric.catalog_key, ric.source_api, ric.raw_payload, ric.inserted_at
            FROM raw_ingestion_cache ric
            LEFT JOIN sync_watermarks sw ON ric.catalog_key = sw.catalog_key
            WHERE ric.inserted_at > COALESCE(sw.last_cleaned_at, '1970-01-01')
            ORDER BY ric.catalog_key, ric.inserted_at
        """
        if limit:
            query += f" LIMIT {limit}"
        cursor = self.session.execute(query)
        return cursor.fetchall()


class TimeSeriesOperations:
    """CRUD operations for timeseries tables."""

    def __init__(self, session: DatabaseSession):
        self.session = session

    def insert_macro(self, catalog_key: str, date: str, value: float):
        """Insert macro time series data (idempotent)."""
        with self.session.transaction():
            self.session.execute("""
                INSERT OR REPLACE INTO timeseries_macro (catalog_key, date, value)
                VALUES (?, ?, ?)
            """, (catalog_key, date, value))

    def insert_micro(self, catalog_key: str, date: str, ohlcv: Dict[str, float]):
        """Insert micro (OHLCV) time series data (idempotent)."""
        with self.session.transaction():
            self.session.execute("""
                INSERT OR REPLACE INTO timeseries_micro
                (catalog_key, date, val_open, val_high, val_low, val_close, val_volume)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                catalog_key,
                date,
                ohlcv.get('open'),
                ohlcv.get('high'),
                ohlcv.get('low'),
                ohlcv.get('close'),
                ohlcv.get('volume')
            ))

    def get_macro(self, catalog_key: str, limit: Optional[int] = None):
        """Get macro time series data."""
        query = "SELECT date, value FROM timeseries_macro WHERE catalog_key = ? ORDER BY date DESC"
        if limit:
            query += f" LIMIT {limit}"
        cursor = self.session.execute(query, (catalog_key,))
        return cursor.fetchall()

    def get_micro(self, catalog_key: str, limit: Optional[int] = None):
        """Get micro (OHLCV) time series data."""
        query = "SELECT date, val_open, val_high, val_low, val_close, val_volume FROM timeseries_micro WHERE catalog_key = ? ORDER BY date DESC"
        if limit:
            query += f" LIMIT {limit}"
        cursor = self.session.execute(query, (catalog_key,))
        return cursor.fetchall()


class NewsOperations:
    """CRUD operations for news_intel_pool table."""

    def __init__(self, session: DatabaseSession):
        self.session = session

    def insert_or_ignore(self, fingerprint: str, data: Dict[str, Any]):
        """Insert news with idempotent guarantee (INSERT OR IGNORE)."""
        with self.session.transaction():
            self.session.execute("""
                INSERT OR IGNORE INTO news_intel_pool
                (fingerprint, title_hash, catalog_key, published_at, title, url, sentiment_score, ai_summary)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                fingerprint,
                data.get('title_hash'),
                data.get('catalog_key'),
                data.get('published_at'),
                data.get('title'),
                data.get('url'),
                data.get('sentiment_score'),
                data.get('ai_summary')
            ))

    def exists(self, fingerprint: str) -> bool:
        """Check if fingerprint already exists."""
        cursor = self.session.execute("""
            SELECT 1 FROM news_intel_pool WHERE fingerprint = ?
        """, (fingerprint,))
        return cursor.fetchone() is not None


class DatabaseCore:
    """Central database manager combining all operations."""

    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.session = DatabaseSession(db_path)

        self.catalog = DataCatalogOperations(self.session)
        self.watermarks = SyncWatermarkOperations(self.session)
        self.raw_ingestion = RawIngestionOperations(self.session)
        self.timeseries = TimeSeriesOperations(self.session)
        self.news = NewsOperations(self.session)

    def close(self):
        """Close database connection."""
        self.session.close()
