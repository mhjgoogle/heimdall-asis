# filepath: local/src/tests/test_database_core.py

"""Unit tests for DatabaseCore operations."""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import pytest
import tempfile
import sqlite3
import json
from datetime import datetime, timezone

from local.src.database import DatabaseCore


class TestDatabaseCore:
    """Test DatabaseCore and related operations."""

    @pytest.fixture
    def temp_db(self):
        """Create temporary test database."""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            db_path = f.name

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Create all required tables
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS data_catalog (
            catalog_key TEXT PRIMARY KEY,
            country TEXT NOT NULL,
            scope TEXT NOT NULL,
            role TEXT NOT NULL,
            entity_name TEXT NOT NULL,
            source_api TEXT NOT NULL,
            update_frequency TEXT NOT NULL,
            config_params JSON DEFAULT '{}',
            search_keywords TEXT,
            is_active INTEGER DEFAULT 1,
            priority INTEGER DEFAULT 5
        )
        ''')

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS sync_watermarks (
            catalog_key TEXT PRIMARY KEY,
            last_ingested_at TIMESTAMP,
            last_cleaned_at TIMESTAMP,
            last_synced_at TIMESTAMP,
            last_meta_synced_at TIMESTAMP,
            checksum TEXT
        )
        ''')

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS raw_ingestion_cache (
            request_hash TEXT PRIMARY KEY,
            catalog_key TEXT NOT NULL,
            source_api TEXT NOT NULL,
            raw_payload TEXT NOT NULL,
            inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS timeseries_macro (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            catalog_key TEXT NOT NULL,
            date DATE NOT NULL,
            value REAL,
            UNIQUE(catalog_key, date)
        )
        ''')

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS timeseries_micro (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            catalog_key TEXT NOT NULL,
            date DATE NOT NULL,
            val_open REAL,
            val_high REAL,
            val_low REAL,
            val_close REAL,
            val_volume INTEGER,
            UNIQUE(catalog_key, date)
        )
        ''')

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS news_intel_pool (
            fingerprint TEXT PRIMARY KEY,
            title_hash TEXT NOT NULL,
            catalog_key TEXT NOT NULL,
            published_at TIMESTAMP NOT NULL,
            title TEXT NOT NULL,
            url TEXT NOT NULL,
            sentiment_score REAL,
            ai_summary TEXT
        )
        ''')

        # Insert test data
        cursor.execute('''
        INSERT INTO data_catalog VALUES (
            'TEST_METRIC', 'US', 'MACRO', 'JUDGMENT', 'Test Metric',
            'FRED', 'Monthly', '{"series": "WALCL"}', 'test', 1, 5
        )
        ''')

        cursor.execute('''
        INSERT INTO sync_watermarks VALUES (
            'TEST_METRIC', NULL, NULL, NULL, NULL, NULL
        )
        ''')

        conn.commit()
        conn.close()

        yield db_path

        # Cleanup
        os.unlink(db_path)

    def test_database_session_connection(self, temp_db):
        """Test DatabaseSession can connect."""
        db_core = DatabaseCore(temp_db)
        # Connection should be established
        assert db_core.session is not None
        db_core.close()

    def test_catalog_operations_get_by_key(self, temp_db):
        """Test getting catalog entry by key."""
        db_core = DatabaseCore(temp_db)
        entry = db_core.catalog.get_by_key('TEST_METRIC')

        assert entry is not None
        assert entry['catalog_key'] == 'TEST_METRIC'
        assert entry['role'] == 'JUDGMENT'
        db_core.close()

    def test_catalog_operations_get_active(self, temp_db):
        """Test getting active catalog entries."""
        db_core = DatabaseCore(temp_db)
        entries = db_core.catalog.get_active()

        assert len(entries) >= 1
        assert any(e[0] == 'TEST_METRIC' for e in entries)
        db_core.close()

    def test_catalog_operations_set_active(self, temp_db):
        """Test setting active status."""
        db_core = DatabaseCore(temp_db)

        # Set to inactive
        db_core.catalog.set_active('TEST_METRIC', False)
        entry = db_core.catalog.get_by_key('TEST_METRIC')
        assert entry['is_active'] == 0

        # Set to active
        db_core.catalog.set_active('TEST_METRIC', True)
        entry = db_core.catalog.get_by_key('TEST_METRIC')
        assert entry['is_active'] == 1

        db_core.close()

    def test_catalog_operations_get_role(self, temp_db):
        """Test getting catalog role."""
        db_core = DatabaseCore(temp_db)
        role = db_core.catalog.get_role('TEST_METRIC')

        assert role == 'JUDGMENT'
        db_core.close()

    def test_watermark_operations_ensure_entry(self, temp_db):
        """Test ensuring watermark entry exists."""
        db_core = DatabaseCore(temp_db)

        # Ensure new entry
        db_core.watermarks.ensure_entry('NEW_METRIC')
        watermark = db_core.watermarks.get('NEW_METRIC')

        assert watermark is not None
        assert watermark['catalog_key'] == 'NEW_METRIC'
        db_core.close()

    def test_watermark_operations_update_ingested(self, temp_db):
        """Test updating ingested timestamp."""
        db_core = DatabaseCore(temp_db)

        before = db_core.watermarks.get('TEST_METRIC')
        assert before['last_ingested_at'] is None

        db_core.watermarks.update_ingested('TEST_METRIC')

        after = db_core.watermarks.get('TEST_METRIC')
        assert after['last_ingested_at'] is not None
        db_core.close()

    def test_watermark_operations_get_last_ingested(self, temp_db):
        """Test getting last ingested timestamp."""
        db_core = DatabaseCore(temp_db)

        # Initially None
        assert db_core.watermarks.get_last_ingested('TEST_METRIC') is None

        # After update
        db_core.watermarks.update_ingested('TEST_METRIC')
        last_ingested = db_core.watermarks.get_last_ingested('TEST_METRIC')
        assert last_ingested is not None
        assert isinstance(last_ingested, datetime)
        db_core.close()

    def test_raw_ingestion_operations_insert_or_ignore(self, temp_db):
        """Test inserting raw ingestion data."""
        db_core = DatabaseCore(temp_db)

        db_core.raw_ingestion.insert_or_ignore(
            'hash_123',
            'TEST_METRIC',
            'FRED',
            '{"data": "test"}'
        )

        assert db_core.raw_ingestion.exists('hash_123')
        db_core.close()

    def test_raw_ingestion_operations_idempotent(self, temp_db):
        """Test idempotent insertion (INSERT OR IGNORE)."""
        db_core = DatabaseCore(temp_db)

        # First insert
        db_core.raw_ingestion.insert_or_ignore(
            'hash_123',
            'TEST_METRIC',
            'FRED',
            '{"data": "test1"}'
        )

        # Second insert with same hash (should be ignored)
        db_core.raw_ingestion.insert_or_ignore(
            'hash_123',
            'TEST_METRIC',
            'FRED',
            '{"data": "test2"}'
        )

        assert db_core.raw_ingestion.exists('hash_123')
        db_core.close()

    def test_timeseries_operations_insert_macro(self, temp_db):
        """Test inserting macro time series data."""
        db_core = DatabaseCore(temp_db)

        db_core.timeseries.insert_macro('TEST_METRIC', '2023-01-01', 100.5)

        data = db_core.timeseries.get_macro('TEST_METRIC')
        assert len(data) > 0
        assert data[0][1] == 100.5  # value column
        db_core.close()

    def test_timeseries_operations_insert_micro(self, temp_db):
        """Test inserting micro (OHLCV) time series data."""
        db_core = DatabaseCore(temp_db)

        ohlcv = {
            'open': 100.0,
            'high': 105.0,
            'low': 99.0,
            'close': 104.0,
            'volume': 1000000
        }

        db_core.timeseries.insert_micro('TEST_METRIC', '2023-01-01', ohlcv)

        data = db_core.timeseries.get_micro('TEST_METRIC')
        assert len(data) > 0
        # Returns: date, val_open, val_high, val_low, val_close, val_volume
        assert data[0][0] == '2023-01-01'  # date
        assert data[0][1] == 100.0  # val_open
        assert data[0][2] == 105.0  # val_high
        db_core.close()

    def test_timeseries_operations_idempotent(self, temp_db):
        """Test idempotent time series insertion."""
        db_core = DatabaseCore(temp_db)

        # First insert
        db_core.timeseries.insert_macro('TEST_METRIC', '2023-01-01', 100.5)

        # Second insert (should replace)
        db_core.timeseries.insert_macro('TEST_METRIC', '2023-01-01', 101.5)

        data = db_core.timeseries.get_macro('TEST_METRIC')
        assert data[0][1] == 101.5  # Should have new value
        db_core.close()

    def test_news_operations_insert_or_ignore(self, temp_db):
        """Test inserting news data."""
        db_core = DatabaseCore(temp_db)

        news_data = {
            'title_hash': 'hash_abc',
            'catalog_key': 'TEST_METRIC',
            'published_at': '2023-01-01T12:00:00Z',
            'title': 'Test News',
            'url': 'https://example.com',
            'sentiment_score': 0.5,
            'ai_summary': 'Summary'
        }

        db_core.news.insert_or_ignore('fingerprint_123', news_data)

        assert db_core.news.exists('fingerprint_123')
        db_core.close()

    def test_news_operations_idempotent(self, temp_db):
        """Test idempotent news insertion."""
        db_core = DatabaseCore(temp_db)

        news_data = {
            'title_hash': 'hash_abc',
            'catalog_key': 'TEST_METRIC',
            'published_at': '2023-01-01T12:00:00Z',
            'title': 'Test News',
            'url': 'https://example.com',
            'sentiment_score': 0.5,
            'ai_summary': 'Summary'
        }

        # First insert
        db_core.news.insert_or_ignore('fingerprint_123', news_data)
        assert db_core.news.exists('fingerprint_123')

        # Second insert (should be ignored)
        db_core.news.insert_or_ignore('fingerprint_123', news_data)
        assert db_core.news.exists('fingerprint_123')

        db_core.close()

    def test_transaction_context_manager(self, temp_db):
        """Test transaction context manager."""
        db_core = DatabaseCore(temp_db)

        try:
            with db_core.session.transaction():
                db_core.catalog.insert_or_update('NEW_METRIC', {
                    'country': 'US',
                    'scope': 'MACRO',
                    'role': 'JUDGMENT',
                    'entity_name': 'New Test',
                    'source_api': 'FRED',
                    'update_frequency': 'Monthly',
                    'config_params': '{}',
                    'search_keywords': 'test',
                    'is_active': 1
                })

            entry = db_core.catalog.get_by_key('NEW_METRIC')
            assert entry is not None
        finally:
            db_core.close()
