# filepath: local/src/tests/test_adapter_manager.py

"""Unit tests for AdapterManager."""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import pytest
from unittest.mock import Mock, patch, MagicMock
import json
import tempfile
import sqlite3
from datetime import datetime

from local.src.adapters.adapter_manager import AdapterManager
from local.src.database import DatabaseCore
from local.config import AppConfig


class TestAdapterManager:
    """Test AdapterManager with database integration."""

    @pytest.fixture
    def temp_db(self):
        """Create temporary test database."""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            db_path = f.name

        # Initialize database
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
            checksum TEXT,
            FOREIGN KEY(catalog_key) REFERENCES data_catalog(catalog_key)
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

        # Insert test data
        cursor.execute('''
        INSERT INTO data_catalog VALUES (
            'TEST_METRIC_FRED', 'US', 'MACRO', 'JUDGMENT', 'Test Metric',
            'FRED', 'Monthly', '{"series": "WALCL"}', 'test,metric', 1, 5
        )
        ''')

        cursor.execute('''
        INSERT INTO data_catalog VALUES (
            'TEST_STOCK_YF', 'US', 'MICRO', 'JUDGMENT', 'Test Stock',
            'yfinance', 'Daily', '{"ticker": "AAPL"}', 'test,stock', 0, 5
        )
        ''')

        cursor.execute('''
        INSERT INTO sync_watermarks VALUES (
            'TEST_METRIC_FRED', NULL, NULL, NULL, NULL, NULL
        )
        ''')

        cursor.execute('''
        INSERT INTO sync_watermarks VALUES (
            'TEST_STOCK_YF', NULL, NULL, NULL, NULL, NULL
        )
        ''')

        conn.commit()
        conn.close()

        yield db_path

        # Cleanup
        os.unlink(db_path)

    def test_ingest_asset_not_found(self, temp_db):
        """Test ingesting non-existent catalog key."""
        db_core = DatabaseCore(temp_db)
        manager = AdapterManager(db_core)

        result = manager.ingest_asset('NONEXISTENT')

        assert result['status'] == 'failed'
        assert 'not found' in result['error'].lower()
        assert manager.ingestion_stats['failed'] == 1

    @patch('local.src.adapters.adapter_manager.create_adapter')
    def test_ingest_asset_invalid_config(self, mock_create_adapter, temp_db):
        """Test ingesting with invalid adapter config."""
        # Mock adapter that reports invalid config
        mock_adapter = MagicMock()
        mock_adapter.validate_config.return_value = False
        mock_create_adapter.return_value = mock_adapter

        db_core = DatabaseCore(temp_db)
        manager = AdapterManager(db_core)

        result = manager.ingest_asset('TEST_METRIC_FRED')

        assert result['status'] == 'failed'
        assert 'invalid config' in result['error'].lower()

    @patch('local.src.adapters.adapter_manager.create_adapter')
    def test_ingest_asset_dry_run_pass(self, mock_create_adapter, temp_db):
        """Test dry run that passes."""
        mock_adapter = MagicMock()
        mock_adapter.validate_config.return_value = True
        mock_adapter.dry_run.return_value = True
        mock_create_adapter.return_value = mock_adapter

        db_core = DatabaseCore(temp_db)
        manager = AdapterManager(db_core)

        result = manager.ingest_asset('TEST_METRIC_FRED', dry_run=True)

        assert result['status'] == 'dry_run_passed'
        assert manager.ingestion_stats['successful'] == 1

    @patch('local.src.adapters.adapter_manager.create_adapter')
    def test_ingest_asset_dry_run_fail(self, mock_create_adapter, temp_db):
        """Test dry run that fails."""
        mock_adapter = MagicMock()
        mock_adapter.validate_config.return_value = True
        mock_adapter.dry_run.return_value = False
        mock_create_adapter.return_value = mock_adapter

        db_core = DatabaseCore(temp_db)
        manager = AdapterManager(db_core)

        result = manager.ingest_asset('TEST_METRIC_FRED', dry_run=True)

        assert result['status'] == 'dry_run_failed'
        assert manager.ingestion_stats['skipped'] == 1

    @patch('local.src.adapters.adapter_manager.create_adapter')
    def test_ingest_asset_success(self, mock_create_adapter, temp_db):
        """Test successful data ingestion."""
        raw_payload = '{"data": "test"}'
        request_hash = 'test_hash_123'

        mock_adapter = MagicMock()
        mock_adapter.validate_config.return_value = True
        mock_adapter.fetch_raw_data.return_value = raw_payload
        mock_adapter.get_request_hash.return_value = request_hash
        mock_create_adapter.return_value = mock_adapter

        db_core = DatabaseCore(temp_db)
        manager = AdapterManager(db_core)

        result = manager.ingest_asset('TEST_METRIC_FRED', dry_run=False)

        assert result['status'] == 'success'
        assert result['stored'] == True
        assert result['request_hash'] == request_hash
        assert manager.ingestion_stats['successful'] == 1

    @patch('local.src.adapters.adapter_manager.create_adapter')
    def test_ingest_asset_idempotent(self, mock_create_adapter, temp_db):
        """Test idempotent ingestion (second run skips)."""
        raw_payload = '{"data": "test"}'

        mock_adapter = MagicMock()
        mock_adapter.validate_config.return_value = True
        mock_adapter.fetch_raw_data.return_value = raw_payload
        mock_adapter.get_request_hash.return_value = 'hash_idempotent_test'
        mock_create_adapter.return_value = mock_adapter

        db_core = DatabaseCore(temp_db)
        manager = AdapterManager(db_core)

        # First ingestion
        result1 = manager.ingest_asset('TEST_METRIC_FRED', dry_run=False)
        if result1['status'] == 'success':
            # Second ingestion (same hash, should skip)
            result2 = manager.ingest_asset('TEST_METRIC_FRED', dry_run=False)
            assert result2['status'] == 'skipped'
            assert result2['stored'] == False


    @patch('local.src.adapters.adapter_manager.create_adapter')
    def test_ingest_batch(self, mock_create_adapter, temp_db):
        """Test batch ingestion."""
        mock_adapter = MagicMock()
        mock_adapter.validate_config.return_value = True
        mock_adapter.fetch_raw_data.return_value = '{"data": "test"}'
        mock_adapter.get_request_hash.return_value = 'hash_123'
        mock_create_adapter.return_value = mock_adapter

        db_core = DatabaseCore(temp_db)
        manager = AdapterManager(db_core)

        # Activate second asset
        db_core.catalog.set_active('TEST_STOCK_YF', True)

        summary = manager.ingest_batch(dry_run=False)

        assert 'results' in summary
        assert 'statistics' in summary
        assert summary['statistics']['total_processed'] > 0

    def test_get_statistics(self, temp_db):
        """Test statistics retrieval."""
        db_core = DatabaseCore(temp_db)
        manager = AdapterManager(db_core)

        stats = manager.get_statistics()

        assert 'total_processed' in stats
        assert 'successful' in stats
        assert 'failed' in stats
        assert 'skipped' in stats

    def test_reset_statistics(self, temp_db):
        """Test statistics reset."""
        db_core = DatabaseCore(temp_db)
        manager = AdapterManager(db_core)

        # Modify stats
        manager.ingestion_stats['total_processed'] = 10
        manager.reset_statistics()

        assert manager.ingestion_stats['total_processed'] == 0
        assert manager.ingestion_stats['successful'] == 0
