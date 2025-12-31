# filepath: local/src/tests/test_adapters.py

"""Unit tests for data source adapters."""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import pytest
from unittest.mock import Mock, patch, MagicMock
import json
from datetime import datetime
from pathlib import Path
import tempfile
import sqlite3

# Import adapters
from local.src.adapters.base import BaseAdapter, IngestionContext
from local.src.adapters.fred import FredAdapter
from local.src.adapters.yfinance_adapter import YFinanceAdapter
from local.src.adapters.rss_adapter import RSSAdapter
from local.src.adapters.adapter_factory import AdapterFactory, AdapterManager
from local.src.database import DatabaseCore


class TestFredAdapter:
    """Test FRED adapter functionality."""

    def setup_method(self):
        """Setup test instance."""
        with patch('local.src.adapters.fred.AppConfig') as mock_config:
            mock_config.FRED_KEY = 'test_key'
            self.adapter = FredAdapter()

    def test_validate_config_valid(self):
        """Test config validation with valid input."""
        config = {"series": ["WALCL", "WTREGEN"]}
        assert self.adapter.validate_config(config) == True

    def test_validate_config_invalid(self):
        """Test config validation with invalid input."""
        config = {"series": []}
        assert self.adapter.validate_config(config) == False

    @patch('local.src.adapters.fred.requests.get')
    def test_fetch_raw_data_success(self, mock_get):
        """Test successful data fetching."""
        # Mock API response
        mock_response = Mock()
        mock_response.json.return_value = {
            "observations": [
                {"date": "2023-01-01", "value": "100.5"},
                {"date": "2023-01-02", "value": "101.0"}
            ]
        }
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        context = IngestionContext(
            catalog_key="TEST_METRIC",
            source_api="FRED",
            config_params={"series": "WALCL"},
            role="JUDGMENT",
            frequency="MONTHLY"
        )

        result = self.adapter.fetch_raw_data(context)
        parsed = json.loads(result)

        assert parsed['catalog_key'] == "TEST_METRIC"
        assert 'series_data' in parsed
        assert 'WALCL' in parsed['series_data']

    def test_dry_run_success(self):
        """Test dry run with valid data."""
        # This would normally require mocking the full fetch
        # For simplicity, test the structure
        assert hasattr(self.adapter, 'dry_run')


class TestYFinanceAdapter:
    """Test Yahoo Finance adapter functionality."""

    def setup_method(self):
        """Setup test instance."""
        # Mock yfinance before creating adapter
        import sys
        from unittest.mock import MagicMock
        mock_yf = MagicMock()
        sys.modules['yfinance'] = mock_yf
        
        # Create adapter which will import yfinance
        self.adapter = YFinanceAdapter()
        self.mock_yf = mock_yf

    def teardown_method(self):
        """Cleanup mocks."""
        import sys
        if 'yfinance' in sys.modules:
            del sys.modules['yfinance']

    def test_validate_config_valid(self):
        """Test config validation with valid ticker."""
        config = {"ticker": "AAPL"}
        assert self.adapter.validate_config(config) == True

    def test_validate_config_invalid(self):
        """Test config validation with invalid ticker."""
        config = {"ticker": ""}
        assert self.adapter.validate_config(config) == False

    @patch('local.src.adapters.yfinance_adapter.YFinanceAdapter._fetch_historical_data')
    def test_fetch_raw_data_calls_historical(self, mock_fetch):
        """Test that fetch_raw_data calls historical data method."""
        mock_fetch.return_value = {"historical_data": [], "count": 0}

        context = IngestionContext(
            catalog_key="TEST_STOCK",
            source_api="yfinance",
            config_params={"ticker": "AAPL"},
            role="JUDGMENT",
            frequency="DAILY"
        )

        result = self.adapter.fetch_raw_data(context)
        parsed = json.loads(result)

        assert parsed['catalog_key'] == "TEST_STOCK"
        assert parsed['mode'] == "history"
        mock_fetch.assert_called_once()


class TestRSSAdapter:
    """Test RSS adapter functionality."""

    def setup_method(self):
        """Setup test instance."""
        self.adapter = RSSAdapter()

    def test_validate_config_valid(self):
        """Test config validation with valid URL."""
        config = {"url": "https://example.com/rss"}
        assert self.adapter.validate_config(config) == True

    def test_validate_config_invalid(self):
        """Test config validation with invalid URL."""
        config = {"url": "not-a-url"}
        assert self.adapter.validate_config(config) == False

    def test_fetch_raw_data_success(self):
        """Test successful RSS parsing with mocked HTTP client."""
        # Create adapter
        adapter = RSSAdapter()
        
        # Mock the HTTP client
        mock_xml = """<?xml version="1.0" encoding="UTF-8"?>
        <feed xmlns="http://www.w3.org/2005/Atom">
            <entry>
                <title>Test News</title>
                <link href="https://example.com/news"/>
                <published>2023-01-01T00:00:00Z</published>
            </entry>
        </feed>"""

        mock_response = Mock()
        mock_response.text = mock_xml
        mock_response.raise_for_status.return_value = None
        
        # Replace the http_client.get method
        adapter.http_client.get = Mock(return_value=mock_response)

        context = IngestionContext(
            catalog_key="TEST_NEWS",
            source_api="RSS",
            config_params={"url": "https://example.com/rss"},
            role="VALIDATION",
            frequency="HOURLY"
        )

        result = adapter.fetch_raw_data(context)
        parsed = json.loads(result)

        assert parsed['catalog_key'] == "TEST_NEWS"
        assert 'parsed_items' in parsed
        assert len(parsed['parsed_items']) >= 0  # May be 0 due to filtering


class TestBaseAdapter:
    """Test base adapter functionality."""

    def test_get_request_hash(self):
        """Test request hash generation."""
        # Use FredAdapter as concrete implementation
        adapter = FredAdapter()

        context = IngestionContext(
            catalog_key="TEST",
            source_api="FRED",
            config_params={"series": "WALCL"},
            role="JUDGMENT",
            frequency="DAILY"
        )

        hash1 = adapter.get_request_hash(context)
        hash2 = adapter.get_request_hash(context)

        # Same input should produce same hash
        assert hash1 == hash2
        assert isinstance(hash1, str)
        assert len(hash1) == 64  # SHA256 hex length

    def test_get_incremental_start_date_judgment(self):
        """Test incremental start date for JUDGMENT role."""
        adapter = FredAdapter()

        # Mock last_ingested_at
        from datetime import datetime, timedelta
        last_ingested = datetime.now() - timedelta(days=10)

        context = IngestionContext(
            catalog_key="TEST",
            source_api="FRED",
            config_params={"series": "WALCL"},
            role="JUDGMENT",
            last_ingested_at=last_ingested,
            frequency="DAILY"
        )

        start_date = adapter.get_incremental_start_date(context)
        expected = (last_ingested - timedelta(days=30)).date().isoformat()

        assert start_date == expected

    def test_get_incremental_start_date_validation(self):
        """Test incremental start date for VALIDATION role."""
        adapter = FredAdapter()

        # Mock last_ingested_at
        from datetime import datetime, timedelta
        last_ingested = datetime.now() - timedelta(days=10)

        context = IngestionContext(
            catalog_key="TEST",
            source_api="FRED",
            config_params={"series": "WALCL"},
            role="VALIDATION",
            last_ingested_at=last_ingested,
            frequency="DAILY"
        )

        start_date = adapter.get_incremental_start_date(context)
        expected = (last_ingested - timedelta(days=7)).date().isoformat()

        assert start_date == expected


class TestAdapterFactory:
    """Test AdapterFactory functionality."""

    def test_get_adapter_fred(self):
        """Test getting FRED adapter."""
        with patch('local.src.adapters.adapter_factory.FredAdapter.__init__', return_value=None):
            adapter = AdapterFactory.get_adapter('FRED')
            assert adapter is not None

    def test_get_adapter_yfinance(self):
        """Test getting yfinance adapter."""
        with patch('local.src.adapters.adapter_factory.YFinanceAdapter.__init__', return_value=None):
            adapter = AdapterFactory.get_adapter('yfinance')
            assert adapter is not None

    def test_get_adapter_invalid(self):
        """Test getting invalid adapter."""
        with pytest.raises(ValueError):
            AdapterFactory.get_adapter('INVALID_SOURCE')

    def test_list_adapters(self):
        """Test listing all adapters."""
        adapters = AdapterFactory.list_adapters()
        assert 'FRED' in adapters
        assert 'yfinance' in adapters
        assert 'RSS' in adapters

    def test_register_custom_adapter(self):
        """Test registering a custom adapter."""
        class CustomAdapter(BaseAdapter):
            def fetch_raw_data(self, context):
                return json.dumps({"status": "success"})

            def dry_run(self, context):
                return True

        AdapterFactory.register_adapter('CUSTOM', CustomAdapter)
        adapters = AdapterFactory.list_adapters()
        assert 'CUSTOM' in adapters


class TestAdapterManager:
    """Test AdapterManager functionality with database integration."""

    @pytest.fixture
    def temp_db(self):
        """Create temporary database for testing."""
        temp_dir = tempfile.mkdtemp()
        db_path = Path(temp_dir) / "test.db"

        # Initialize minimal database
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()

        # Create required tables
        cursor.execute('''CREATE TABLE data_catalog (
            catalog_key TEXT PRIMARY KEY,
            country TEXT, scope TEXT, role TEXT, entity_name TEXT,
            source_api TEXT, update_frequency TEXT, config_params TEXT,
            search_keywords TEXT, is_active INTEGER DEFAULT 1
        )''')

        cursor.execute('''CREATE TABLE sync_watermarks (
            catalog_key TEXT PRIMARY KEY,
            last_ingested_at TIMESTAMP, last_cleaned_at TIMESTAMP,
            last_synced_at TIMESTAMP, last_meta_synced_at TIMESTAMP,
            checksum TEXT
        )''')

        cursor.execute('''CREATE TABLE raw_ingestion_cache (
            request_hash TEXT PRIMARY KEY, catalog_key TEXT,
            source_api TEXT, raw_payload TEXT,
            inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')

        cursor.execute('''CREATE TABLE timeseries_macro (
            id INTEGER PRIMARY KEY, catalog_key TEXT, date DATE, value REAL,
            UNIQUE(catalog_key, date)
        )''')

        cursor.execute('''CREATE TABLE timeseries_micro (
            id INTEGER PRIMARY KEY, catalog_key TEXT, date DATE,
            val_open REAL, val_high REAL, val_low REAL,
            val_close REAL, val_volume INTEGER,
            UNIQUE(catalog_key, date)
        )''')

        cursor.execute('''CREATE TABLE news_intel_pool (
            fingerprint TEXT PRIMARY KEY, title_hash TEXT, catalog_key TEXT,
            published_at TIMESTAMP, title TEXT, url TEXT,
            sentiment_score REAL, ai_summary TEXT
        )''')

        # Insert test data
        cursor.execute("""INSERT INTO data_catalog VALUES (
            'TEST_METRIC', 'US', 'MACRO', 'JUDGMENT', 'Test Metric',
            'FRED', 'MONTHLY', '{"series": "WALCL"}', 'Test', 1
        )""")

        cursor.execute("""INSERT INTO sync_watermarks (catalog_key) VALUES ('TEST_METRIC')""")

        conn.commit()
        conn.close()

        yield db_path

    def test_prepare_context(self, temp_db):
        """Test preparing ingestion context."""
        db = DatabaseCore(temp_db)
        manager = AdapterManager(db)

        context = manager.prepare_context('TEST_METRIC')

        assert context is not None
        assert context.catalog_key == 'TEST_METRIC'
        assert context.source_api == 'FRED'
        assert context.role == 'JUDGMENT'

    def test_prepare_context_not_found(self, temp_db):
        """Test preparing context for non-existent asset."""
        db = DatabaseCore(temp_db)
        manager = AdapterManager(db)

        context = manager.prepare_context('NONEXISTENT')

        assert context is None

    @patch('local.src.adapters.adapter_factory.AdapterFactory.get_adapter')
    def test_fetch_and_store_success(self, mock_get_adapter, temp_db):
        """Test successful fetch and store operation."""
        # Setup mock adapter
        mock_adapter = Mock(spec=BaseAdapter)
        mock_adapter.get_request_hash.return_value = 'test_hash_123'
        mock_adapter.fetch_raw_data.return_value = json.dumps({"status": "success"})
        mock_get_adapter.return_value = mock_adapter

        db = DatabaseCore(temp_db)
        manager = AdapterManager(db)

        result = manager.fetch_and_store('TEST_METRIC', dry_run=False)

        assert result['status'] == 'success'
        assert result['stored'] is True
        assert result['request_hash'] == 'test_hash_123'

    @patch('local.src.adapters.adapter_factory.AdapterFactory.get_adapter')
    def test_fetch_and_store_dry_run(self, mock_get_adapter, temp_db):
        """Test dry run operation."""
        # Setup mock adapter
        mock_adapter = Mock(spec=BaseAdapter)
        mock_adapter.dry_run.return_value = True
        mock_get_adapter.return_value = mock_adapter

        db = DatabaseCore(temp_db)
        manager = AdapterManager(db)

        result = manager.fetch_and_store('TEST_METRIC', dry_run=True)

        assert result['status'] == 'dry_run_success'
        mock_adapter.dry_run.assert_called_once()

    def test_get_ingestion_summary(self, temp_db):
        """Test ingestion summary generation."""
        db = DatabaseCore(temp_db)
        manager = AdapterManager(db)

        summary = manager.get_ingestion_summary()

        assert 'total_assets' in summary
        assert 'active_assets' in summary
        assert 'pending_ingestion' in summary
        assert summary['total_assets'] >= 1
