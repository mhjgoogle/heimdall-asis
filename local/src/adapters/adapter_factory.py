# filepath: local/src/adapters/adapter_factory.py

import logging
from typing import Optional, Type, Dict, Any
from datetime import datetime
from pathlib import Path
import sys
import os

project_root = os.path.join(os.path.dirname(__file__), '..', '..')
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from .base import BaseAdapter, IngestionContext
from .fred import FredAdapter
from .yfinance_adapter import YFinanceAdapter
from .rss_adapter import RSSAdapter
from .newsapi_adapter import NewsAPIAdapter
from local.src.database import DatabaseCore
from local.config import AppConfig

logger = logging.getLogger(__name__)


class AdapterFactory:
    """Factory for creating and managing data source adapters with database integration."""

    _adapters: Dict[str, Type[BaseAdapter]] = {
        'FRED': FredAdapter,
        'yfinance': YFinanceAdapter,
        'RSS': RSSAdapter,
        'NewsAPI': NewsAPIAdapter,
    }

    @classmethod
    def register_adapter(cls, source_name: str, adapter_class: Type[BaseAdapter]):
        """Register a new adapter class."""
        cls._adapters[source_name] = adapter_class
        logger.info(f"Registered adapter: {source_name}")

    @classmethod
    def get_adapter(cls, source_name: str) -> BaseAdapter:
        """Get an adapter instance by source name."""
        if source_name not in cls._adapters:
            raise ValueError(f"Unknown adapter: {source_name}")
        adapter_class = cls._adapters[source_name]
        return adapter_class()

    @classmethod
    def list_adapters(cls) -> list:
        """List all registered adapters."""
        return list(cls._adapters.keys())


# Backward compatibility: expose factory function at module level
def create_adapter(source_api: str) -> BaseAdapter:
    """Factory function to create adapter instances based on source_api.
    
    Convenience wrapper around AdapterFactory.get_adapter for backward compatibility.
    """
    return AdapterFactory.get_adapter(source_api)


class AdapterManager:
    """Manages adapter execution with database integration and incremental ingestion."""

    def __init__(self, db_core: Optional[DatabaseCore] = None):
        self.db = db_core or DatabaseCore(AppConfig.DB_PATH)
        self.factory = AdapterFactory()

    def prepare_context(self, catalog_key: str) -> Optional[IngestionContext]:
        """Prepare ingestion context from catalog and watermarks."""
        catalog_entry = self.db.catalog.get_by_key(catalog_key)
        if not catalog_entry:
            logger.warning(f"Catalog entry not found: {catalog_key}")
            return None

        watermark = self.db.watermarks.get(catalog_key)

        context = IngestionContext(
            catalog_key=catalog_key,
            source_api=catalog_entry['source_api'],
            config_params=catalog_entry['config_params'],
            role=catalog_entry['role'],
            frequency=catalog_entry['update_frequency'],
            last_ingested_at=datetime.fromisoformat(watermark['last_ingested_at']) if watermark and watermark['last_ingested_at'] else None
        )

        return context

    def fetch_and_store(self, catalog_key: str, dry_run: bool = False) -> Dict[str, Any]:
        """Fetch data from source and store in database (Bronze Layer)."""
        result = {
            'catalog_key': catalog_key,
            'status': 'pending',
            'message': '',
            'request_hash': None,
            'stored': False,
        }

        try:
            # Prepare context
            context = self.prepare_context(catalog_key)
            if not context:
                result['status'] = 'error'
                result['message'] = 'Failed to prepare ingestion context'
                return result

            # Get adapter
            adapter = self.factory.get_adapter(context.source_api)
            logger.info(f"[{catalog_key}] Using adapter: {context.source_api}")

            # Perform dry run first
            if dry_run:
                success = adapter.dry_run(context)
                result['status'] = 'dry_run_success' if success else 'dry_run_failed'
                result['message'] = f"Dry run {'passed' if success else 'failed'}"
                return result

            # Generate request hash for deduplication
            request_hash = adapter.get_request_hash(context)
            result['request_hash'] = request_hash

            # Check if this request already exists
            if self.db.raw_ingestion.exists(request_hash):
                result['status'] = 'skipped'
                result['message'] = 'Duplicate request (same config and time window)'
                return result

            # Fetch raw data
            logger.info(f"[{catalog_key}] Fetching data from {context.source_api}...")
            raw_payload = adapter.fetch_raw_data(context)

            # Store in Bronze Layer
            self.db.raw_ingestion.insert_or_ignore(
                request_hash=request_hash,
                catalog_key=catalog_key,
                source_api=context.source_api,
                raw_payload=raw_payload
            )

            # Update watermark
            self.db.watermarks.update_ingested(catalog_key)

            result['status'] = 'success'
            result['stored'] = True
            result['message'] = f'Successfully ingested {len(raw_payload)} bytes'
            logger.info(f"[{catalog_key}] Data stored successfully")

        except Exception as e:
            logger.error(f"[{catalog_key}] Error: {e}", exc_info=True)
            result['status'] = 'error'
            result['message'] = str(e)

        return result

    def ingest_asset(self, catalog_key: str) -> Dict[str, Any]:
        """Ingest a single asset (public wrapper for fetch_and_store)."""
        return self.fetch_and_store(catalog_key, dry_run=False)

    def dry_run_asset(self, catalog_key: str) -> Dict[str, Any]:
        """Perform dry run for a single asset."""
        return self.fetch_and_store(catalog_key, dry_run=True)

    def batch_ingest(self, catalog_keys: list, skip_errors: bool = True) -> Dict[str, Any]:
        """Ingest multiple assets and return summary."""
        results = []
        success_count = 0
        error_count = 0

        for catalog_key in catalog_keys:
            try:
                result = self.ingest_asset(catalog_key)
                results.append(result)
                if result['status'] == 'success':
                    success_count += 1
                else:
                    error_count += 1
            except Exception as e:
                logger.error(f"[{catalog_key}] Batch ingest error: {e}")
                error_count += 1
                if not skip_errors:
                    raise

        return {
            'total': len(catalog_keys),
            'success': success_count,
            'error': error_count,
            'results': results
        }

    def get_ingestion_summary(self) -> Dict[str, Any]:
        """Get summary of ingestion status for all assets."""
        summary = {
            'total_assets': 0,
            'active_assets': 0,
            'pending_ingestion': 0,
            'recently_ingested': 0,
        }

        active_entries = self.db.catalog.get_active()
        summary['total_assets'] = len(active_entries)

        for entry in active_entries:
            catalog_key = entry[0]
            watermark = self.db.watermarks.get(catalog_key)

            if watermark and watermark['last_ingested_at']:
                summary['recently_ingested'] += 1
            else:
                summary['pending_ingestion'] += 1

        summary['active_assets'] = len(active_entries)
        return summary

    def close(self):
        """Close database connection."""
        self.db.close()
