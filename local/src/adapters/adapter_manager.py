# filepath: local/src/adapters/adapter_manager.py

import logging
import json
import hashlib
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime, timezone
from pathlib import Path

from .adapter_factory import create_adapter
from .base import IngestionContext, StandardMetric
from local.src.database import DatabaseCore
from local.config.config import AppConfig

logger = logging.getLogger(__name__)


class AdapterManager:
    """Manages adapters for data ingestion with structured logging and DB integration."""

    def __init__(self, db_core: Optional[DatabaseCore] = None):
        self.db_core = db_core or DatabaseCore(AppConfig.DB_PATH)
        self.ingestion_stats = {
            'total_processed': 0,
            'successful': 0,
            'failed': 0,
            'skipped': 0,
            'errors': []
        }

    def ingest_asset(self, catalog_key: str, dry_run: bool = False) -> Dict[str, Any]:
        """Ingest a single asset by catalog_key with full error handling.

        Args:
            catalog_key: The catalog key to ingest
            dry_run: If True, only test connectivity without storing

        Returns:
            Dictionary with ingestion results
        """
        result = {
            'catalog_key': catalog_key,
            'status': 'pending',
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'raw_payload': None,
            'error': None,
            'request_hash': None,
            'stored': False
        }

        try:
            # Get catalog entry
            catalog_entry = self.db_core.catalog.get_by_key(catalog_key)
            if not catalog_entry:
                result['status'] = 'failed'
                result['error'] = f"Catalog key not found: {catalog_key}"
                self.ingestion_stats['failed'] += 1
                logger.error(f"[{catalog_key}] Catalog entry not found")
                return result

            # Ensure watermark entry exists
            self.db_core.watermarks.ensure_entry(catalog_key)

            # Build ingestion context
            config_params = json.loads(catalog_entry['config_params']) if isinstance(catalog_entry['config_params'], str) else catalog_entry['config_params']
            
            # Add search_keywords from catalog if present
            # Handle both dict and sqlite3.Row objects
            search_keywords = None
            if hasattr(catalog_entry, 'get'):
                search_keywords = catalog_entry.get('search_keywords')
            else:
                # sqlite3.Row - use index-based access
                try:
                    search_keywords = catalog_entry[8]  # search_keywords is column 8
                except (IndexError, TypeError):
                    pass
            
            if search_keywords:
                # Parse comma-separated keywords into list
                config_params['keywords'] = [k.strip() for k in search_keywords.split(',') if k.strip()]
            
            last_ingested = self.db_core.watermarks.get_last_ingested(catalog_key)

            context = IngestionContext(
                catalog_key=catalog_key,
                source_api=catalog_entry['source_api'],
                config_params=config_params,
                role=catalog_entry['role'],
                last_ingested_at=last_ingested,
                frequency=catalog_entry['update_frequency']
            )

            # Create adapter
            adapter = create_adapter(context.source_api)

            # Validate configuration
            if not adapter.validate_config(context.config_params):
                result['status'] = 'failed'
                result['error'] = f"Invalid config for {context.source_api}: {context.config_params}"
                self.ingestion_stats['failed'] += 1
                logger.error(f"[{catalog_key}] Invalid adapter config")
                return result

            # Dry run test
            if dry_run:
                can_fetch = adapter.dry_run(context)
                result['status'] = 'dry_run_passed' if can_fetch else 'dry_run_failed'
                if not can_fetch:
                    self.ingestion_stats['skipped'] += 1
                    logger.info(f"[{catalog_key}] Dry run failed - no data available")
                else:
                    self.ingestion_stats['successful'] += 1
                    logger.info(f"[{catalog_key}] Dry run passed")
                return result

            # Fetch raw data
            raw_payload = adapter.fetch_raw_data(context)
            request_hash = adapter.get_request_hash(context)

            result['raw_payload'] = raw_payload
            result['request_hash'] = request_hash

            # Check if already ingested (idempotency)
            if self.db_core.raw_ingestion.exists(request_hash):
                result['status'] = 'skipped'
                result['stored'] = False
                self.ingestion_stats['skipped'] += 1
                logger.info(f"[{catalog_key}] Already ingested (idempotent skip)")
                return result

            # Store raw payload in Bronze layer
            self.db_core.raw_ingestion.insert_or_ignore(
                request_hash,
                catalog_key,
                context.source_api,
                raw_payload
            )

            # Update watermark
            self.db_core.watermarks.update_ingested(catalog_key)

            result['status'] = 'success'
            result['stored'] = True
            self.ingestion_stats['successful'] += 1
            logger.info(f"[{catalog_key}] Successfully ingested and stored")

        except Exception as e:
            result['status'] = 'failed'
            result['error'] = str(e)
            self.ingestion_stats['failed'] += 1
            self.ingestion_stats['errors'].append({
                'catalog_key': catalog_key,
                'error': str(e),
                'timestamp': datetime.now(timezone.utc).isoformat()
            })
            logger.error(f"[{catalog_key}] Ingestion failed: {e}", exc_info=True)

        self.ingestion_stats['total_processed'] += 1
        return result

    def ingest_batch(self, catalog_keys: Optional[List[str]] = None, dry_run: bool = False, limit: Optional[int] = None) -> Dict[str, Any]:
        """Ingest multiple assets with batch orchestration.

        Args:
            catalog_keys: List of catalog keys to ingest. If None, ingest all active.
            dry_run: If True, only test connectivity
            limit: Maximum number of assets to process

        Returns:
            Batch ingestion summary
        """
        batch_start = datetime.now(timezone.utc)

        # Determine which assets to process
        if catalog_keys is None:
            active_entries = self.db_core.catalog.get_active()
            catalog_keys = [entry[0] for entry in active_entries]

        if limit:
            catalog_keys = catalog_keys[:limit]

        results = []
        for catalog_key in catalog_keys:
            result = self.ingest_asset(catalog_key, dry_run=dry_run)
            results.append(result)

        batch_end = datetime.now(timezone.utc)
        duration = (batch_end - batch_start).total_seconds()

        summary = {
            'batch_start': batch_start.isoformat(),
            'batch_end': batch_end.isoformat(),
            'duration_seconds': duration,
            'dry_run': dry_run,
            'results': results,
            'statistics': {
                'total_processed': self.ingestion_stats['total_processed'],
                'successful': self.ingestion_stats['successful'],
                'failed': self.ingestion_stats['failed'],
                'skipped': self.ingestion_stats['skipped'],
                'error_count': len(self.ingestion_stats['errors'])
            },
            'errors': self.ingestion_stats['errors']
        }

        logger.info(f"Batch ingestion complete: {self.ingestion_stats['successful']}/{self.ingestion_stats['total_processed']} successful")
        return summary

    def get_statistics(self) -> Dict[str, Any]:
        """Get current ingestion statistics."""
        return {
            'total_processed': self.ingestion_stats['total_processed'],
            'successful': self.ingestion_stats['successful'],
            'failed': self.ingestion_stats['failed'],
            'skipped': self.ingestion_stats['skipped'],
            'error_count': len(self.ingestion_stats['errors']),
            'errors': self.ingestion_stats['errors']
        }

    def reset_statistics(self):
        """Reset ingestion statistics."""
        self.ingestion_stats = {
            'total_processed': 0,
            'successful': 0,
            'failed': 0,
            'skipped': 0,
            'errors': []
        }
