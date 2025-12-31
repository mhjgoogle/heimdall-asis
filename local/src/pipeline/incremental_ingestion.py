# filepath: local/src/pipeline/incremental_ingestion.py

"""Incremental ingestion engine with structured logging and watermark management."""

import logging
import json
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime, timezone
from pathlib import Path

from local.src.database import DatabaseCore
from local.src.adapters.adapter_manager import AdapterManager
from local.config import AppConfig

logger = logging.getLogger(__name__)


class IncrementalIngestionEngine:
    """Engine for incremental data ingestion with structured logging."""

    def __init__(self, db_core: Optional[DatabaseCore] = None):
        """Initialize incremental ingestion engine.

        Args:
            db_core: DatabaseCore instance (defaults to local SQLite)
        """
        self.db_core = db_core or DatabaseCore(AppConfig.DB_PATH)
        self.adapter_manager = AdapterManager(self.db_core)
        self.run_start_time: Optional[datetime] = None
        self.run_end_time: Optional[datetime] = None

    def ingest_by_asset_key(self, catalog_key: str, dry_run: bool = False) -> Dict[str, Any]:
        """Ingest a single asset using incremental logic.

        Args:
            catalog_key: Asset catalog key
            dry_run: If True, test connectivity only

        Returns:
            Ingestion result with metadata
        """
        asset_start = datetime.now(timezone.utc)

        result = {
            'catalog_key': catalog_key,
            'status': 'pending',
            'timestamp': asset_start.isoformat(),
            'duration_seconds': 0,
            'request_hash': None,
            'stored': False,
            'error': None,
            'last_ingested_at': None,
            'incremental_start_date': None
        }

        try:
            # Get catalog and watermark info
            catalog_entry = self.db_core.catalog.get_by_key(catalog_key)
            if not catalog_entry:
                result['status'] = 'failed'
                result['error'] = 'Catalog key not found'
                logger.error(f"[{catalog_key}] Catalog entry not found")
                return result

            # Get last ingested timestamp for incremental fetching
            last_ingested = self.db_core.watermarks.get_last_ingested(catalog_key)
            result['last_ingested_at'] = last_ingested.isoformat() if last_ingested else None

            # Ingest using AdapterManager
            ingest_result = self.adapter_manager.ingest_asset(catalog_key, dry_run=dry_run)

            # Merge results
            result.update(ingest_result)
            result['status'] = ingest_result['status']

            if ingest_result['status'] == 'success':
                logger.info(f"[{catalog_key}] Successfully ingested (incremental)")
            elif ingest_result['status'] == 'skipped':
                logger.debug(f"[{catalog_key}] Skipped (idempotent)")
            elif ingest_result['status'] == 'dry_run_passed':
                logger.info(f"[{catalog_key}] Dry-run passed")
            else:
                logger.warning(f"[{catalog_key}] {ingest_result['status']}")

        except Exception as e:
            result['status'] = 'failed'
            result['error'] = str(e)
            logger.error(f"[{catalog_key}] Ingestion failed: {e}", exc_info=True)

        finally:
            asset_end = datetime.now(timezone.utc)
            result['duration_seconds'] = (asset_end - asset_start).total_seconds()

        return result

    def ingest_by_scope(self, scope: str, role: Optional[str] = None, dry_run: bool = False, limit: Optional[int] = None) -> Dict[str, Any]:
        """Ingest assets filtered by scope and/or role.

        Args:
            scope: Data scope ('MACRO' or 'MICRO')
            role: Optional role filter ('JUDGMENT' or 'VALIDATION')
            dry_run: If True, test connectivity only
            limit: Maximum number of assets to process

        Returns:
            Batch ingestion summary
        """
        batch_start = datetime.now(timezone.utc)

        # Get active assets matching filters
        active_entries = self.db_core.catalog.get_active()
        catalog_keys = [
            entry[0] for entry in active_entries
            if entry  # Ensure not empty
        ]

        # Filter by scope and role using database
        filtered_keys = []
        for key in catalog_keys:
            entry = self.db_core.catalog.get_by_key(key)
            if entry and entry['scope'] == scope:
                if role is None or entry['role'] == role:
                    filtered_keys.append(key)

        if limit:
            filtered_keys = filtered_keys[:limit]

        logger.info(f"Ingesting {len(filtered_keys)} assets with scope={scope}, role={role}")

        # Ingest each asset
        results = []
        for catalog_key in filtered_keys:
            result = self.ingest_by_asset_key(catalog_key, dry_run=dry_run)
            results.append(result)

        batch_end = datetime.now(timezone.utc)
        duration = (batch_end - batch_start).total_seconds()

        # Aggregate statistics
        stats = self._calculate_statistics(results)

        summary = {
            'batch_start': batch_start.isoformat(),
            'batch_end': batch_end.isoformat(),
            'duration_seconds': duration,
            'filters': {
                'scope': scope,
                'role': role,
                'dry_run': dry_run
            },
            'results': results,
            'statistics': stats
        }

        logger.info(f"Batch complete: {stats['successful']}/{stats['total']} successful in {duration:.2f}s")
        return summary

    def ingest_by_role(self, role: str, dry_run: bool = False, limit: Optional[int] = None) -> Dict[str, Any]:
        """Ingest assets by role (JUDGMENT or VALIDATION).

        Args:
            role: Asset role ('JUDGMENT', 'VALIDATION', 'J', or 'V')
            dry_run: If True, test connectivity only
            limit: Maximum number of assets to process

        Returns:
            Batch ingestion summary
        """
        # Normalize role to short form
        role_map = {'JUDGMENT': 'J', 'VALIDATION': 'V', 'J': 'J', 'V': 'V'}
        normalized_role = role_map.get(role, role)

        batch_start = datetime.now(timezone.utc)

        # Get assets with specified role
        active_entries = self.db_core.catalog.get_active()
        filtered_keys = []

        for entry in active_entries:
            catalog_key = entry[0]
            entry_data = self.db_core.catalog.get_by_key(catalog_key)
            if entry_data and entry_data['role'] == normalized_role:
                filtered_keys.append(catalog_key)

        if limit:
            filtered_keys = filtered_keys[:limit]

        logger.info(f"Ingesting {len(filtered_keys)} assets with role={normalized_role}")

        results = []
        for catalog_key in filtered_keys:
            result = self.ingest_by_asset_key(catalog_key, dry_run=dry_run)
            results.append(result)

        batch_end = datetime.now(timezone.utc)
        duration = (batch_end - batch_start).total_seconds()

        stats = self._calculate_statistics(results)

        summary = {
            'batch_start': batch_start.isoformat(),
            'batch_end': batch_end.isoformat(),
            'duration_seconds': duration,
            'role': role,
            'dry_run': dry_run,
            'results': results,
            'statistics': stats
        }

        logger.info(f"Role-based batch complete: {stats['successful']}/{stats['total']} successful")
        return summary

    def ingest_all_active(self, dry_run: bool = False, limit: Optional[int] = None) -> Dict[str, Any]:
        """Ingest all active assets.

        Args:
            dry_run: If True, test connectivity only
            limit: Maximum number of assets to process

        Returns:
            Batch ingestion summary
        """
        batch_start = datetime.now(timezone.utc)

        active_entries = self.db_core.catalog.get_active()
        catalog_keys = [entry[0] for entry in active_entries]

        if limit:
            catalog_keys = catalog_keys[:limit]

        logger.info(f"Starting incremental ingestion for {len(catalog_keys)} assets")

        results = []
        for catalog_key in catalog_keys:
            result = self.ingest_by_asset_key(catalog_key, dry_run=dry_run)
            results.append(result)

        batch_end = datetime.now(timezone.utc)
        duration = (batch_end - batch_start).total_seconds()

        stats = self._calculate_statistics(results)

        summary = {
            'batch_start': batch_start.isoformat(),
            'batch_end': batch_end.isoformat(),
            'duration_seconds': duration,
            'dry_run': dry_run,
            'results': results,
            'statistics': stats
        }

        logger.info(f"Full ingestion complete: {stats['successful']}/{stats['total']} successful in {duration:.2f}s")
        self.run_start_time = batch_start
        self.run_end_time = batch_end

        return summary

    def _calculate_statistics(self, results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Calculate aggregate statistics from results.

        Args:
            results: List of ingestion results

        Returns:
            Statistics dictionary
        """
        total = len(results)
        successful = sum(1 for r in results if r['status'] == 'success' or r['status'] == 'dry_run_passed')
        failed = sum(1 for r in results if r['status'] == 'failed')
        skipped = sum(1 for r in results if r['status'] == 'skipped' or r['status'] == 'dry_run_failed')

        total_duration = sum(r.get('duration_seconds', 0) for r in results)

        return {
            'total': total,
            'successful': successful,
            'failed': failed,
            'skipped': skipped,
            'success_rate': (successful / total * 100) if total > 0 else 0,
            'total_duration_seconds': total_duration,
            'avg_duration_seconds': total_duration / total if total > 0 else 0
        }

    def get_watermark_status(self, catalog_key: str) -> Optional[Dict[str, Any]]:
        """Get watermark status for an asset.

        Args:
            catalog_key: Asset catalog key

        Returns:
            Watermark status dictionary or None if not found
        """
        watermark = self.db_core.watermarks.get(catalog_key)
        if not watermark:
            return None

        return {
            'catalog_key': catalog_key,
            'last_ingested_at': watermark['last_ingested_at'],
            'last_cleaned_at': watermark['last_cleaned_at'],
            'last_synced_at': watermark['last_synced_at'],
            'last_meta_synced_at': watermark['last_meta_synced_at'],
            'checksum': watermark['checksum']
        }

    def print_summary_report(self, summary: Dict[str, Any]):
        """Print structured summary report.

        Args:
            summary: Ingestion summary dictionary
        """
        logger.info("=" * 80)
        logger.info("INGESTION SUMMARY REPORT")
        logger.info("=" * 80)

        if 'batch_start' in summary:
            logger.info(f"Batch Duration: {summary['duration_seconds']:.2f} seconds")

        stats = summary.get('statistics', {})
        logger.info(f"Total Processed: {stats.get('total', 0)}")
        logger.info(f"Successful: {stats.get('successful', 0)}")
        logger.info(f"Failed: {stats.get('failed', 0)}")
        logger.info(f"Skipped: {stats.get('skipped', 0)}")
        logger.info(f"Success Rate: {stats.get('success_rate', 0):.1f}%")
        logger.info("=" * 80)
