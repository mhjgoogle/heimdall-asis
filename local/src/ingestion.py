# filepath: local/src/ingestion.py

"""Main ingestion pipeline orchestrator."""

import hashlib
import json
import logging
import os
import sqlite3
import sys
from datetime import datetime
from typing import Dict, Any, List, Optional

# Add project root to path for imports
project_root = os.path.join(os.path.dirname(__file__), '..', '..')
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from local.config.config import AppConfig
from local.src.adapters.base import IngestionContext
from local.src.adapters.adapter_factory import create_adapter
from .database.db_operations import get_catalog_info, get_catalog_role, get_last_ingested_at, update_sync_watermarks_ingested

logger = logging.getLogger(__name__)


class IngestionEngine:
    """Engine for ingesting data from various sources with caching."""

    def __init__(self):
        self.db_path = AppConfig.DB_PATH

    def _get_request_hash(self, catalog_key: str, params: Dict[str, Any], frequency: str) -> str:
        """Generate hash for request deduplication with time window [ID-061]."""
        now = datetime.now()

        # Add time suffix based on frequency
        if frequency == 'HOURLY':
            time_suffix = now.strftime('%Y-%m-%d-%H')
        elif frequency == 'DAILY':
            time_suffix = now.strftime('%Y-%m-%d')
        elif frequency == 'MONTHLY':
            time_suffix = now.strftime('%Y-%m')
        else:
            time_suffix = now.strftime('%Y-%m-%d')  # Default to daily

        hash_input = f"{catalog_key}:{json.dumps(params, sort_keys=True)}:{time_suffix}"
        return hashlib.sha256(hash_input.encode()).hexdigest()

    def _is_cached(self, request_hash: str) -> bool:
        """Check if request is already cached."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT 1 FROM raw_ingestion_cache WHERE request_hash = ?",
                (request_hash,)
            )
            return cursor.fetchone() is not None

    def _cache_response(self, request_hash: str, catalog_key: str,
                        source_api: str, raw_payload: str):
        """Cache the raw response."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO raw_ingestion_cache
                (request_hash, catalog_key, source_api, raw_payload)
                VALUES (?, ?, ?, ?)
            """, (request_hash, catalog_key, source_api, raw_payload))
            conn.commit()

    def _get_catalog_info(self, catalog_key: str) -> tuple:
        """Get frequency for the catalog entry (last_bronze_ts now in sync_watermarks)."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT update_frequency FROM data_catalog
                WHERE catalog_key = ?
            """, (catalog_key,))
            result = cursor.fetchone()
            # Return frequency, and last_bronze_ts will be retrieved separately from sync_watermarks
            return (result[0] if result else None, None)

    def _get_catalog_role(self, catalog_key: str) -> str:
        """Get the role (JUDGMENT/VALIDATION) for the catalog entry."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT role FROM data_catalog
                WHERE catalog_key = ?
            """, (catalog_key,))
            result = cursor.fetchone()
            return result[0] if result else "JUDGMENT"  # Default to JUDGMENT

    def _get_last_ingested_at(self, catalog_key: str) -> Optional[datetime]:
        """Get the last ingested timestamp from sync_watermarks."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT last_ingested_at FROM sync_watermarks
                WHERE catalog_key = ?
            """, (catalog_key,))
            result = cursor.fetchone()

            if result and result[0]:
                # Parse ISO datetime string to datetime object (UTC)
                try:
                    from datetime import timezone
                    dt = datetime.fromisoformat(result[0])
                    # If the parsed datetime doesn't have timezone info, assume UTC
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    return dt
                except ValueError:
                    return None
            return None

    def _update_catalog_status(self, catalog_key: str, status: str,
                               error_msg: Optional[str] = None):
        """Update catalog entry status."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
        # Status tracking is now handled by sync_watermarks updates and logging
        if status == 'FAILED' and error_msg:
            logger.error(f"[{catalog_key}] Ingestion failed: {error_msg}")
        elif status == 'SUCCESS':
            logger.debug(f"[{catalog_key}] Ingestion successful")

    def _update_sync_watermarks(self, catalog_key: str):
        """Update sync_watermarks with current timestamp for last_ingested_at."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            # Use UTC time to avoid timezone confusion
            from datetime import timezone
            now = datetime.now(timezone.utc).isoformat()
            cursor.execute("""
                UPDATE sync_watermarks
                SET last_ingested_at = ?
                WHERE catalog_key = ?
            """, (now, catalog_key))
            conn.commit()

    def ingest_data(self, catalog_key: str, source_api: str,
                    config_params: Dict[str, Any]) -> tuple[bool, bool]:
        """
        Ingest data for a catalog entry.
        Returns (success, data_was_new) where data_was_new indicates if new data was written.
        """
        try:
            # Get frequency and last_bronze_ts
            frequency, _ = get_catalog_info(catalog_key)

            # Determine fetch mode
            last_ingested_at = get_last_ingested_at(catalog_key)
            if last_ingested_at is None:
                fetch_mode = "首次全量采集"
            else:
                fetch_mode = "日常差分补齐"

            logger.info(f"[{catalog_key}] {fetch_mode} - 开始处理")

            # Generate request hash
            request_hash = self._get_request_hash(catalog_key, config_params, frequency or 'DAILY')

            # Check cache - if already cached, no need to fetch
            if self._is_cached(request_hash):
                logger.debug(f"[{catalog_key}] 数据已缓存 (hash: {request_hash[:8]}...)")
                logger.debug(f"[{catalog_key}] Ingestion successful")
                return True, False  # Success but no new data

            # Create adapter using factory
            try:
                adapter = create_adapter(source_api)
            except ValueError as e:
                logger.warning(f"[{catalog_key}] 不支持的来源: {e}")
                raw_data = '{"error": "unsupported source"}'
                self._cache_response(request_hash, catalog_key, source_api, raw_data)
                self._update_catalog_status(catalog_key, 'SUCCESS')
                return True, True

                # Get role for context
                role = get_catalog_role(catalog_key)

                # Create ingestion context
                context = IngestionContext(
                    catalog_key=catalog_key,
                    source_api=source_api,
                    config_params=config_params,
                    role=role,
                    last_ingested_at=last_ingested_at,
                    frequency=frequency
                )

                # Fetch raw data using new adapter interface
                raw_data = adapter.fetch_raw_data(context)

            except ImportError as e:
                logger.error(f"[{catalog_key}] 适配器导入失败: {e}")
                raw_data = f'{{"error": "adapter import failed: {e}"}}'
            except Exception as e:
                logger.error(f"[{catalog_key}] 适配器初始化失败: {e}")
                raw_data = f'{{"error": "adapter initialization failed: {e}"}}'

            # Cache the response (new data)
            self._cache_response(request_hash, catalog_key, source_api, raw_data)
            self._update_catalog_status(catalog_key, 'SUCCESS')

            # Update sync_watermarks only when new data is written
            self._update_sync_watermarks(catalog_key)

            logger.info(f"[{catalog_key}] {fetch_mode} - 新数据缓存成功 (hash: {request_hash[:8]}...)")
            return True, True  # Success with new data

        except Exception as e:
            logger.error(f"[{catalog_key}] 采集失败: {e}", exc_info=True)
            self._update_catalog_status(catalog_key, 'FAILED', str(e))
            return False, False


def get_active_catalog_tasks() -> List[Dict[str, Any]]:
    """Query data_catalog for all active tasks with sync watermarks (is_active=1)."""
    db_path = AppConfig.DB_PATH
    tasks = []

    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT dc.catalog_key, dc.source_api, dc.config_params, dc.update_frequency,
                   sw.last_ingested_at, dc.role, dc.entity_name
            FROM data_catalog dc
            LEFT JOIN sync_watermarks sw ON dc.catalog_key = sw.catalog_key
            WHERE dc.is_active = 1
            ORDER BY dc.source_api, dc.catalog_key
        """)

        for row in cursor.fetchall():
            catalog_key, source_api, config_params_json, update_frequency, last_ingested_at, role, entity_name = row
            try:
                config_params = json.loads(config_params_json) if config_params_json else {}

                # Parse last_ingested_at (stored as UTC)
                last_ingested_dt = None
                if last_ingested_at:
                    try:
                        from datetime import timezone
                        dt = datetime.fromisoformat(last_ingested_at)
                        # Ensure it's treated as UTC
                        if dt.tzinfo is None:
                            last_ingested_dt = dt.replace(tzinfo=timezone.utc)
                        else:
                            last_ingested_dt = dt
                    except ValueError:
                        logger.warning(f"Invalid last_ingested_at for {catalog_key}: {last_ingested_at}")

                tasks.append({
                    'catalog_key': catalog_key,
                    'source_api': source_api,
                    'config_params': config_params,
                    'update_frequency': update_frequency,
                    'last_ingested_at': last_ingested_dt,
                    'role': role,
                    'entity_name': entity_name
                })
            except json.JSONDecodeError as e:
                logger.error(f"Invalid config_params for {catalog_key}: {e}")
                continue

    return tasks


def should_update_task(task: Dict[str, Any]) -> tuple[bool, str]:
    """
    Determine if a task should be updated based on delta check.
    Returns (should_update, reason)
    """
    catalog_key = task['catalog_key']
    source_api = task['source_api']
    update_frequency = task['update_frequency']
    last_ingested_at = task['last_ingested_at']

    # Use UTC time for consistency
    from datetime import timezone
    now = datetime.now(timezone.utc)

    # For RSS feeds, always attempt update (rely on hash deduplication)
    if source_api == 'RSS':
        return True, "RSS feeds always check for updates"

    # For time series data (FRED, yfinance), check based on frequency
    if last_ingested_at is None:
        return True, "首次采集"

    # Calculate time since last ingestion
    time_since_last = now - last_ingested_at

    # Check frequency requirements
    if update_frequency == 'HOURLY':
        should_update = time_since_last.total_seconds() >= 3600  # 1 hour
        reason = f"距离上次采集 {time_since_last.total_seconds()/3600:.1f} 小时"
    elif update_frequency == 'DAILY':
        should_update = time_since_last.total_seconds() >= 86400  # 24 hours
        reason = f"距离上次采集 {time_since_last.days} 天"
    elif update_frequency == 'MONTHLY':
        should_update = last_ingested_at.month != now.month or last_ingested_at.year != now.year
        reason = f"距离上次采集 {(now - last_ingested_at).days} 天"
    else:
        # Default to daily
        should_update = time_since_last.total_seconds() >= 86400
        reason = f"默认每日检查，距离上次采集 {time_since_last.days} 天"

    if not should_update:
        reason = f"跳过更新: {reason} (频率: {update_frequency})"

    return should_update, reason


def main():
    """Main ingestion pipeline execution with differential sync."""
    start_time = datetime.now()
    logger.info("Starting batch ingestion pipeline with differential sync")

    engine = IngestionEngine()
    tasks = get_active_catalog_tasks()

    if not tasks:
        logger.warning("No active tasks found in data_catalog")
        print("执行摘要: 无活跃任务")
        return

    total_tasks = len(tasks)
    processed_tasks = 0
    skipped_tasks = 0
    successful_tasks = 0
    failed_tasks = 0
    errors = []

    logger.info(f"Found {total_tasks} active tasks to evaluate")

    for i, task in enumerate(tasks, 1):
        catalog_key = task['catalog_key']
        source_api = task['source_api']
        entity_name = task.get('entity_name', catalog_key)

        # Delta check: determine if this task needs updating
        should_update, reason = should_update_task(task)

        if not should_update:
            skipped_tasks += 1
            logger.info(f"[{i}/{total_tasks}] {catalog_key} ({entity_name}) - SKIPPED: {reason}")
            continue

        processed_tasks += 1
        logger.info(f"[{i}/{total_tasks}] Processing {catalog_key} ({entity_name}) - {reason}")

        try:
            # Execute ingestion
            success, data_was_new = engine.ingest_data(catalog_key, source_api, task['config_params'])
            if success:
                successful_tasks += 1
                status_msg = "SUCCESS (新数据)" if data_was_new else "SUCCESS (缓存命中)"
                logger.info(f"[{processed_tasks}/{total_tasks}] {catalog_key} - {status_msg}")
            else:
                failed_tasks += 1
                errors.append(f"{catalog_key}: Ingestion failed")
                logger.error(f"[{processed_tasks}/{total_tasks}] {catalog_key} - FAILED")

        except Exception as e:
            failed_tasks += 1
            error_msg = f"{catalog_key}: {str(e)}"
            errors.append(error_msg)
            logger.error(f"[{processed_tasks}/{total_tasks}] {catalog_key} - ERROR: {e}", exc_info=True)

    end_time = datetime.now()
    duration = end_time - start_time

    # Print execution summary
    print("执行摘要 (批处理差分同步):")
    print(f"- 总任务数: {total_tasks}")
    print(f"- 跳过任务: {skipped_tasks}")
    print(f"- 处理任务: {processed_tasks}")
    print(f"- 成功任务: {successful_tasks}")
    print(f"- 失败任务: {failed_tasks}")
    print(f"- 执行时长: {duration}")
    print(f"- 开始时间: {start_time.isoformat()}")
    print(f"- 结束时间: {end_time.isoformat()}")

    if errors:
        print("\n错误详情:")
        for error in errors[:5]:  # Show first 5 errors
            print(f"- {error}")
        if len(errors) > 5:
            print(f"... 还有 {len(errors) - 5} 个错误")

    logger.info(f"Batch ingestion pipeline completed: {successful_tasks}/{processed_tasks} successful, {skipped_tasks} skipped")


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    main()