# filepath: local/src/pipeline/batch_orchestrator.py

"""Batch orchestration engine for daily data ingestion."""

import json
import logging
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Any

from local.src.pipeline.ingestion import IngestionEngine, get_active_catalog_tasks, should_update_task
from local.config import AppConfig


logger = logging.getLogger(__name__)


class BatchOrchestrator:
    """Orchestrates batch data ingestion with delta checking."""

    def __init__(self):
        self.engine = IngestionEngine()
        self.db_path = AppConfig.DB_PATH

    def run_batch(self, 
                  tasks: List[Dict[str, Any]],
                  dry_run: bool = False,
                  force: bool = False) -> Dict[str, Any]:
        """Run batch ingestion with statistics.

        Args:
            tasks: List of catalog task dictionaries
            dry_run: Preview mode (no database writes)
            force: Override frequency limits

        Returns:
            Dictionary with execution statistics
        """
        total = len(tasks)
        skipped = 0
        processed = 0
        successful = 0
        failed = 0
        task_details = []

        logger.info(f"Starting batch with {total} tasks (dry_run={dry_run}, force={force})")

        for i, task in enumerate(tasks, 1):
            catalog_key = task['catalog_key']
            source_api = task['source_api']
            entity_name = task.get('entity_name', catalog_key)

            # Delta check
            should_update, reason = should_update_task(task)

            if force:
                # Override delta check
                should_update = True
                reason = "Forced by --force flag"

            if should_update:
                processed += 1
                logger.info(f"[{i}/{total}] Processing {catalog_key} ({reason})")

                if dry_run:
                    logger.debug(f"[DRY-RUN] Would process: {catalog_key}")
                    task_details.append({
                        'catalog_key': catalog_key,
                        'source_api': source_api,
                        'status': 'DRY-RUN',
                        'reason': reason
                    })
                    successful += 1
                else:
                    try:
                        success, data_was_new = self.engine.ingest_data(
                            catalog_key=catalog_key,
                            source_api=source_api,
                            config_params=task['config_params']
                        )

                        if success:
                            successful += 1
                            status_msg = "NEW DATA" if data_was_new else "CACHED"
                            logger.info(f"[SUCCESS] {catalog_key} ({status_msg})")
                            task_details.append({
                                'catalog_key': catalog_key,
                                'source_api': source_api,
                                'status': 'SUCCESS',
                                'data_was_new': data_was_new
                            })
                        else:
                            failed += 1
                            logger.error(f"[FAILED] {catalog_key}")
                            task_details.append({
                                'catalog_key': catalog_key,
                                'source_api': source_api,
                                'status': 'FAILED'
                            })

                    except Exception as e:
                        failed += 1
                        logger.error(f"[ERROR] {catalog_key}: {e}", exc_info=True)
                        task_details.append({
                            'catalog_key': catalog_key,
                            'source_api': source_api,
                            'status': 'ERROR',
                            'error': str(e)
                        })
            else:
                skipped += 1
                logger.debug(f"[{i}/{total}] Skipping {catalog_key} ({reason})")
                task_details.append({
                    'catalog_key': catalog_key,
                    'source_api': source_api,
                    'status': 'SKIPPED',
                    'reason': reason
                })

        # Compile statistics
        stats = {
            'total': total,
            'skipped': skipped,
            'processed': processed,
            'successful': successful,
            'failed': failed,
            'dry_run': dry_run,
            'forced': force,
            'timestamp': datetime.now().isoformat(),
            'task_details': task_details
        }

        logger.info(f"Batch completed: {successful} successful, {failed} failed, {skipped} skipped")

        return stats

    def get_summary_lines(self, stats: Dict[str, Any]) -> List[str]:
        """Format execution statistics as display lines.

        Args:
            stats: Dictionary from run_batch()

        Returns:
            List of formatted output lines
        """
        lines = [
            "\n" + "="*80,
            "EXECUTION SUMMARY",
            "="*80,
            f"Total tasks:      {stats['total']}",
            f"Skipped:          {stats['skipped']} (frequency limit or no watermark change)",
            f"Processed:        {stats['processed']}",
        ]

        if stats['dry_run']:
            lines.append(f"  └─ Dry-run mode: {stats['successful']} tasks would be processed")
        else:
            lines.append(f"  ├─ Successful:   {stats['successful']}")
            lines.append(f"  └─ Failed:       {stats['failed']}")

        lines.extend([
            f"Mode:             {'DRY-RUN' if stats['dry_run'] else 'ACTUAL'}",
            f"Timestamp:        {stats['timestamp']}",
            "="*80 + "\n"
        ])

        return lines

    def get_task_summary_lines(self, stats: Dict[str, Any]) -> List[str]:
        """Format per-task summary as display lines.

        Args:
            stats: Dictionary from run_batch()

        Returns:
            List of formatted output lines
        """
        lines = [
            "\n" + "="*80,
            "BATCH INGESTION: DELTA CHECK SUMMARY",
            "="*80
        ]

        total = stats['total']
        for i, detail in enumerate(stats['task_details'], 1):
            catalog_key = detail['catalog_key']
            source_api = detail['source_api']
            status = detail['status']
            reason = detail.get('reason', '')

            status_short = "PROCESS" if status in ['SUCCESS', 'DRY-RUN'] else status
            marker = "→" if status in ['SUCCESS', 'DRY-RUN'] else "◊"

            reason_str = f" ({reason})" if reason else ""
            print(f"{marker} [{i:3d}/{total}] {status_short:7s} {catalog_key:20s} ({source_api:10s}){reason_str}")

        lines.append("="*80)
        return lines
