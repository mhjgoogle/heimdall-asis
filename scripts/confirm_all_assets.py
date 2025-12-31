#!/usr/bin/env python3
"""Asset activation with connectivity and non-empty validation (步骤三).

Verifies all inactive assets (is_active=0) by:
1. Testing HTTP connectivity via adapter dry_run
2. Checking result set is non-empty
3. Activating only if both conditions pass
"""

import sys
import os
import logging
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from local.src.database.database_core import DatabaseCore
from local.src.adapters.adapter_manager import AdapterManager
from local.config import AppConfig

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger(__name__)


def confirm_assets():
    """Confirm all inactive assets and activate successful ones."""
    logger.info("=" * 80)
    logger.info("ASSET ACTIVATION START (步骤三)")
    logger.info("=" * 80)

    db = DatabaseCore(AppConfig.DB_PATH)
    manager = AdapterManager(db)

    # Get all inactive assets
    inactive_entries = db.catalog.get_inactive()
    catalog_keys = [entry[0] for entry in inactive_entries]

    if not catalog_keys:
        logger.info("No inactive assets to confirm.")
        return 0

    logger.info(f"Found {len(catalog_keys)} inactive assets to verify")

    activated = 0
    failed = 0
    failed_assets = []

    for catalog_key in catalog_keys:
        try:
            logger.info(f"Testing connectivity: {catalog_key}")
            
            # Test via dry_run: checks HTTP 200 + non-empty result
            result = manager.ingest_asset(catalog_key, dry_run=True)
            
            if result['status'] == 'dry_run_passed':
                # Activation criteria met: connectivity + non-empty
                db.catalog.set_active(catalog_key, is_active=True)
                logger.info(f"✓ Activated: {catalog_key}")
                activated += 1
            else:
                logger.warning(f"✗ Verification failed: {catalog_key} (status: {result['status']})")
                if result.get('error'):
                    logger.warning(f"  Reason: {result['error']}")
                failed += 1
                failed_assets.append(catalog_key)

        except Exception as e:
            logger.error(f"✗ Error testing {catalog_key}: {e}")
            failed += 1
            failed_assets.append(catalog_key)

    # Summary
    logger.info("=" * 80)
    logger.info("ACTIVATION SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Total Tested: {len(catalog_keys)}")
    logger.info(f"Activated: {activated}")
    logger.info(f"Failed: {failed}")

    if failed_assets:
        logger.warning("Failed assets (requires manual investigation):")
        for asset in failed_assets:
            logger.warning(f"  - {asset}")

    logger.info("=" * 80)

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(confirm_assets())
