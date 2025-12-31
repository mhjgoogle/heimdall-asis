g# filepath: local/src/confirmation.py

"""Confirm and activate assets by testing data availability."""

import sys
import os
import json
from datetime import datetime

# Add project root to path
project_root = os.path.join(os.path.dirname(__file__), '..', '..')
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from local.src.adapters.base import IngestionContext
from local.src.adapters.adapter_factory import create_adapter
from .database.db_operations import get_inactive_catalog_entries, ensure_sync_watermarks_entry, activate_asset

def test_asset_dry_run(catalog_key, source_api, config_params_str, role, entity_name):
    """Test dry run for a single asset."""
    try:
        # Parse config_params
        config_params = json.loads(config_params_str)

        print(f"  Testing {catalog_key} ({entity_name}) - {source_api}")

        # Create adapter
        adapter = create_adapter(source_api)

        # Create context (no last_ingested_at for dry run)
        context = IngestionContext(
            catalog_key=catalog_key,
            source_api=source_api,
            config_params=config_params,
            role=role,
            frequency='DAILY'  # Default
        )

        # Dry run
        start_time = datetime.now()
        success = adapter.dry_run(context)
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        if success:
            print(f"  SUCCESS in {duration:.1f}s")
            # Ensure sync_watermarks entry and activate
            ensure_sync_watermarks_entry(catalog_key)
            activate_asset(catalog_key)
            return True
        else:
            print(f"  FAILED in {duration:.1f}s")
            return False

    except Exception as e:
        error_msg = str(e)
        print(f"  ERROR: {error_msg[:100]}{'...' if len(error_msg) > 100 else ''}")
        return False

def main():
    """Main function to confirm all inactive assets."""
    print("🚀 Starting asset confirmation and activation...")
    print(f"Current time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Get inactive entries
    inactive_entries = get_inactive_catalog_entries()
    print(f"Found {len(inactive_entries)} inactive catalog entries")

    if not inactive_entries:
        print("No inactive assets to confirm. All assets are already active.")
        return

    successful = 0
    failed = 0

    for catalog_key, source_api, config_params_str, role, entity_name in inactive_entries:
        if test_asset_dry_run(catalog_key, source_api, config_params_str, role, entity_name):
            successful += 1
        else:
            failed += 1

    print("\n" + "="*50)
    print("CONFIRMATION SUMMARY")
    print("="*50)
    print(f"Total inactive assets tested: {len(inactive_entries)}")
    print(f"Successfully activated: {successful}")
    print(f"Failed to activate: {failed}")
    print(f"Success rate: {(successful / len(inactive_entries) * 100):.1f}%" if inactive_entries else "0%")
    print("="*50)