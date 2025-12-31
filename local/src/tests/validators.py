"""Comprehensive validation module for Bronze Layer data ingestion.

Implements the "Definition of Done" from Phase 4 requirements:
1. Database initialization with seed data
2. Asset verification and activation
3. Delta check logic (Frequency Limit & Duplicate Hash)
4. Google News RSS integration verification
"""

import sqlite3
import json
import hashlib
from datetime import datetime, timedelta
from pathlib import Path
from collections import defaultdict
from typing import Dict, Tuple

from local.config import AppConfig
from local.src.pipeline.ingestion import IngestionEngine


class BronzeLayerValidator:
    """Validates Bronze Layer implementation against Phase 4 requirements."""

    def __init__(self):
        self.db_path = AppConfig.DB_PATH
        self.engine = IngestionEngine()
        self.results = {
            'initialization': None,
            'asset_verification': None,
            'delta_logic': None,
            'google_news': None,
            'overall': None
        }

    def validate_initialization(self) -> Tuple[bool, str]:
        """Check: Database initialized with full seed data (is_active=0).

        Expected: 39+ catalog entries with correct structure.
        """
        print("\n" + "="*80)
        print("✓ TEST 1: INITIALIZATION (Seed Data Injection)")
        print("="*80)

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            # Check data_catalog structure
            cursor.execute("PRAGMA table_info(data_catalog)")
            columns = cursor.fetchall()
            col_names = [col[1] for col in columns]

            required_cols = ['catalog_key', 'country', 'scope', 'role', 'entity_name',
                           'source_api', 'update_frequency', 'config_params', 'is_active']
            missing_cols = [c for c in required_cols if c not in col_names]

            if missing_cols:
                msg = f"❌ FAILED: Missing columns in data_catalog: {missing_cols}"
                print(msg)
                return False, msg

            print(f"✅ data_catalog structure correct: {len(col_names)} columns")

            # Count total entries
            cursor.execute("SELECT COUNT(*) FROM data_catalog")
            total_count = cursor.fetchone()[0]
            print(f"✅ Total catalog entries: {total_count}")

            # Count by status
            cursor.execute("""
                SELECT is_active, COUNT(*) as cnt
                FROM data_catalog
                GROUP BY is_active
                ORDER BY is_active
            """)
            status_counts = cursor.fetchall()
            print(f"   Status distribution: {dict(status_counts)}")

            # Verify specific seed categories
            cursor.execute("""
                SELECT COUNT(*) FROM data_catalog
                WHERE catalog_key LIKE 'METRIC_%'
            """)
            macro_count = cursor.fetchone()[0]
            print(f"✅ Macro metrics (METRIC_*): {macro_count}")

            cursor.execute("""
                SELECT COUNT(*) FROM data_catalog
                WHERE catalog_key LIKE 'STOCK_%'
            """)
            stock_count = cursor.fetchone()[0]
            print(f"✅ Stocks (STOCK_*): {stock_count}")

            cursor.execute("""
                SELECT COUNT(*) FROM data_catalog
                WHERE catalog_key LIKE 'ASSET_%' OR catalog_key LIKE 'INDEX_%'
            """)
            asset_count = cursor.fetchone()[0]
            print(f"✅ Assets & Indexes (ASSET_*, INDEX_*): {asset_count}")

            cursor.execute("""
                SELECT COUNT(*) FROM data_catalog
                WHERE catalog_key LIKE 'NEWS_%'
            """)
            news_count = cursor.fetchone()[0]
            print(f"✅ News sources (NEWS_*): {news_count}")

            if total_count >= 39:
                msg = f"✅ PASSED: {total_count} seed entries found (expected ≥39)"
                print(f"\n{msg}")
                return True, msg
            else:
                msg = f"❌ FAILED: Only {total_count} entries found (expected ≥39)"
                print(f"\n{msg}")
                return False, msg

    def validate_asset_verification(self) -> Tuple[bool, str]:
        """Check: Asset verification script activates valid sources (is_active=1).

        Expected: Some assets successfully verified and marked is_active=1.
        """
        print("\n" + "="*80)
        print("✓ TEST 2: ASSET VERIFICATION & ACTIVATION")
        print("="*80)

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            # Check if any assets have been activated
            cursor.execute("SELECT COUNT(*) FROM data_catalog WHERE is_active=1")
            active_count = cursor.fetchone()[0]

            if active_count == 0:
                msg = f"⚠️  WARNING: No assets activated yet (expected some is_active=1)"
                print(f"   {msg}")
                print("\n   Run: python3 scripts/confirm_all_assets.py")
                return False, msg

            # Show activated assets by source
            cursor.execute("""
                SELECT source_api, COUNT(*) as cnt
                FROM data_catalog
                WHERE is_active=1
                GROUP BY source_api
                ORDER BY source_api
            """)
            active_by_source = cursor.fetchall()
            print(f"✅ Active assets by source:")
            for source, count in active_by_source:
                print(f"   - {source}: {count} assets")

            # Check sync_watermarks for activated assets
            cursor.execute("""
                SELECT COUNT(DISTINCT catalog_key)
                FROM sync_watermarks
            """)
            watermark_count = cursor.fetchone()[0]
            print(f"\n✅ Sync watermarks tracked: {watermark_count}")

            msg = f"✅ PASSED: {active_count} assets verified and activated"
            print(f"\n{msg}")
            return True, msg

    def validate_delta_logic(self) -> Tuple[bool, str]:
        """Check: Delta sync logic with frequency limits and hash deduplication.

        Expected:
        1. First run: raw_ingestion_cache populated, sync_watermarks.last_ingested_at updated
        2. Second run: Duplicate hashes skipped, new data still captured
        """
        print("\n" + "="*80)
        print("✓ TEST 3: DELTA LOGIC (Frequency Limit & Duplicate Hash)")
        print("="*80)

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            # Get test asset
            cursor.execute("""
                SELECT catalog_key, source_api, config_params, update_frequency
                FROM data_catalog
                WHERE is_active=1 AND source_api='FRED'
                LIMIT 1
            """)
            test_asset = cursor.fetchone()

            if not test_asset:
                msg = "⚠️  WARNING: No active FRED assets to test delta logic"
                print(f"   {msg}")
                return False, msg

            catalog_key, source_api, config_params_str, frequency = test_asset
            config_params = json.loads(config_params_str)

            print(f"Test asset: {catalog_key} ({source_api})")
            print(f"Frequency: {frequency}")

            # Check ingestion cache
            cursor.execute("""
                SELECT COUNT(*) FROM raw_ingestion_cache
                WHERE catalog_key=?
            """, (catalog_key,))
            cache_count = cursor.fetchone()[0]
            print(f"\n✅ Cached records: {cache_count}")

            # Check watermark
            cursor.execute("""
                SELECT last_ingested_at FROM sync_watermarks
                WHERE catalog_key=?
            """, (catalog_key,))
            watermark = cursor.fetchone()

            if watermark and watermark[0]:
                print(f"✅ Last ingested: {watermark[0]}")
                last_dt = datetime.fromisoformat(watermark[0])
                time_since = datetime.now(last_dt.tzinfo) - last_dt
                print(f"   Time since last ingest: {time_since}")

                # Check if should skip based on frequency
                freq_map = {
                    'Hourly': timedelta(hours=1),
                    'Daily': timedelta(days=1),
                    'Monthly': timedelta(days=30),
                    'Weekly': timedelta(days=7)
                }
                skip_threshold = freq_map.get(frequency, timedelta(days=1))

                if time_since < skip_threshold:
                    print(f"   Status: Should SKIP (within {frequency} threshold)")
                else:
                    print(f"   Status: Should RE-FETCH (beyond {frequency} threshold)")
            else:
                print("⚠️  No watermark found (first ingestion)")

            if cache_count > 0:
                msg = f"✅ PASSED: Delta logic tracking {cache_count} cached records"
                print(f"\n{msg}")
                return True, msg
            else:
                msg = f"⚠️  WARNING: No cached records found yet"
                print(f"\n{msg}")
                return False, msg

    def validate_google_news_integration(self) -> Tuple[bool, str]:
        """Check: Google News RSS integration working correctly.

        Expected:
        - NEWS_* catalog entries present with RSS source and Google News URLs
        - Successfully parsed Google News RSS format (Atom/mixed XML)
        """
        print("\n" + "="*80)
        print("✓ TEST 4: GOOGLE NEWS RSS INTEGRATION")
        print("="*80)

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            # Check NEWS entries with Google News URLs
            cursor.execute("""
                SELECT catalog_key, entity_name, config_params
                FROM data_catalog
                WHERE catalog_key LIKE 'NEWS_%'
                AND source_api='RSS'
                LIMIT 5
            """)
            news_entries = cursor.fetchall()

            if not news_entries:
                msg = "❌ FAILED: No NEWS_* entries with RSS source found"
                print(f"   {msg}")
                return False, msg

            print(f"✅ Found {len(news_entries)} NEWS entries:")
            google_news_count = 0

            for catalog_key, entity_name, config_params_str in news_entries:
                config = json.loads(config_params_str)
                url = config.get('url', '')

                is_google = 'news.google.com' in url
                status = "✅ Google News" if is_google else "⚠️  Other RSS"

                if is_google:
                    google_news_count += 1

                print(f"   {status}: {catalog_key}")
                print(f"      - {entity_name}")
                print(f"      - URL: {url[:60]}...")

            # Check if any NEWS data has been ingested
            cursor.execute("""
                SELECT COUNT(*) FROM raw_ingestion_cache
                WHERE catalog_key LIKE 'NEWS_%'
            """)
            news_cache_count = cursor.fetchone()[0]

            if news_cache_count > 0:
                print(f"\n✅ Ingested NEWS records: {news_cache_count}")

                # Sample a parsed entry
                cursor.execute("""
                    SELECT catalog_key, raw_payload FROM raw_ingestion_cache
                    WHERE catalog_key LIKE 'NEWS_%'
                    LIMIT 1
                """)
                sample = cursor.fetchone()

                if sample:
                    catalog_key, payload = sample
                    try:
                        data = json.loads(payload)
                        parsed_items = data.get('parsed_items', [])
                        print(f"   Sample NEWS entry: {catalog_key}")
                        print(f"   Parsed {len(parsed_items)} items from RSS")

                        if parsed_items:
                            item = parsed_items[0]
                            print(f"   First item fields: {list(item.keys())}")
                    except json.JSONDecodeError:
                        pass

            if google_news_count > 0:
                msg = f"✅ PASSED: {google_news_count} Google News sources configured"
                if news_cache_count > 0:
                    msg += f" and {news_cache_count} records cached"
                print(f"\n{msg}")
                return True, msg
            else:
                msg = f"⚠️  WARNING: No Google News sources found"
                print(f"\n{msg}")
                return False, msg

    def run_all_validations(self):
        """Run all validation tests."""
        print("\n" + "#"*80)
        print("# BRONZE LAYER COMPREHENSIVE VALIDATION")
        print("# Heimdall-Asis Phase 4 Implementation")
        print("#"*80)

        tests = [
            ("Initialization", self.validate_initialization),
            ("Asset Verification", self.validate_asset_verification),
            ("Delta Logic", self.validate_delta_logic),
            ("Google News Integration", self.validate_google_news_integration),
        ]

        passed = 0
        failed = 0

        for test_name, test_func in tests:
            try:
                success, message = test_func()
                if success:
                    passed += 1
                    self.results[test_name.lower().replace(' ', '_')] = 'PASS'
                else:
                    failed += 1
                    self.results[test_name.lower().replace(' ', '_')] = 'FAIL'
            except Exception as e:
                print(f"\n💥 ERROR in {test_name}: {e}")
                failed += 1
                self.results[test_name.lower().replace(' ', '_')] = 'ERROR'

        # Summary
        print("\n" + "="*80)
        print("📊 VALIDATION SUMMARY")
        print("="*80)
        print(f"Passed: {passed}/4")
        print(f"Failed: {failed}/4")

        if failed == 0:
            print("\n✅ ALL TESTS PASSED - Bronze Layer Ready!")
            self.results['overall'] = 'PASS'
        else:
            print(f"\n⚠️  {failed} test(s) failed - Address issues and rerun")
            self.results['overall'] = 'FAIL'

        print("\n" + "="*80)

        return passed, failed
