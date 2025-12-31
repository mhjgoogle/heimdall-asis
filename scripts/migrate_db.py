# filepath: scripts/migrate_db.py
"""Thin wrapper script for database migration tool.

Entry point that calls the core migration logic from local.src.database.
"""

import sys
import os
import argparse

# Add project root to path
project_root = os.path.join(os.path.dirname(__file__), '..')
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from local.config import AppConfig
from local.src.database.migrator import DatabaseMigrator


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description='Heimdall-Asis Database Migration Tool')
    parser.add_argument('--db-path', default=AppConfig.DB_PATH,
                       help='Path to SQLite database')
    parser.add_argument('--apply', action='store_true',
                       help='Actually apply migrations (default is dry run)')
    parser.add_argument('--force', action='store_true',
                       help='Force migration even if risky operations are detected')

    args = parser.parse_args()

    if not os.path.exists(args.db_path):
        print(f"❌ Database file not found: {args.db_path}")
        print("Run init_db.py first to create the database")
        return

    print("🔄 Heimdall-Asis Database Migration Tool")
    print("=" * 50)

    migrator = DatabaseMigrator(args.db_path)

    try:
        # Get schemas
        print("📖 Reading current database schema...")
        current_schema = migrator.get_current_schema()
        print(f"   Found {len(current_schema)} tables")

        print("🎯 Reading target schema from Phase4_DataModel.md...")
        target_schema = migrator.parse_target_schema()
        print(f"   Found {len(target_schema)} tables in document")

        # Calculate migrations
        print("⚖️  Calculating required migrations...")
        migrations = migrator.calculate_migrations(current_schema, target_schema)

        if not migrations:
            print("✅ Schemas are already synchronized")
            return

        print(f"🔧 Found {len(migrations)} migration(s) needed")

        # Check for risky operations
        risky_ops = [m for m in migrations if m.operation in ['drop_column', 'rename_column']]
        if risky_ops and not args.force:
            print("\n⚠️  WARNING: Risky operations detected:")
            for op in risky_ops:
                print(f"   • {op.description}")
            print("\nUse --force to proceed with risky operations")
            print("⚠️  Data loss may occur with DROP COLUMN operations!")

            # Show preview anyway
            args.apply = False

        # Apply migrations
        success = migrator.apply_migrations(migrations, dry_run=not args.apply)

        if success and args.apply:
            print("\n🎉 Migration completed successfully!")
            print(f"Applied {len(migrations)} migration(s)")
        elif success and not args.apply:
            print("\n🔍 Dry run completed - no changes made")
            print("Use --apply to execute the migrations")

    except Exception as e:
        print(f"❌ Migration process failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()