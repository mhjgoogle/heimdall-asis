# filepath: scripts/check_schema.py
"""Thin wrapper script for database schema comparison tool.

Entry point that calls the core schema checking logic from local.src.database.
"""

import sys
import os

# Add project root to path
project_root = os.path.join(os.path.dirname(__file__), '..')
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from local.config import AppConfig
from local.src.database.schema_checker import (
    parse_table_definition,
    get_database_schema,
    compare_schemas,
    generate_migration_script
)
from local.src.database.init_db import parse_datamodel_doc


def main():
    """Main function."""
    print("🔍 Heimdall-Asis Schema Comparison Tool")
    print("=" * 50)

    # Get database path
    db_path = AppConfig.DB_PATH

    if not os.path.exists(db_path):
        print("❌ Database file not found. Please run init_db.py first.")
        return

    try:
        # Parse document schema
        print("📖 Parsing Phase4_DataModel.md...")
        doc_data = parse_datamodel_doc()

        # Extract table schemas from DDL
        doc_schema = {}
        for ddl in doc_data['ddl_statements']:
            table_name, columns = parse_table_definition(ddl)
            if table_name:
                doc_schema[table_name] = columns

        print(f"   Found {len(doc_schema)} tables in document")

        # Get database schema
        print("💾 Reading database schema...")
        db_schema = get_database_schema(db_path)
        print(f"   Found {len(db_schema)} tables in database")

        # Compare schemas
        print("⚖️  Comparing schemas...")
        suggestions = compare_schemas(doc_schema, db_schema)

        if not suggestions:
            print("✅ Schemas are synchronized!")
            return

        print(f"🔧 Found differences in {len(suggestions)} table(s)")

        # Generate migration script
        migration_script = generate_migration_script(suggestions)

        print("\n📝 Migration Script:")
        print("=" * 50)
        print(migration_script)

        # Save to file
        migration_file = os.path.join(project_root, 'scripts', 'migration.sql')
        with open(migration_file, 'w', encoding='utf-8') as f:
            f.write(migration_script)

        print(f"\n💾 Migration script saved to: {migration_file}")
        print("\n⚠️  IMPORTANT: Review the migration script carefully before executing!")
        print("   Run with: sqlite3 local/data/heimdall.db < scripts/migration.sql")

    except Exception as e:
        print(f"❌ Error during schema comparison: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()