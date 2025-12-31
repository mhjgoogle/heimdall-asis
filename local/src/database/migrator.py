# filepath: local/src/database/migrator.py

"""Database migration tool."""

import json
import re
import sqlite3
from datetime import datetime
from typing import Dict, Any, Optional, Tuple
from dataclasses import dataclass

from local.src.database.init_db import parse_datamodel_doc


@dataclass
class MigrationStep:
    """Represents a single migration step."""
    operation: str  # 'add_column', 'rename_column', 'change_type', 'drop_column'
    table: str
    details: Dict[str, Any]
    description: str

    def to_sql(self) -> list:
        """Convert migration step to SQL statements."""
        if self.operation == 'add_column':
            col_name = self.details['column']
            col_def = self.details['definition']
            return [f"ALTER TABLE {self.table} ADD COLUMN {col_name} {col_def};"]

        elif self.operation == 'rename_column':
            # SQLite doesn't support direct column rename, need to recreate table
            old_name = self.details['old_name']
            new_name = self.details['new_name']
            # This will be handled by the table recreation logic
            return []

        elif self.operation == 'change_type':
            col_name = self.details['column']
            new_type = self.details['new_type']
            # This will be handled by the table recreation logic
            return []

        elif self.operation == 'drop_column':
            col_name = self.details['column']
            # This will be handled by the table recreation logic
            return []

        return []


class DatabaseMigrator:
    """Handles complex database schema migrations."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.migration_history_table = '_migrations'

    def get_current_schema(self) -> Dict[str, Dict[str, str]]:
        """Get current database schema."""
        schema = {}

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            # Get all tables
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
            tables = [row[0] for row in cursor.fetchall()]

            for table in tables:
                if table == self.migration_history_table:
                    continue

                # Get column info
                cursor.execute(f"PRAGMA table_info({table})")
                columns = cursor.fetchall()

                schema[table] = {}
                for col in columns:
                    cid, name, type_, notnull, dflt_value, pk = col
                    # Reconstruct column definition
                    col_def = type_
                    if pk:
                        col_def += " PRIMARY KEY"
                    if notnull:
                        col_def += " NOT NULL"
                    if dflt_value is not None:
                        col_def += f" DEFAULT {dflt_value}"

                    schema[table][name] = col_def

        return schema

    def parse_target_schema(self) -> Dict[str, Dict[str, str]]:
        """Parse target schema from Phase4_DataModel.md."""
        doc_data = parse_datamodel_doc()

        # Extract table schemas from DDL
        target_schema = {}
        for ddl in doc_data['ddl_statements']:
            table_name, columns = self._parse_table_definition(ddl)
            if table_name:
                target_schema[table_name] = columns

        return target_schema

    def _parse_table_definition(self, ddl_statement: str) -> Tuple[str, Dict[str, str]]:
        """Parse a CREATE TABLE DDL statement."""
        # Extract table name
        table_match = re.search(r'CREATE TABLE(?: IF NOT EXISTS)? (\w+)', ddl_statement, re.IGNORECASE)
        if not table_match:
            return None, {}

        table_name = table_match.group(1)

        # Extract column definitions
        columns = {}

        # Split by commas and clean up
        lines = [line.strip() for line in ddl_statement.split('\n') if line.strip()]

        # Find the column definition section
        in_columns = False
        for line in lines:
            if line.startswith('('):
                in_columns = True
                continue
            elif line.startswith(')'):
                break
            elif in_columns:
                # Parse column definition: column_name TYPE [constraints]
                # Remove comments and clean up
                line = re.sub(r'--.*$', '', line).strip()
                if not line or line.startswith(','):
                    continue

                # Match column definitions like: column_name TEXT PRIMARY KEY,
                col_match = re.match(r'(\w+)\s+(.+?)(?:,|$)', line)
                if col_match:
                    col_name = col_match.group(1)
                    col_def = col_match.group(2).strip()

                    # Remove trailing comma and clean up
                    col_def = re.sub(r',\s*$', '', col_def).strip()

                    columns[col_name] = col_def

        return table_name, columns

    def calculate_migrations(self, current_schema: Dict[str, Dict[str, str]],
                           target_schema: Dict[str, Dict[str, str]]) -> list:
        """Calculate required migrations to go from current to target schema."""
        migrations = []

        # Check each target table
        for table_name, target_columns in target_schema.items():
            if table_name not in current_schema:
                # New table - this should be handled by init_db.py
                continue

            current_columns = current_schema[table_name]

            # Find columns to add
            for col_name, col_def in target_columns.items():
                if col_name not in current_columns:
                    migrations.append(MigrationStep(
                        operation='add_column',
                        table=table_name,
                        details={'column': col_name, 'definition': col_def},
                        description=f"Add column {col_name} to {table_name}"
                    ))

            # Find columns to remove (less common, but supported)
            for col_name in current_columns:
                if col_name not in target_columns:
                    migrations.append(MigrationStep(
                        operation='drop_column',
                        table=table_name,
                        details={'column': col_name},
                        description=f"Drop column {col_name} from {table_name}"
                    ))

            # Check for type changes (simplified - just compare definitions)
            for col_name in set(current_columns.keys()) & set(target_columns.keys()):
                if current_columns[col_name] != target_columns[col_name]:
                    migrations.append(MigrationStep(
                        operation='change_type',
                        table=table_name,
                        details={
                            'column': col_name,
                            'old_type': current_columns[col_name],
                            'new_type': target_columns[col_name]
                        },
                        description=f"Change type of {col_name} in {table_name} from {current_columns[col_name]} to {target_columns[col_name]}"
                    ))

        return migrations

    def generate_migration_sql(self, migrations: list) -> str:
        """Generate complete migration SQL script."""
        sql_lines = [
            "-- =======================================================",
            f"-- Heimdall-Asis Database Migration - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "-- =======================================================",
            "",
            "BEGIN TRANSACTION;",
            "",
            "-- Migration steps:"
        ]

        for i, migration in enumerate(migrations, 1):
            sql_lines.append(f"-- {i}. {migration.description}")

            if migration.operation == 'add_column':
                # Simple ALTER TABLE for adding columns
                col_name = migration.details['column']
                col_def = migration.details['definition']
                sql_lines.append(f"ALTER TABLE {migration.table} ADD COLUMN {col_name} {col_def};")

            elif migration.operation in ['rename_column', 'change_type', 'drop_column']:
                # Complex operations require table recreation
                sql_lines.extend(self._generate_table_recreation_sql(migration.table, migration.operation, migration.details))

            sql_lines.append("")

        sql_lines.extend([
            "COMMIT;",
            "",
            "-- Migration completed successfully",
            f"-- Total steps: {len(migrations)}"
        ])

        return "\n".join(sql_lines)

    def _generate_table_recreation_sql(self, table_name: str, operation: str, details: Dict[str, Any]) -> list:
        """Generate SQL for complex operations that require table recreation."""
        sql_lines = []

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            # Get current table structure
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()

            if not columns:
                return [f"-- Error: Table {table_name} not found"]

            # Build column list for recreation
            col_defs = []
            for col in columns:
                cid, name, type_, notnull, dflt_value, pk = col

                # Skip dropped columns
                if operation == 'drop_column' and name == details['column']:
                    continue

                # Handle renamed columns
                if operation == 'rename_column' and name == details['old_name']:
                    name = details['new_name']

                col_def = f"{name} {type_}"
                if pk:
                    col_def += " PRIMARY KEY"
                if notnull:
                    col_def += " NOT NULL"
                if dflt_value is not None:
                    col_def += f" DEFAULT {dflt_value}"

                col_defs.append(col_def)

            # Generate recreation SQL
            temp_table = f"{table_name}_temp_migration"

            sql_lines.extend([
                f"-- Recreating table {table_name} for complex migration",
                f"CREATE TABLE {temp_table} (",
                ",\n".join(f"    {col_def}" for col_def in col_defs),
                ");",
                "",
                f"INSERT INTO {temp_table} SELECT {', '.join(col.split()[0] for col in col_defs)} FROM {table_name};",
                "",
                f"DROP TABLE {table_name};",
                "",
                f"ALTER TABLE {temp_table} RENAME TO {table_name};",
                ""
            ])

        return sql_lines

    def apply_migrations(self, migrations: list, dry_run: bool = True) -> bool:
        """Apply migrations to the database."""
        if not migrations:
            print("✅ No migrations needed")
            return True

        print(f"📋 Found {len(migrations)} migration(s) to apply")

        if dry_run:
            print("🔍 DRY RUN MODE - No changes will be made to database")
            print("Run with --apply to execute migrations")

        # Generate migration script
        migration_sql = self.generate_migration_sql(migrations)

        if dry_run:
            print("\n📝 Migration SQL Preview:")
            print("=" * 50)
            print(migration_sql)
            return True

        # Apply migrations
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.executescript(migration_sql)

                # Record migration in history
                self._record_migration(migrations, cursor)
                conn.commit()

            print("✅ Migrations applied successfully")
            return True

        except Exception as e:
            print(f"❌ Migration failed: {e}")
            return False

    def _record_migration(self, migrations: list, cursor):
        """Record applied migrations in history table."""
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {self.migration_history_table} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            description TEXT,
            migration_data TEXT
        )
        """)

        # Record this migration batch
        migration_data = json.dumps([{
            'operation': m.operation,
            'table': m.table,
            'details': m.details,
            'description': m.description
        } for m in migrations])

        cursor.execute(f"""
        INSERT INTO {self.migration_history_table} (description, migration_data)
        VALUES (?, ?)
        """, (f"Schema migration: {len(migrations)} steps", migration_data))
