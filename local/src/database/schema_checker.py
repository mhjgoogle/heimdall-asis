# filepath: local/src/database/schema_checker.py

"""Schema comparison and validation tool."""

import re
import sqlite3
from typing import Dict, Tuple
from local.src.database.init_db import parse_datamodel_doc


def parse_table_definition(ddl_statement: str) -> Tuple[str, Dict[str, str]]:
    """Parse a CREATE TABLE DDL statement and extract table name and columns.

    Args:
        ddl_statement: Raw DDL statement

    Returns:
        Tuple of (table_name, {column_name: column_definition})
    """
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


def get_database_schema(db_path: str) -> Dict[str, Dict[str, str]]:
    """Get actual database schema from SQLite database.

    Args:
        db_path: Path to SQLite database

    Returns:
        {table_name: {column_name: column_definition}}
    """
    schema = {}

    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()

        # Get all tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()

        for (table_name,) in tables:
            # Skip system tables
            if table_name.startswith('sqlite_'):
                continue

            # Get column info
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()

            schema[table_name] = {}
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

                schema[table_name][name] = col_def

    return schema


def compare_schemas(doc_schema: Dict[str, Dict[str, str]],
                   db_schema: Dict[str, Dict[str, str]]) -> Dict[str, list]:
    """Compare document schema against database schema.

    Args:
        doc_schema: Schema from Phase4_DataModel.md
        db_schema: Actual database schema

    Returns:
        {table_name: [list of ALTER TABLE suggestions]}
    """
    suggestions = {}

    for table_name, doc_columns in doc_schema.items():
        if table_name not in db_schema:
            suggestions[table_name] = [f"-- Table '{table_name}' does not exist in database"]
            continue

        db_columns = db_schema[table_name]
        table_suggestions = []

        # Find missing columns
        for col_name, col_def in doc_columns.items():
            if col_name not in db_columns:
                # Generate ALTER TABLE statement
                alter_sql = f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_def};"
                table_suggestions.append(alter_sql)

        # Check for column definition differences (basic check)
        for col_name in set(doc_columns.keys()) & set(db_columns.keys()):
            doc_def = doc_columns[col_name].upper()
            db_def = db_columns[col_name].upper()

            # Simple comparison - could be enhanced for more sophisticated diffing
            if doc_def != db_def:
                table_suggestions.append(
                    f"-- Column '{col_name}' definition differs:"
                )
                table_suggestions.append(
                    f"--   Document: {doc_columns[col_name]}"
                )
                table_suggestions.append(
                    f"--   Database: {db_columns[col_name]}"
                )

        if table_suggestions:
            suggestions[table_name] = table_suggestions

    return suggestions


def generate_migration_script(suggestions: Dict[str, list]) -> str:
    """Generate a complete migration script.

    Args:
        suggestions: Output from compare_schemas

    Returns:
        Complete SQL migration script
    """
    if not suggestions:
        return "-- No schema changes needed. Database is up to date!"

    script_lines = [
        "-- =======================================================",
        "-- Heimdall-Asis Database Schema Migration Script",
        "-- Generated by schema_checker.py",
        "-- =======================================================",
        "",
        "BEGIN TRANSACTION;",
        ""
    ]

    for table_name, alters in suggestions.items():
        script_lines.append(f"-- Table: {table_name}")
        script_lines.extend(alters)
        script_lines.append("")

    script_lines.extend([
        "COMMIT;",
        "",
        "-- End of migration script"
    ])

    return "\n".join(script_lines)
