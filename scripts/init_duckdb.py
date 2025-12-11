#!/usr/bin/env python3
"""Initialize DuckDB database for ASIS system."""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from src.adapters.duckdb_storage import DuckDBStorage
from src.adapters.input_handler import InputHandler
from src.adapters.config import Config

def main():
    """Initialize DuckDB database."""
    config = Config()

    print(f"Initializing DuckDB database at: {config.DUCKDB_PATH}")

    # Create DuckDB storage instance
    storage = DuckDBStorage(config.DUCKDB_PATH)

    # Initialize database (this will create tables on first access)
    with storage:
        print("Database initialized successfully!")

        # Check table info using InputHandler
        input_handler = InputHandler(config=config)
        info = input_handler.get_duckdb_table_info()
        if info:
            print("Existing tables:")
            for table, table_info in info.items():
                print(f"  - {table}: {table_info['row_count']} rows")
        else:
            print("No tables exist yet - they will be created when data is first saved")

    print(f"DuckDB database ready at: {config.DUCKDB_PATH}")

if __name__ == "__main__":
    main()