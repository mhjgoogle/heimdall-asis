# src/adapters/input_handler.py
"""Input Handler adapter for ASIS system.

Handles reading input files (CSV, Parquet, etc.).
"""

import logging
from pathlib import Path
from typing import List, Optional, Union
import pandas as pd
import dask.dataframe as dd

from src.adapters.duckdb_storage import DuckDBStorage

logger = logging.getLogger(__name__)


class InputHandler:
    """Handles input file operations."""

    def __init__(self, config=None):
        self.config = config
        self._duckdb_storage = None

    @property
    def duckdb_storage(self):
        """Lazy initialization of DuckDB storage."""
        if self._duckdb_storage is None and self.config and self.config.DUCKDB_ENABLE_STORAGE:
            read_only = getattr(self.config, 'DUCKDB_READ_ONLY', True)  # InputHandler always read-only
            self._duckdb_storage = DuckDBStorage(self.config.DUCKDB_PATH, read_only=read_only, config=self.config)
        return self._duckdb_storage

    def read_csv(self, file_path: Path) -> Optional[pd.DataFrame]:
        """Read CSV file.

        Args:
            file_path: Path to CSV file

        Returns:
            DataFrame or None on error
        """
        try:
            return pd.read_csv(file_path)
        except Exception as e:
            logger.error(f"Failed to read CSV {file_path}: {e}")
            return None

    def read_meta_mapping_input_directory(self, use_dask: bool = False) -> List[Union[pd.DataFrame, dd.DataFrame]]:
        """Read all files from meta mapping input directory.

        Args:
            use_dask: Whether to use Dask for large files

        Returns:
            List of DataFrames
        """
        if not self.config:
            return []

        input_dir = Path(self.config.META_MAPPING_INPUT)
        if not input_dir.exists():
            return []

        dfs = []
        for file_path in input_dir.glob("*.parquet"):
            try:
                if use_dask:
                    df = dd.read_parquet(file_path)
                else:
                    df = pd.read_parquet(file_path)
                dfs.append(df)
            except Exception as e:
                logger.error(f"Failed to read {file_path}: {e}")

        return dfs

    def load_meta_mapping_from_duckdb(self, layer: str = "gold", active_only: bool = True) -> Optional[pd.DataFrame]:
        """Load meta mapping data from DuckDB.

        Args:
            layer: Data layer to load from ('bronze', 'silver', 'gold')
            active_only: For gold layer, load only active records

        Returns:
            DataFrame with loaded data, or None on error
        """
        if not self.duckdb_storage:
            logger.error("DuckDB storage not available")
            return None

        try:
            if layer == "bronze":
                df = self.duckdb_storage.load_bronze_meta_mapping()
            elif layer == "silver":
                df = self.duckdb_storage.load_silver_meta_mapping()
            elif layer == "gold":
                df = self.duckdb_storage.load_gold_meta_mapping(active_only=active_only)
            else:
                logger.error(f"Unknown layer: {layer}")
                return None

            logger.info(f"Loaded {len(df)} rows from DuckDB {layer} layer")
            return df

        except Exception as e:
            logger.error(f"Failed to load from DuckDB {layer} layer: {e}")
            return None

    def get_duckdb_table_info(self) -> dict:
        """Get information about DuckDB tables.

        Returns:
            Dictionary with table information
        """
        if not self.duckdb_storage:
            return {}

        info = {}
        layers = ['bronze_meta_mapping', 'silver_meta_mapping', 'gold_meta_mapping']

        for table in layers:
            if self.duckdb_storage.table_exists(table):
                row_count = self.duckdb_storage.get_table_row_count(table)
                schema = self.duckdb_storage.get_table_schema(table)
                info[table] = {
                    'row_count': row_count,
                    'schema': schema
                }
            else:
                info[table] = {
                    'row_count': 0,
                    'schema': None
                }

        return info