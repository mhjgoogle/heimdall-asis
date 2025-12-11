# src/adapters/output_handler.py
"""Output Handler adapter for ASIS system.

Handles writing output files (Parquet, CSV, etc.).
"""

import logging
from pathlib import Path
from typing import Optional
import pandas as pd
from datetime import datetime

from src.adapters.duckdb_storage import DuckDBStorage

logger = logging.getLogger(__name__)


class OutputHandler:
    """Handles output file operations."""

    def __init__(self, config=None):
        self.config = config
        self._duckdb_storage = None

    @property
    def duckdb_storage(self):
        """Lazy initialization of DuckDB storage."""
        if self._duckdb_storage is None and self.config and self.config.DUCKDB_ENABLE_STORAGE:
            read_only = getattr(self.config, 'DUCKDB_READ_ONLY', False)
            self._duckdb_storage = DuckDBStorage(self.config.DUCKDB_PATH, read_only=read_only, config=self.config)
        return self._duckdb_storage

    def save_meta_mapping(self, df: pd.DataFrame, prefix: str = "output", format: str = "parquet") -> Path:
        """Save meta mapping results.

        Args:
            df: DataFrame to save
            prefix: Filename prefix
            format: File format ('parquet' or 'csv')

        Returns:
            Path to saved file
        """
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        if format == "parquet":
            filename = f"{prefix}_{timestamp}.parquet"
            output_path = Path(self.config.data_paths.output_dir) / filename
            output_path.parent.mkdir(parents=True, exist_ok=True)
            df.to_parquet(output_path, index=False)
        else:
            filename = f"{prefix}_{timestamp}.csv"
            output_path = Path(self.config.data_paths.output_dir) / filename
            output_path.parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(output_path, index=False)

        logger.info(f"Saved {format} file: {output_path}")
        return output_path

    def save_meta_mapping_to_duckdb(self, df: pd.DataFrame, layer: str = "bronze") -> bool:
        """Save meta mapping data to DuckDB.

        Args:
            df: DataFrame to save
            layer: Data layer ('bronze', 'silver', 'gold')

        Returns:
            True if successful
        """
        if not self.duckdb_storage:
            logger.error("DuckDB storage not available")
            return False

        try:
            # Add timestamp columns based on layer
            df = df.copy()
            now = datetime.now()

            if layer == "bronze":
                if 'extracted_at' not in df.columns:
                    df['extracted_at'] = now
                if 'source' not in df.columns:
                    df['source'] = 'api'
                self.duckdb_storage.save_bronze_meta_mapping(df)

            elif layer == "silver":
                if 'processed_at' not in df.columns:
                    df['processed_at'] = now
                if 'quality_score' not in df.columns:
                    df['quality_score'] = 1.0  # Default quality score
                self.duckdb_storage.save_silver_meta_mapping(df)

            elif layer == "gold":
                if 'created_at' not in df.columns:
                    df['created_at'] = now
                if 'updated_at' not in df.columns:
                    df['updated_at'] = now
                if 'is_active' not in df.columns:
                    df['is_active'] = True
                self.duckdb_storage.save_gold_meta_mapping(df)

            else:
                logger.error(f"Unknown layer: {layer}")
                return False

            logger.info(f"Saved {len(df)} rows to DuckDB {layer} layer")
            return True

        except Exception as e:
            logger.error(f"Failed to save to DuckDB {layer} layer: {e}")
            return False

    def save_timeseries(self, df: pd.DataFrame, prefix: str = "timeseries", format: str = "parquet") -> Path:
        """Save time series results.

        Args:
            df: DataFrame to save
            prefix: Filename prefix
            format: File format ('parquet' or 'csv')

        Returns:
            Path to saved file
        """
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        if format == "parquet":
            filename = f"{prefix}_{timestamp}.parquet"
            output_path = Path(self.config.data_paths.output_dir) / filename
            output_path.parent.mkdir(parents=True, exist_ok=True)
            df.to_parquet(output_path, index=False)
        else:
            filename = f"{prefix}_{timestamp}.csv"
            output_path = Path(self.config.data_paths.output_dir) / filename
            output_path.parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(output_path, index=False)

        logger.info(f"Saved {format} file: {output_path}")
        return output_path

    def save_timeseries_to_duckdb(self, df: pd.DataFrame) -> bool:
        """Save time series data to DuckDB.

        Args:
            df: DataFrame to save

        Returns:
            True if successful
        """
        if not self.duckdb_storage:
            logger.error("DuckDB storage not available")
            return False

        try:
            self.duckdb_storage.save_timeseries_data(df)
            logger.info(f"Saved {len(df)} time series observations to DuckDB")
            return True

        except Exception as e:
            logger.error(f"Failed to save time series data to DuckDB: {e}")
            return False

    def optimize_duckdb(self):
        """Optimize DuckDB database (create indexes, etc.)."""
        if self.duckdb_storage:
            try:
                self.duckdb_storage.optimize_tables()
                logger.info("DuckDB optimization completed")
            except Exception as e:
                logger.error(f"DuckDB optimization failed: {e}")