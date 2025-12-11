# src/adapters/duckdb_storage.py
"""DuckDB Storage adapter for ASIS system.

Handles DuckDB database operations for Bronze/Silver/Gold data layers.
"""

import logging
from pathlib import Path
from typing import Optional, Dict, Any, List
import duckdb
import pandas as pd

logger = logging.getLogger(__name__)


class DuckDBStorage:
    """Handles DuckDB database operations for data storage layers.

    Manages Bronze (raw), Silver (interim), and Gold (processed) data layers.
    """

    def __init__(self, db_path: Path, read_only: bool = False, config=None):
        """Initialize DuckDB storage.

        Args:
            db_path: Path to DuckDB database file
            read_only: Whether to open database in read-only mode
            config: Config object for additional settings
        """
        self.db_path = db_path
        self.read_only = read_only
        self.config = config
        self._conn = None

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

    def connect(self):
        """Connect to DuckDB database."""
        if self._conn is None:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            self._conn = duckdb.connect(str(self.db_path), read_only=self.read_only)
            logger.info(f"Connected to DuckDB: {self.db_path}")

    def close(self):
        """Close DuckDB connection."""
        if self._conn:
            self._conn.close()
            self._conn = None
            logger.info("Closed DuckDB connection")

    def execute(self, query: str, params: Optional[Dict[str, Any]] = None) -> duckdb.DuckDBPyConnection:
        """Execute SQL query.

        Args:
            query: SQL query string
            params: Query parameters

        Returns:
            DuckDB connection object
        """
        if not self._conn:
            self.connect()

        try:
            if params:
                return self._conn.execute(query, params)
            else:
                return self._conn.execute(query)
        except Exception as e:
            logger.error(f"Query execution failed: {query}")
            raise

    def create_table_if_not_exists(self, table_name: str, schema: Dict[str, str]):
        """Create table if it doesn't exist.

        Args:
            table_name: Name of the table
            schema: Dictionary mapping column names to SQL types
        """
        columns = ", ".join(f"{col} {dtype}" for col, dtype in schema.items())
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})"
        self.execute(query)
        logger.info(f"Ensured table exists: {table_name}")

    def insert_dataframe(self, table_name: str, df: pd.DataFrame, if_exists: str = 'append'):
        """Insert DataFrame into DuckDB table.

        Args:
            table_name: Target table name
            df: DataFrame to insert
            if_exists: What to do if table exists ('append', 'replace', 'fail')
        """
        if df.empty:
            logger.warning(f"Skipping empty DataFrame for table {table_name}")
            return

        try:
            # Use DuckDB's df() function for efficient insertion
            if if_exists == 'replace':
                self.execute(f"DROP TABLE IF EXISTS {table_name}")
                self._conn.df(df).to_table(table_name)
            else:
                # For append, we need to handle schema compatibility
                existing_schema = self.get_table_schema(table_name)
                if existing_schema:
                    # Table exists, append
                    self._conn.df(df).to_table(table_name, if_exists='append')
                else:
                    # Table doesn't exist, create
                    self._conn.df(df).to_table(table_name)

            logger.info(f"Inserted {len(df)} rows into {table_name}")

        except Exception as e:
            logger.error(f"Failed to insert DataFrame into {table_name}: {e}")
            raise

    def query_to_dataframe(self, query: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """Execute query and return results as DataFrame.

        Args:
            query: SQL query
            params: Query parameters

        Returns:
            Query results as DataFrame
        """
        result = self.execute(query, params)
        return result.df()

    def get_table_schema(self, table_name: str) -> Optional[Dict[str, str]]:
        """Get table schema information.

        Args:
            table_name: Name of the table

        Returns:
            Dictionary of column names to types, or None if table doesn't exist
        """
        try:
            query = """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = ?
            ORDER BY ordinal_position
            """
            result = self.execute(query, {'table_name': table_name})
            df = result.df()
            if df.empty:
                return None
            return dict(zip(df['column_name'], df['data_type']))
        except Exception:
            return None

    def table_exists(self, table_name: str) -> bool:
        """Check if table exists.

        Args:
            table_name: Name of the table

        Returns:
            True if table exists
        """
        query = f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table_name}'"
        result = self.execute(query)
        count = result.fetchone()[0]
        return count > 0

    def get_table_row_count(self, table_name: str) -> int:
        """Get row count for table.

        Args:
            table_name: Name of the table

        Returns:
            Number of rows in table
        """
        if not self.table_exists(table_name):
            return 0

        query = f"SELECT COUNT(*) FROM {table_name}"
        result = self.execute(query)
        return result.fetchone()[0]

    def create_indexes(self, table_name: str, indexes: List[str]):
        """Create indexes on table.

        Args:
            table_name: Name of the table
            indexes: List of column names to index
        """
        for column in indexes:
            index_name = f"idx_{table_name}_{column}"
            query = f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name}({column})"
            self.execute(query)
            logger.info(f"Created index: {index_name}")

    # Bronze Layer Methods
    def save_bronze_meta_mapping(self, df: pd.DataFrame):
        """Save raw metadata to Bronze layer."""
        schema = {
            'provider_code': 'VARCHAR',
            'dataset_code': 'VARCHAR',
            'dataset_name': 'VARCHAR',
            'series_count': 'INTEGER',
            'dimensions': 'VARCHAR',
            'extracted_at': 'TIMESTAMP',
            'source': 'VARCHAR'
        }
        self.create_table_if_not_exists('bronze_meta_mapping', schema)
        self.insert_dataframe('bronze_meta_mapping', df)

    def load_bronze_meta_mapping(self) -> pd.DataFrame:
        """Load raw metadata from Bronze layer."""
        query = "SELECT * FROM bronze_meta_mapping ORDER BY extracted_at DESC"
        return self.query_to_dataframe(query)

    # Silver Layer Methods
    def save_silver_meta_mapping(self, df: pd.DataFrame):
        """Save processed metadata to Silver layer."""
        schema = {
            'provider_code': 'VARCHAR',
            'dataset_code': 'VARCHAR',
            'dataset_name': 'VARCHAR',
            'series_count': 'INTEGER',
            'dimensions': 'VARCHAR',
            'processed_at': 'TIMESTAMP',
            'quality_score': 'FLOAT'
        }
        self.create_table_if_not_exists('silver_meta_mapping', schema)
        self.insert_dataframe('silver_meta_mapping', df)

    def load_silver_meta_mapping(self) -> pd.DataFrame:
        """Load processed metadata from Silver layer."""
        query = "SELECT * FROM silver_meta_mapping ORDER BY processed_at DESC"
        return self.query_to_dataframe(query)

    # Gold Layer Methods
    def save_gold_meta_mapping(self, df: pd.DataFrame):
        """Save final metadata to Gold layer."""
        schema = {
            'provider_code': 'VARCHAR',
            'dataset_code': 'VARCHAR',
            'dataset_name': 'VARCHAR',
            'series_count': 'INTEGER',
            'dimensions': 'VARCHAR',
            'created_at': 'TIMESTAMP',
            'updated_at': 'TIMESTAMP',
            'is_active': 'BOOLEAN'
        }
        self.create_table_if_not_exists('gold_meta_mapping', schema)
        self.insert_dataframe('gold_meta_mapping', df)

    def load_gold_meta_mapping(self, active_only: bool = True) -> pd.DataFrame:
        """Load final metadata from Gold layer."""
        if active_only:
            query = "SELECT * FROM gold_meta_mapping WHERE is_active = true ORDER BY updated_at DESC"
        else:
            query = "SELECT * FROM gold_meta_mapping ORDER BY updated_at DESC"
        return self.query_to_dataframe(query)

    # Timeseries Layer Methods
    def save_timeseries_data(self, df: pd.DataFrame):
        """Save time series observations to database."""
        schema = {
            'provider_code': 'VARCHAR',
            'dataset_code': 'VARCHAR',
            'series_code': 'VARCHAR',
            'series_name': 'VARCHAR',
            'period': 'VARCHAR',
            'value': 'DOUBLE',
            'extracted_at': 'TIMESTAMP'
        }
        self.create_table_if_not_exists('timeseries_observations', schema)

        # Add extracted_at timestamp if not present
        if 'extracted_at' not in df.columns:
            df = df.copy()
            from datetime import datetime
            df['extracted_at'] = datetime.now()

        self.insert_dataframe('timeseries_observations', df)

    def load_timeseries_data(self, provider_code: Optional[str] = None, dataset_code: Optional[str] = None) -> pd.DataFrame:
        """Load time series observations from database.

        Args:
            provider_code: Filter by provider code (optional)
            dataset_code: Filter by dataset code (optional)

        Returns:
            DataFrame with time series observations
        """
        conditions = []
        params = {}

        if provider_code:
            conditions.append("provider_code = ?")
            params['provider_code'] = provider_code

        if dataset_code:
            conditions.append("dataset_code = ?")
            params['dataset_code'] = dataset_code

        where_clause = " AND ".join(conditions) if conditions else ""
        if where_clause:
            where_clause = f"WHERE {where_clause}"

        query = f"SELECT * FROM timeseries_observations {where_clause} ORDER BY provider_code, dataset_code, series_code, period"
        return self.query_to_dataframe(query, params)

    def optimize_tables(self):
        """Optimize database tables and create indexes."""
        # Bronze layer indexes
        if self.table_exists('bronze_meta_mapping'):
            self.create_indexes('bronze_meta_mapping', ['provider_code', 'dataset_code', 'extracted_at'])

        # Silver layer indexes
        if self.table_exists('silver_meta_mapping'):
            self.create_indexes('silver_meta_mapping', ['provider_code', 'dataset_code', 'processed_at'])

        # Gold layer indexes
        if self.table_exists('gold_meta_mapping'):
            self.create_indexes('gold_meta_mapping', ['provider_code', 'dataset_code', 'is_active', 'updated_at'])

        # Timeseries layer indexes
        if self.table_exists('timeseries_observations'):
            self.create_indexes('timeseries_observations', ['provider_code', 'dataset_code', 'series_code', 'period', 'extracted_at'])

        logger.info("Database optimization completed")