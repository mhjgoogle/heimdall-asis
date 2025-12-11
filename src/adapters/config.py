# src/adapters/config.py
"""Configuration adapter for ASIS system.

Handles configuration loading and management.
"""

import os
from pathlib import Path
from typing import Dict, Any


class DataPaths:
    """Data path configuration."""

    def __init__(self):
        self.base_dir = Path(__file__).parent.parent.parent
        self.data_dir = self.base_dir / "data"
        self.input_dir = self.data_dir / "input"
        self.output_dir = self.data_dir / "output"


class Config:
    """Main configuration class."""

    def __init__(self):
        # API settings
        self.DBNOMICS_API_BASE_URL = os.getenv("DBNOMICS_API_BASE_URL", "https://api.db.nomics.world/v22")
        self.REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))
        self.API_MAX_WORKERS = int(os.getenv("API_MAX_WORKERS", "4"))

        # Data paths
        self.data_paths = DataPaths()
        self.META_MAPPING_INPUT = self.data_paths.input_dir / "meta_mapping"
        self.DUCKDB_PATH = self.data_paths.data_dir / "asis.duckdb"

        # DuckDB settings
        self.DUCKDB_ENABLE_STORAGE = os.getenv("DUCKDB_ENABLE_STORAGE", "true").lower() == "true"
        self.DUCKDB_READ_ONLY = os.getenv("DUCKDB_READ_ONLY", "false").lower() == "true"
        self.DUCKDB_AUTO_OPTIMIZE = os.getenv("DUCKDB_AUTO_OPTIMIZE", "true").lower() == "true"

        # Logging
        self.LOG_DIR = self.data_paths.base_dir / "logs"
        self.LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
        self.LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        self.LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
        self.ENABLE_FILE_LOGGING = True
        self.ENABLE_CONSOLE_LOGGING = True

        # Processing
        self.MAX_RECORDS = int(os.getenv("MAX_RECORDS", "1000"))

        # DBnomics API params
        self.DBNOMICS_API_PARAMS = {
            'limit': 1000,
            'observations': True,
        }

        # Enable log summary
        self.ENABLE_LOG_SUMMARY = True