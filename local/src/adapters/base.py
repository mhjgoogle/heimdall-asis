# filepath: local/src/adapters/base.py
"""Base adapter classes for Heimdall-Asis Bronze Layer data ingestion."""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
from dataclasses import dataclass
from datetime import datetime
import json

@dataclass
class StandardMetric:
    """Standardized metric data structure for Bronze Layer."""
    catalog_key: str
    ts_label: str  # Date/timestamp for the data point
    value: Any     # The actual metric value
    metadata: Dict[str, Any] = None  # Additional context

@dataclass
class IngestionContext:
    """Context information for data ingestion."""
    catalog_key: str
    source_api: str
    config_params: Dict[str, Any]
    role: str  # JUDGMENT or VALIDATION
    last_ingested_at: Optional[datetime] = None
    frequency: str = "DAILY"

class BaseAdapter(ABC):
    """Base class for all data source adapters."""

    def __init__(self):
        self.source_name = "base"

    @abstractmethod
    def fetch_raw_data(self, context: IngestionContext) -> str:
        """Fetch raw data from source and return as JSON string.

        Args:
            context: Ingestion context with catalog info and config

        Returns:
            Raw data as JSON string for Bronze Layer storage
        """
        pass

    @abstractmethod
    def dry_run(self, context: IngestionContext) -> bool:
        """Test if data can be fetched without storing it.

        Args:
            context: Ingestion context

        Returns:
            True if data can be fetched successfully, False otherwise
        """
        pass

    def get_incremental_start_date(self, context: IngestionContext) -> Optional[str]:
        """Determine the start date for incremental data fetching.

        Args:
            context: Ingestion context

        Returns:
            ISO date string for incremental start, or None for full refresh
        """
        if context.last_ingested_at is None:
            return None  # Full refresh

        # For JUDGMENT data, be conservative - get last 30 days
        # For VALIDATION data, be more aggressive - get last 7 days
        if context.role == "JUDGMENT":
            days_back = 30
        else:  # VALIDATION
            days_back = 7

        from datetime import timedelta
        start_date = context.last_ingested_at - timedelta(days=days_back)
        return start_date.date().isoformat()

    def validate_config(self, config_params: Dict[str, Any]) -> bool:
        """Validate adapter-specific configuration parameters.

        Args:
            config_params: Configuration parameters from data_catalog

        Returns:
            True if valid, False otherwise
        """
        return True

    def get_request_hash(self, context: IngestionContext) -> str:
        """Generate unique hash for request deduplication.

        Args:
            context: Ingestion context

        Returns:
            SHA256 hash string
        """
        import hashlib
        now = datetime.now()

        # Create time window based on frequency
        if context.frequency == 'HOURLY':
            time_suffix = now.strftime('%Y-%m-%d-%H')
        elif context.frequency == 'DAILY':
            time_suffix = now.strftime('%Y-%m-%d')
        elif context.frequency == 'MONTHLY':
            time_suffix = now.strftime('%Y-%m')
        else:
            time_suffix = now.strftime('%Y-%m-%d')

        hash_input = f"{context.catalog_key}:{json.dumps(context.config_params, sort_keys=True)}:{time_suffix}"
        return hashlib.sha256(hash_input.encode()).hexdigest()