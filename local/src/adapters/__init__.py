# filepath: local/src/adapters/__init__.py
from .base import BaseAdapter, IngestionContext, StandardMetric
from .adapter_factory import AdapterFactory
from .adapter_manager import AdapterManager
from .fred import FredAdapter
from .yfinance_adapter import YFinanceAdapter
from .rss_adapter import RSSAdapter

__all__ = [
    'BaseAdapter',
    'IngestionContext',
    'StandardMetric',
    'AdapterFactory',
    'AdapterManager',
    'FredAdapter',
    'YFinanceAdapter',
    'RSSAdapter',
]