# filepath: local/src/adapters/yfinance_adapter.py
"""Yahoo Finance adapter supporting both historical price data and news intelligence."""

import sys
import os
import json
from datetime import datetime, timedelta
from typing import Dict, Any

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from .base import BaseAdapter, IngestionContext
from .http_client import RetryableHTTPClient
from local.config import AppConfig

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

try:
    from dateutil import parser
    HAS_DATEUTIL = True
except ImportError:
    HAS_DATEUTIL = False


class YFinanceAdapter(BaseAdapter):
    """Adapter for Yahoo Finance data supporting OHLCV and news intelligence."""

    def __init__(self):
        super().__init__()
        self.source_name = "yfinance"
        self.http_client = RetryableHTTPClient(timeout=30)
        try:
            import yfinance as yf
            self.yf = yf
        except ImportError:
            raise ImportError("yfinance package not installed")

    def validate_config(self, config_params: Dict[str, Any]) -> bool:
        """Validate yfinance-specific configuration parameters."""
        if 'ticker' not in config_params:
            return False

        ticker = config_params['ticker']
        if not isinstance(ticker, str) or not ticker.strip():
            return False

        return True

    def fetch_raw_data(self, context: IngestionContext) -> str:
        """Fetch raw yfinance OHLCV data and return as JSON string for Bronze Layer."""
        if not self.validate_config(context.config_params):
            raise ValueError(f"Invalid yfinance config for {context.catalog_key}: {context.config_params}")

        ticker = context.config_params['ticker']
        ticker_obj = self.yf.Ticker(ticker)

        # Only fetch historical OHLCV data (news handled by RSSAdapter)
        data = self._fetch_historical_data(ticker_obj, ticker, context)

        # Structure the response
        response_data = {
            'catalog_key': context.catalog_key,
            'source_api': context.source_api,
            'ticker': ticker,
            'mode': 'history',
            'fetched_at': datetime.now().isoformat(),
            'data': data,
            'metadata': {
                'role': context.role,
                'frequency': context.frequency,
                'data_type': 'ohlcv'
            }
        }

        return json.dumps(response_data, ensure_ascii=False, indent=2, default=str)

    def dry_run(self, context: IngestionContext) -> bool:
        """Test if yfinance OHLCV data can be fetched without storing it."""
        try:
            data = self.fetch_raw_data(context)
            parsed = json.loads(data)
            data_part = parsed.get('data', {})
            # Check historical_data
            if 'historical_data' in data_part and data_part['historical_data']:
                return len(data_part['historical_data']) > 0
            return False
        except Exception:
            return False

    def _fetch_historical_data(self, ticker_obj, ticker: str, context: IngestionContext) -> Dict[str, Any]:
        """Fetch historical OHLCV data."""
        # Determine start date for incremental fetching
        start_date = self.get_incremental_start_date(context)

        try:
            # Get historical data
            if start_date:
                hist = ticker_obj.history(start=start_date, interval="1d")
            else:
                # For JUDGMENT data or first run, get max period
                hist = ticker_obj.history(period="max", interval="1d")

            if hist.empty:
                return {
                    'historical_data': [],
                    'count': 0,
                    'date_range': {'start': None, 'end': None},
                    'error': 'No data available'
                }

            # Convert to records and clean data
            records = hist.reset_index().to_dict('records')

            # Clean records - remove NaN values and convert to float where possible
            cleaned_records = []
            for record in records:
                clean_record = {}
                for key, value in record.items():
                    # Handle NaN/None values
                    if HAS_PANDAS and pd.isna(value):
                        clean_record[key] = None
                    elif value is None or (isinstance(value, float) and str(value).lower() in ['nan', 'inf', '-inf']):
                        clean_record[key] = None
                    elif isinstance(value, (int, float)):
                        clean_record[key] = float(value)
                    else:
                        clean_record[key] = str(value)
                cleaned_records.append(clean_record)

            # Extract dates for range calculation
            dates = []
            for r in cleaned_records:
                date_val = r.get('Date')
                if date_val:
                    # Handle different date formats
                    if isinstance(date_val, str):
                        try:
                            # Try to parse string date
                            if HAS_DATEUTIL:
                                date_obj = parser.parse(date_val)
                                dates.append(date_obj)
                        except:
                            pass
                    elif hasattr(date_val, 'isoformat'):  # datetime-like object
                        dates.append(date_val)
                    # If it's already a date object, use it directly

            return {
                'historical_data': cleaned_records,
                'count': len(cleaned_records),
                'date_range': {
                    'start': min(dates).isoformat() if dates else None,
                    'end': max(dates).isoformat() if dates else None
                }
            }

        except Exception as e:
            return {
                'historical_data': [],
                'count': 0,
                'date_range': {'start': None, 'end': None},
                'error': str(e)
            }

    def _fetch_news_data(self, ticker_obj, ticker: str, context: IngestionContext) -> Dict[str, Any]:
        """Fetch news intelligence data."""
        try:
            # Get news data
            news_data = ticker_obj.news

            if not news_data:
                return {
                    'news_items': [],
                    'count': 0,
                    'error': 'No news data available'
                }

            # Clean and structure news data
            cleaned_news = []
            for item in news_data:
                # Extract relevant fields
                news_item = {
                    'title': item.get('title', ''),
                    'publisher': item.get('publisher', ''),
                    'link': item.get('link', ''),
                    'published_timestamp': item.get('providerPublishTime'),
                    'type': item.get('type', ''),
                    'related_tickers': item.get('relatedTickers', [])
                }

                # Convert timestamp if available
                if news_item['published_timestamp']:
                    try:
                        # Yahoo Finance uses Unix timestamp
                        dt = datetime.fromtimestamp(news_item['published_timestamp'])
                        news_item['published_at'] = dt.isoformat()
                    except (ValueError, TypeError):
                        news_item['published_at'] = None
                else:
                    news_item['published_at'] = None

                cleaned_news.append(news_item)

            return {
                'news_items': cleaned_news,
                'count': len(cleaned_news)
            }

        except Exception as e:
            return {
                'news_items': [],
                'count': 0,
                'error': str(e)
            }