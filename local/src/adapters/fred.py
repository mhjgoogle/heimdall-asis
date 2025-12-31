# filepath: local/src/adapters/fred.py
"""FRED (Federal Reserve Economic Data) adapter for macroeconomic indicators."""

import os
import sys
import json
import requests
from datetime import datetime
from typing import Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add project root to path if not already
project_root = os.path.join(os.path.dirname(__file__), '..', '..')
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from .base import BaseAdapter, IngestionContext
from .http_client import RetryableHTTPClient
from local.config.config import AppConfig


class FredAdapter(BaseAdapter):
    """Adapter for fetching data from Federal Reserve Economic Data (FRED) API."""

    def __init__(self):
        super().__init__()
        self.source_name = "FRED"
        self.api_key = AppConfig.FRED_KEY
        self.base_url = "https://api.stlouisfed.org/fred"
        self.request_timeout = getattr(AppConfig, 'FRED_REQUEST_TIMEOUT', 30)
        self.http_client = RetryableHTTPClient(max_retries=3, backoff_factor=1.0, timeout=self.request_timeout)

        if not self.api_key:
            raise ValueError("FRED_API_KEY not configured in AppConfig")

    def validate_config(self, config_params: Dict[str, Any]) -> bool:
        """Validate FRED-specific configuration parameters."""
        if 'series' not in config_params:
            return False

        series = config_params['series']
        if isinstance(series, str):
            series = [series]

        if not isinstance(series, list) or len(series) == 0:
            return False

        # Basic validation - series IDs should be non-empty strings
        for series_id in series:
            if not isinstance(series_id, str) or not series_id.strip():
                return False

        return True

    def fetch_raw_data(self, context: IngestionContext) -> str:
        """Fetch raw FRED data and return as JSON string for Bronze Layer."""
        if not self.validate_config(context.config_params):
            raise ValueError(f"Invalid FRED config for {context.catalog_key}: {context.config_params}")

        series_ids = context.config_params['series']
        if isinstance(series_ids, str):
            series_ids = [series_ids]

        # Determine observation start date
        observation_start = self.get_incremental_start_date(context)

        # For JUDGMENT data, get full historical if no watermark exists
        if context.role == "JUDGMENT" and observation_start is None:
            observation_start = '1950-01-01'  # FRED inception

        # Fetch data for all series concurrently
        all_data = self._fetch_multiple_series(series_ids, observation_start)

        # Structure the response
        response_data = {
            'catalog_key': context.catalog_key,
            'source_api': context.source_api,
            'fetched_at': datetime.now().isoformat(),
            'observation_start': observation_start,
            'series_data': all_data,
            'metadata': {
                'series_count': len(series_ids),
                'role': context.role,
                'frequency': context.frequency
            }
        }

        return json.dumps(response_data, ensure_ascii=False, indent=2)

    def dry_run(self, context: IngestionContext) -> bool:
        """Test if FRED data can be fetched without storing it."""
        try:
            data = self.fetch_raw_data(context)
            parsed = json.loads(data)
            series_data = parsed.get('series_data', {})
            if not series_data:
                return False
            # Check if at least one series has observations
            for series_info in series_data.values():
                if series_info.get('observations'):
                    return True
            return False
        except Exception:
            return False

    def _fetch_multiple_series(self, series_ids: list, observation_start: str = None) -> Dict[str, Any]:
        """Fetch data for multiple series concurrently."""
        all_data = {}

        # Use ThreadPoolExecutor for concurrent requests
        with ThreadPoolExecutor(max_workers=min(len(series_ids), 5)) as executor:
            future_to_series = {
                executor.submit(self._fetch_single_series, series_id, observation_start): series_id
                for series_id in series_ids
            }

            for future in as_completed(future_to_series):
                series_id = future_to_series[future]
                try:
                    data = future.result()
                    all_data[series_id] = data
                except Exception as e:
                    all_data[series_id] = {'error': str(e)}

        return all_data

    def _fetch_single_series(self, series_id: str, observation_start: str = None) -> Dict[str, Any]:
        """Fetch data for a single FRED series."""
        url = f"{self.base_url}/series/observations"

        api_params = {
            'series_id': series_id,
            'api_key': self.api_key,
            'file_type': 'json',
            'observation_end': datetime.now().strftime('%Y-%m-%d')
        }

        if observation_start:
            api_params['observation_start'] = observation_start

        try:
            response = self.http_client.get(url, params=api_params)
            data = response.json()

            # Validate response structure
            if 'observations' not in data:
                raise ValueError(f"Unexpected FRED API response structure for {series_id}")

            # Clean observations (remove '.' values which indicate missing data)
            cleaned_observations = []
            for obs in data['observations']:
                if obs.get('value') != '.' and obs.get('value') is not None:
                    try:
                        # Convert value to float
                        obs_copy = obs.copy()
                        obs_copy['value'] = float(obs['value'])
                        cleaned_observations.append(obs_copy)
                    except (ValueError, TypeError):
                        # Skip invalid values
                        continue

            return {
                'series_id': series_id,
                'observations': cleaned_observations,
                'count': len(cleaned_observations),
                'date_range': {
                    'start': min((obs['date'] for obs in cleaned_observations), default=None),
                    'end': max((obs['date'] for obs in cleaned_observations), default=None)
                } if cleaned_observations else {}
            }

        except requests.exceptions.RequestException as e:
            raise Exception(f"FRED API request failed for {series_id}: {e}")
        except json.JSONDecodeError as e:
            raise Exception(f"Invalid JSON response from FRED API for {series_id}: {e}")
        except Exception as e:
            raise Exception(f"Unexpected error fetching FRED data for {series_id}: {e}")