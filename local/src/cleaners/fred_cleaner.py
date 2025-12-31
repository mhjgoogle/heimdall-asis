# filepath: local/src/cleaners/fred_cleaner.py

from .base_cleaner import BaseCleaner

from typing import Dict, Any, List

from datetime import datetime

class FredCleaner(BaseCleaner):
    """
    Clean FRED macro economic data.
    
    Converts raw FRED observations into standardized timeseries_macro records.
    - Extracts date (YYYY-MM-DD) and numeric values
    - Aggregates multiple series if present
    """

    def _validate_fred_payload(self, raw_data: Dict[str, Any]) -> bool:
        """Validate FRED raw data structure."""
        if not isinstance(raw_data, dict):
            return False
        if 'series_data' not in raw_data:
            return False
        if not isinstance(raw_data.get('series_data'), dict):
            return False
        return True

    def process(self, raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Process FRED raw data and return cleaned records for timeseries_macro.
        
        Args:
            raw_data: Dict containing 'series_data' with observations
            
        Returns:
            List of cleaned dicts with keys: catalog_key, date, value
        """
        # Validate input structure
        if not self._validate_fred_payload(raw_data):
            return []
        
        cleaned_data = []
        value_by_date = {}

        series_data = raw_data.get('series_data', {})

        for series_info in series_data.values():
            observations = series_info.get('observations', [])

            for obs in observations:
                date_str = obs.get('date')
                value = obs.get('value')

                if date_str and value is not None:
                    try:
                        # Convert value to float
                        value_float = float(value) if isinstance(value, str) else value
                        
                        # Aggregate values by date (sum if multiple series)
                        if date_str in value_by_date:
                            value_by_date[date_str] += value_float
                        else:
                            value_by_date[date_str] = value_float

                    except (ValueError, TypeError):
                        continue

        # Build cleaned records
        for date, value in sorted(value_by_date.items()):
            cleaned_data.append({
                'catalog_key': self.catalog_key,
                'date': date,  # Already in YYYY-MM-DD format from FRED
                'value': value
            })

        return cleaned_data