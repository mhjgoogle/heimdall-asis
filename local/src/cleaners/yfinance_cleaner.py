# filepath: local/src/cleaners/yfinance_cleaner.py

from .base_cleaner import BaseCleaner

from typing import Dict, Any, List, Union

from datetime import datetime
import json

class YFinanceCleaner(BaseCleaner):
    """
    Clean YFinance data for both OHLCV and News.
    
    Dynamically routes data:
    - OHLCV data -> timeseries_micro (val_open, val_high, val_low, val_close, val_volume)
    - News data -> news_intel_pool (title, url, published_at, fingerprint, title_hash)
    """

    def process(self, raw_data: Union[Dict, List]) -> List[Dict[str, Any]]:
        """
        Process YFinance raw data. Detects type and routes accordingly.
        
        Args:
            raw_data: Either OHLCV data dict or News list
            
        Returns:
            List of cleaned dicts for appropriate Silver table
        """
        # Detect data type
        if isinstance(raw_data, dict):
            # Check if it's OHLCV (has 'data' key with 'historical_data' or 'mode'='history')
            if raw_data.get('mode') == 'history' or 'data' in raw_data:
                return self._process_ohlcv(raw_data)
            # Fallback: check if structure looks like OHLCV dict
            elif self._is_ohlcv_dict(raw_data):
                return self._process_ohlcv(raw_data)
        
        # Check if it's News data (list of dicts with news properties)
        elif isinstance(raw_data, list) and len(raw_data) > 0:
            if isinstance(raw_data[0], dict) and ('title' in raw_data[0] or 'headline' in raw_data[0]):
                return self._process_news(raw_data)
        
        return []

    def _is_ohlcv_dict(self, data: Dict) -> bool:
        """Check if data looks like OHLCV structure - strict validation."""
        if not isinstance(data, dict):
            return False
        
        # Required OHLCV fields
        required_ohlcv_keys = {'Open', 'High', 'Low', 'Close'}
        
        # Check if has 'data' > 'historical_data' structure
        if 'data' in data and isinstance(data['data'], dict):
            hist_data = data['data'].get('historical_data', [])
            if isinstance(hist_data, list) and len(hist_data) > 0:
                # Check if first element has OHLC keys
                first = hist_data[0]
                if isinstance(first, dict) and required_ohlcv_keys.issubset(set(first.keys())):
                    return True
        
        # Check if direct structure is OHLCV (multiple date keys with OHLC)
        if len(data) > 0:
            ohlcv_count = 0
            sample_size = min(3, len(data))
            for i, val in enumerate(list(data.values())[:sample_size]):
                if isinstance(val, dict) and required_ohlcv_keys.issubset(set(val.keys())):
                    ohlcv_count += 1
            # If at least 2 out of 3 samples have all OHLC keys, it's likely OHLCV
            if sample_size > 0 and ohlcv_count >= 2:
                return True
        
        return False

    def _process_ohlcv(self, raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Process OHLCV historical data."""
        cleaned = []
        
        # Handle structure: {mode, data: {historical_data: [...]}}
        if 'data' in raw_data and isinstance(raw_data['data'], dict):
            historical_data = raw_data['data'].get('historical_data', [])
            if isinstance(historical_data, list):
                for row in historical_data:
                    cleaned.append(self._ohlcv_record(row))
        # Handle structure: {date: {Open, High, Low, Close, Volume}}
        elif isinstance(raw_data, dict):
            for date, row in raw_data.items():
                if isinstance(row, dict) and 'Open' in row:
                    # Check if date is a string or the row contains date
                    if 'Date' in row:
                        date = self._normalize_date(row['Date'])
                    else:
                        date = str(date)
                    
                    record = self._ohlcv_record(row)
                    record['date'] = date
                    cleaned.append(record)
        
        return cleaned

    def _ohlcv_record(self, row: Dict) -> Dict[str, Any]:
        """Convert a single OHLCV row to cleaned format."""
        # Extract date if present in row
        date_value = row.get('Date')
        if date_value:
            date = self._normalize_date(date_value)
        else:
            date = None
        
        record = {
            'catalog_key': self.catalog_key,
            'val_open': self._safe_float(row.get('Open')),
            'val_high': self._safe_float(row.get('High')),
            'val_low': self._safe_float(row.get('Low')),
            'val_close': self._safe_float(row.get('Close')),
            'val_volume': self._safe_int(row.get('Volume'))
        }
        
        if date:
            record['date'] = date
        
        return record

    def _normalize_date(self, date_val: Any) -> str:
        """Convert various date formats to YYYY-MM-DD."""
        if isinstance(date_val, str):
            # If already YYYY-MM-DD, return as-is
            if len(date_val) == 10 and date_val[4] == '-':
                return date_val[:10]
            # If ISO format with timezone (e.g., "2003-12-01 00:00:00+00:00")
            if ' ' in date_val:
                return date_val.split()[0]
            # Try to parse datetime string
            try:
                dt = datetime.fromisoformat(date_val.replace('Z', '+00:00'))
                return dt.strftime('%Y-%m-%d')
            except:
                return date_val[:10] if len(date_val) >= 10 else str(date_val)
        
        return str(date_val)[:10]

    def _process_news(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Process YFinance or pre-parsed news data."""
        cleaned = []

        for item in raw_data:
            title = item.get('title') or item.get('headline')
            url = item.get('link') or item.get('url')
            
            if not (title and url):
                continue

            # Generate fingerprints
            fingerprint = self.generate_fingerprint(url)
            title_hash = self.generate_title_hash(title)

            # Extract published_at
            published_at = self._extract_publish_time(item)

            cleaned.append({
                'fingerprint': fingerprint,
                'title_hash': title_hash,
                'catalog_key': self.catalog_key,
                'published_at': published_at,
                'title': title,
                'url': url,
                'sentiment_score': None,
                'ai_summary': None
            })

        return cleaned

    def _extract_publish_time(self, item: Dict) -> str:
        """Extract and standardize publication time."""
        # Try multiple field names
        pub_time = (
            item.get('providerPublishTime') or
            item.get('publishedAt') or
            item.get('published_at') or
            item.get('raw_pub_date')
        )

        if isinstance(pub_time, int):
            # Unix timestamp
            try:
                dt = datetime.utcfromtimestamp(pub_time)
                return dt.strftime('%Y-%m-%d %H:%M:%S')
            except:
                pass
        
        elif isinstance(pub_time, str):
            # Try to parse ISO format
            try:
                if '+' in pub_time or 'Z' in pub_time:
                    dt = datetime.fromisoformat(pub_time.replace('Z', '+00:00'))
                else:
                    dt = datetime.fromisoformat(pub_time)
                return dt.strftime('%Y-%m-%d %H:%M:%S')
            except:
                # Return as-is if can't parse
                return pub_time[:19] if len(pub_time) >= 19 else pub_time

        # Default to current time
        return datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

    def _safe_float(self, val: Any) -> float:
        """Safely convert value to float."""
        if val is None:
            return 0.0
        try:
            return float(val)
        except (ValueError, TypeError):
            return 0.0

    def _safe_int(self, val: Any) -> int:
        """Safely convert value to int."""
        if val is None:
            return 0
        try:
            return int(float(val))  # Handle float strings
        except (ValueError, TypeError):
            return 0