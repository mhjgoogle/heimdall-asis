# src/adapters/api_client.py
"""API Client adapter for ASIS system.

Handles HTTP requests to external APIs.
"""

import logging
from typing import Dict, Any, Optional
import requests

logger = logging.getLogger(__name__)


class APIClient:
    """HTTP client for API interactions."""

    def __init__(self):
        self.session = requests.Session()

    def get(self, url: str, timeout: int = 30, **kwargs) -> requests.Response:
        """Make GET request.

        Args:
            url: Request URL
            timeout: Request timeout
            **kwargs: Additional request parameters

        Returns:
            Response object

        Raises:
            requests.RequestException: On request error
        """
        try:
            response = self.session.get(url, timeout=timeout, **kwargs)
            response.raise_for_status()
            return response
        except requests.RequestException as e:
            logger.error(f"API request failed: {e}")
            raise

    def close(self):
        """Close the session."""
        self.session.close()


def fetch_dbnomics_series(client: APIClient, url: str, params: Dict[str, Any], timeout: int) -> Optional[Dict[str, Any]]:
    """Fetch time series data from DBnomics API.

    Args:
        client: API client instance
        url: API URL
        params: Query parameters
        timeout: Request timeout

    Returns:
        API response data or None on error
    """
    try:
        response = client.get(url, params=params, timeout=timeout)
        return response.json()
    except Exception as e:
        logger.error(f"Failed to fetch DBnomics series: {e}")
        return None