# filepath: local/src/adapters/http_client.py

import logging
import time
from typing import Optional, Dict, Any
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


class RetryableHTTPClient:
    def __init__(self, max_retries: int = 3, backoff_factor: float = 1.0, timeout: int = 30):
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.timeout = timeout
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        session = requests.Session()
        retry_strategy = Retry(
            total=self.max_retries,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["GET", "POST"],
            backoff_factor=self.backoff_factor
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def get(self, url: str, params: Optional[Dict[str, Any]] = None, **kwargs) -> requests.Response:
        for attempt in range(self.max_retries + 1):
            try:
                response = self.session.get(url, params=params, timeout=self.timeout, **kwargs)
                response.raise_for_status()
                return response
            except requests.exceptions.RequestException as e:
                if attempt == self.max_retries:
                    logger.error(f"Failed after {self.max_retries + 1} attempts: {e}")
                    raise
                wait_time = (2 ** attempt) * self.backoff_factor
                logger.warning(f"Attempt {attempt + 1} failed, retrying in {wait_time}s: {e}")
                time.sleep(wait_time)

    def post(self, url: str, data: Optional[Dict[str, Any]] = None, json: Optional[Dict[str, Any]] = None, **kwargs) -> requests.Response:
        for attempt in range(self.max_retries + 1):
            try:
                response = self.session.post(url, data=data, json=json, timeout=self.timeout, **kwargs)
                response.raise_for_status()
                return response
            except requests.exceptions.RequestException as e:
                if attempt == self.max_retries:
                    logger.error(f"Failed after {self.max_retries + 1} attempts: {e}")
                    raise
                wait_time = (2 ** attempt) * self.backoff_factor
                logger.warning(f"Attempt {attempt + 1} failed, retrying in {wait_time}s: {e}")
                time.sleep(wait_time)

    def close(self):
        self.session.close()
