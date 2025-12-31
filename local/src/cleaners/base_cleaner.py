# filepath: local/src/cleaners/base_cleaner.py

from abc import ABC, abstractmethod

import json
import hashlib
from typing import Dict, Any, List

class BaseCleaner(ABC):

    def __init__(self, catalog_key: str):
        self.catalog_key = catalog_key

    @abstractmethod
    def process(self, raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Process raw data and return a list of standardized dictionaries for insertion into Silver layer.
        Each dict should match the schema of the target table.
        """
        pass

    def generate_fingerprint(self, url: str) -> str:
        """Generate MD5 fingerprint for URL."""
        return hashlib.md5(url.encode()).hexdigest()

    def generate_title_hash(self, title: str) -> str:
        """Generate short hash for title deduplication."""
        return hashlib.md5(title.encode()).hexdigest()[:16]