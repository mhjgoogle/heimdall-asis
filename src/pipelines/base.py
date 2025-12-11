# src/pipelines/base.py
"""Base Pipeline class for ASIS system.

Provides common pipeline orchestration pattern.
"""

import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from pathlib import Path

logger = logging.getLogger(__name__)


class BasePipeline(ABC):
    """Base class for all pipelines.

    Implements Extract -> Process -> Export pattern.
    """

    def __init__(self, name: str, provider_limit: Optional[int] = None):
        self.name = name
        self.provider_limit = provider_limit

    @abstractmethod
    def extract(self) -> Dict[str, Any]:
        """Extract phase: Read input data."""
        pass

    @abstractmethod
    def process(self, input_data: Dict[str, Any]) -> Any:
        """Process phase: Transform data."""
        pass

    @abstractmethod
    def export(self, processed_data: Any) -> Dict[str, Path]:
        """Export phase: Save results."""
        pass

    def run(self) -> Dict[str, Path]:
        """Run the complete pipeline."""
        logger.info(f"Starting {self.name} pipeline")

        # Extract
        extracted = self.extract()

        # Process
        processed = self.process(extracted)

        # Export
        exported = self.export(processed)

        logger.info(f"Completed {self.name} pipeline")
        return exported