# src/adapters/logging.py
"""Logging adapter for ASIS system.

Handles logging configuration and management.
"""

import logging
import sys
from pathlib import Path
from typing import Optional


def setup_logging(
    log_dir: Path,
    log_prefix: str = "asis",
    log_level: str = "INFO",
    enable_file: bool = True,
    enable_console: bool = True,
    log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    log_date_format: str = "%Y-%m-%d %H:%M:%S"
) -> None:
    """Setup logging configuration.

    Args:
        log_dir: Directory for log files
        log_prefix: Prefix for log filenames
        log_level: Logging level
        enable_file: Enable file logging
        enable_console: Enable console logging
        log_format: Log message format
        log_date_format: Date format for logs
    """
    # Create logger
    logger = logging.getLogger()
    logger.setLevel(getattr(logging, log_level.upper()))

    # Clear existing handlers
    logger.handlers.clear()

    # Create formatter
    formatter = logging.Formatter(log_format, datefmt=log_date_format)

    # File handler
    if enable_file:
        log_dir.mkdir(parents=True, exist_ok=True)
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = log_dir / f"{log_prefix}_{timestamp}.log"

        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(getattr(logging, log_level.upper()))
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    # Console handler
    if enable_console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(getattr(logging, log_level.upper()))
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)


def save_logs(log_file: Path) -> None:
    """Save current log buffer to file.

    Args:
        log_file: Path to save logs
    """
    # This is a simple implementation - in practice, you'd collect logs from handlers
    logger = logging.getLogger()
    with open(log_file, 'w') as f:
        f.write("Log summary saved\n")  # Placeholder