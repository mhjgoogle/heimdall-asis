# filepath: local/config.py
"""Runtime parameters for local processing."""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file (in config directory)
load_dotenv(dotenv_path=Path(__file__).parent / ".env")


class AppConfig:
    """Application configuration class."""

    # Database configuration - point to parent 'local' directory, not 'config' directory
    DATA_DIR = Path(__file__).parent.parent / "data"
    DB_PATH = DATA_DIR / "heimdall.db"

    # Logging configuration - point to parent 'local' directory, not 'config' directory
    LOG_DIR = Path(__file__).parent.parent / "logs"
    LOG_FILE = LOG_DIR / "ingestion_batch.log"

    # Ensure directories exist
    DATA_DIR.mkdir(exist_ok=True)
    LOG_DIR.mkdir(exist_ok=True)

    # API Keys (loaded from .env)
    FRED_KEY = os.getenv("FRED_API_KEY")
    NEWSAPI_KEY = os.getenv("NEWSAPI_KEY")  # NewsAPI.org API key
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
    SUPABASE_URL = os.getenv("SUPABASE_URL")
    SUPABASE_KEY = os.getenv("SUPABASE_KEY")

    # Other runtime parameters
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    DRY_RUN_LIMIT = 1  # For batch scripts

    # Data source specific configurations
    FRED_REQUEST_TIMEOUT = 30  # seconds
    RSS_REQUEST_TIMEOUT = 10   # seconds
    NEWSAPI_REQUEST_TIMEOUT = 15  # seconds
    YF_REQUEST_TIMEOUT = 10    # seconds


# For backward compatibility
DATA_DIR = AppConfig.DATA_DIR
DB_PATH = AppConfig.DB_PATH
FRED_API_KEY = AppConfig.FRED_KEY
LOG_LEVEL = AppConfig.LOG_LEVEL
DRY_RUN_LIMIT = AppConfig.DRY_RUN_LIMIT