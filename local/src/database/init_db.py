# filepath: local/src/database/init_db.py

"""Initialize SQLite database and data directory for ASIS.

Dynamically parses docs/Phase4_DataModel.md to extract SQL DDL and seed data,
then creates tables and populates data_catalog with asset definitions.
"""
import os
import re
import json
import sqlite3
import logging
from typing import List, Dict, Any

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def parse_datamodel_doc() -> Dict[str, Any]:
    """Parse docs/Phase4_DataModel.md to extract DDL and seed data."""
    doc_path = os.path.join(os.path.dirname(__file__), '..', '..', '..', 'docs', 'Phase4_DataModel.md')

    try:
        with open(doc_path, 'r', encoding='utf-8') as f:
            content = f.read()
    except FileNotFoundError:
        raise FileNotFoundError(f"Phase4_DataModel.md not found at {doc_path}")

    # Extract SQL DDL from section 3
    ddl_section = re.search(r'3\. 物理 Schema 定义.*?(?=4\.|5\.|$)', content, re.DOTALL)
    if not ddl_section:
        raise ValueError("Could not find SQL DDL section in Phase4_DataModel.md")

    ddl_content = ddl_section.group(0)

    # Extract individual CREATE TABLE statements
    table_statements = []
    create_matches = re.findall(r'CREATE TABLE.*?;', ddl_content, re.DOTALL)
    for match in create_matches:
        # Clean up the statement
        statement = match.strip()
        # Handle multi-line comments
        statement = re.sub(r'--.*$', '', statement, flags=re.MULTILINE)
        statement = ' '.join(statement.split())  # Normalize whitespace
        table_statements.append(statement)

    # Extract seed data from section 5
    seed_data = []
    registry_section = re.search(r'5\. 全量资产定义矩阵.*?(?=6\.|$)', content, re.DOTALL)

    if registry_section:
        registry_content = registry_section.group(0)

        # Simple line-by-line parsing for table rows
        lines = registry_content.split('\n')
        in_table = False

        for line in lines:
            line = line.strip()
            if not line or line.startswith('$') or line.startswith('| ---'):
                continue

            # Check if this is a table header
            if '| Catalog Key |' in line:
                in_table = True
                continue

            # Parse data rows (lines starting and ending with |)
            if in_table and line.startswith('|') and line.endswith('|') and '|' in line:
                parts = [part.strip() for part in line.split('|')[1:-1]]  # Remove first and last empty parts

                if len(parts) >= 9 and parts[0] and not parts[0].startswith('-'):
                    catalog_key, country, scope, role, entity_name, source_api, frequency, config_params, search_keywords = parts[:9]

                    # Clean and validate data
                    catalog_key = catalog_key.strip()
                    country = country.strip()
                    scope = scope.strip().upper()
                    role = role.strip().upper()
                    entity_name = entity_name.strip()
                    source_api = source_api.strip()
                    frequency = frequency.strip()
                    config_params = config_params.strip()
                    search_keywords = search_keywords.strip()

                    # Skip if any required field is empty or invalid
                    if not all([catalog_key, country, scope, role, entity_name, source_api, frequency]):
                        continue

                    # Validate role enum (handle short forms)
                    role_map = {'J': 'JUDGMENT', 'V': 'VALIDATION', 'JUDGMENT': 'JUDGMENT', 'VALIDATION': 'VALIDATION'}
                    if role not in role_map:
                        logger.warning(f"Invalid role '{role}' for {catalog_key}, skipping")
                        continue
                    role = role_map[role]

                    # Parse config_params JSON
                    try:
                        config_params_json = json.loads(config_params)
                    except json.JSONDecodeError:
                        logger.warning(f"Invalid JSON in config_params for {catalog_key}: {config_params}, skipping")
                        continue

                    seed_data.append({
                        'catalog_key': catalog_key,
                        'country': country,
                        'scope': scope,
                        'role': role,
                        'entity_name': entity_name,
                        'source_api': source_api,
                        'frequency': frequency,
                        'config_params': json.dumps(config_params_json),
                        'search_keywords': search_keywords
                    })

    return {
        'ddl_statements': table_statements,
        'seed_data': seed_data
    }


def create_tables_from_doc(cursor):
    """Create tables by parsing Phase4_DataModel.md document."""
    try:
        parsed_data = parse_datamodel_doc()
        ddl_statements = parsed_data['ddl_statements']

        for ddl in ddl_statements:
            cursor.execute(ddl)

        logger.info(f"Created {len(ddl_statements)} tables from document parsing")
        return True
    except Exception as e:
        logger.error(f"Failed to create tables from document: {e}")
        return False


def create_tables(cursor):
    """Create all necessary database tables."""
    # Create raw_ingestion_cache table for caching raw data
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS raw_ingestion_cache (
        request_hash TEXT PRIMARY KEY,
        catalog_key TEXT NOT NULL,
        source_api TEXT NOT NULL,
        raw_payload TEXT NOT NULL,
        inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')

    # Create timeseries_macro table for single-value macro time series
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS timeseries_macro (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        catalog_key TEXT NOT NULL,
        date DATE NOT NULL,
        value REAL,
        UNIQUE(catalog_key, date)
    )
    ''')

    # Create timeseries_micro table for OHLCV micro time series
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS timeseries_micro (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        catalog_key TEXT NOT NULL,
        date DATE NOT NULL,
        val_open REAL, val_high REAL, val_low REAL, val_close REAL, val_volume INTEGER,
        UNIQUE(catalog_key, date)
    )
    ''')

    # Create news_intel_pool table for news intelligence
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS news_intel_pool (
        fingerprint TEXT PRIMARY KEY,
        catalog_key TEXT NOT NULL,
        title TEXT NOT NULL,
        url TEXT NOT NULL,
        published_at TIMESTAMP NOT NULL,
        sentiment_score REAL,
        ai_summary TEXT
    )
    ''')

    # Create data_catalog table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS data_catalog (
        catalog_key TEXT PRIMARY KEY,
        country TEXT NOT NULL,
        scope TEXT NOT NULL,
        role TEXT NOT NULL,
        entity_name TEXT NOT NULL,
        source_api TEXT NOT NULL,
        update_frequency TEXT NOT NULL,
        config_params JSON DEFAULT '{}',
        search_keywords TEXT,
        is_active INTEGER DEFAULT 1
    )
    ''')

    # Create sync_watermarks table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS sync_watermarks (
        catalog_key TEXT PRIMARY KEY,
        last_ingested_at TIMESTAMP,
        last_cleaned_at TIMESTAMP,
        last_synced_at TIMESTAMP,
        last_meta_synced_at TIMESTAMP,
        checksum TEXT,
        FOREIGN KEY(catalog_key) REFERENCES data_catalog(catalog_key)
    )
    ''')

    logger.info("All database tables created successfully")


def seed_catalog_from_doc(cursor):
    """Seed the data_catalog and sync_watermarks tables from parsed document."""
    # Check if data already exists
    cursor.execute("SELECT COUNT(*) FROM data_catalog")
    count = cursor.fetchone()[0]
    if count > 0:
        logger.info(f"Data catalog already has {count} entries. Skipping seed.")
        return

    logger.info("Seeding data_catalog and sync_watermarks from document parsing...")

    try:
        parsed_data = parse_datamodel_doc()
        seed_data = parsed_data['seed_data']

        # Seed data_catalog with INSERT OR REPLACE for idempotency
        for item in seed_data:
            cursor.execute('''
            INSERT OR REPLACE INTO data_catalog (
                catalog_key, country, scope, role, entity_name, source_api,
                update_frequency, config_params, search_keywords, is_active
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
            ''', (
                item['catalog_key'], item['country'], item['scope'], item['role'],
                item['entity_name'], item['source_api'], item['frequency'],
                item['config_params'], item['search_keywords']
            ))

        # Seed sync_watermarks with catalog_keys (watermarks start as NULL)
        catalog_keys = [(item['catalog_key'],) for item in seed_data]
        cursor.executemany('''
        INSERT INTO sync_watermarks (catalog_key) VALUES (?)
        ''', catalog_keys)

        logger.info(f"Successfully seeded {len(seed_data)} entries into data_catalog and sync_watermarks from document.")

    except Exception as e:
        logger.error(f"Failed to seed from document: {e}")
        raise


def seed_catalog(cursor):
    """Seed the data_catalog and sync_watermarks tables with initial data."""
    # Check if data already exists
    cursor.execute("SELECT COUNT(*) FROM data_catalog")
    count = cursor.fetchone()[0]
    if count > 0:
        logger.info(f"Data catalog already has {count} entries. Skipping seed.")
        return

    logger.info("Seeding data_catalog and sync_watermarks with initial data...")

    seed_data = [
        # 1. 宏观全景 (Macro Panorama) - US
        ('METRIC_US_NET_LIQUIDITY', 'US', 'MACRO', 'J', 'US Net Liquidity', 'FRED', 'Monthly', '{"series": ["WALCL", "WTREGEN", "RRPONTSYD"]}', 'Fed, Liquidity, Balance Sheet'),
        ('METRIC_US_ISM_PMI', 'US', 'MACRO', 'J', 'ISM Manufacturing PMI', 'FRED', 'Monthly', '{"series": "IPMAN"}', 'Industrial Production, Manufacturing, Economy'),
        ('METRIC_US_CORE_PCE', 'US', 'MACRO', 'J', 'Core PCE (YoY)', 'FRED', 'Monthly', '{"series": "PCEPILFE"}', 'PCE, Inflation, Fed'),
        ('METRIC_US_10Y_YIELD', 'US', 'MACRO', 'V', 'US 10Y Treasury Yield', 'FRED', 'Monthly', '{"series": "DGS10"}', 'Yield, Treasury, Bonds'),
        ('METRIC_US_VIX', 'US', 'MACRO', 'V', 'VIX Index', 'yfinance', 'Daily', '{"ticker": "^VIX"}', 'VIX, Volatility, Fear'),

        # 2. 宏观全景 (Macro Panorama) - JP
        ('METRIC_JP_BOJ_ASSETS', 'JP', 'MACRO', 'J', 'BOJ Total Assets', 'FRED', 'Monthly', '{"series": "JPNASSETS"}', 'BOJ, Assets, QE'),
        ('METRIC_JP_TANKAN', 'JP', 'MACRO', 'J', 'Tankan Mfg Index', 'FRED', 'Quarterly', '{"series": "JPNBS6000S"}', 'Tankan, Japan, Production'),
        ('METRIC_JP_CORE_CPI', 'JP', 'MACRO', 'J', 'JP Core CPI (YoY)', 'FRED', 'Monthly', '{"series": "CPGRLE01JPM657N"}', 'CPI, Inflation, Japan'),
        ('METRIC_JP_GDP_GROWTH', 'JP', 'MACRO', 'J', 'JP GDP Growth (QoQ)', 'FRED', 'Quarterly', '{"series": "JPNRGDPQDSNAQ"}', 'GDP, Growth, Japan'),
        ('METRIC_JP_NIKKEI_VI', 'JP', 'MACRO', 'V', 'Nikkei Volatility Index', 'yfinance', 'Daily', '{"ticker": "^JNIV"}', 'Nikkei, Volatility, Japan'),

        # 3. 核心资产与选筹矩阵 (Micro Assets)
        ('INDEX_US_GSPC', 'US', 'MICRO', 'J', 'S&P 500 Index', 'yfinance', 'Daily', '{"ticker": "^GSPC"}', 'S&P 500, US Market Benchmark'),
        ('INDEX_JP_N225', 'JP', 'MICRO', 'J', 'Nikkei 225 Index', 'yfinance', 'Daily', '{"ticker": "^N225"}', 'Nikkei 225, JP Market Benchmark'),

        # 4. 商品和外汇 (Commodities & FX)
        ('ASSET_GOLD_USD', 'Global', 'MICRO', 'J', 'Gold Spot (USD)', 'yfinance', 'Daily', '{"ticker": "GC=F"}', 'Gold, Safe Haven, Metals'),
        ('ASSET_SILVER_USD', 'Global', 'MICRO', 'J', 'Silver Spot (USD)', 'yfinance', 'Daily', '{"ticker": "SI=F"}', 'Silver, Industrial Metals'),
        ('ASSET_COPPER_USD', 'Global', 'MICRO', 'J', 'Copper High Grade', 'yfinance', 'Daily', '{"ticker": "HG=F"}', 'Copper, Dr. Copper, Economy'),
        ('ASSET_PLATINUM_USD', 'Global', 'MICRO', 'J', 'Platinum Spot', 'yfinance', 'Daily', '{"ticker": "PL=F"}', 'Platinum, Precious Metal'),
        ('ASSET_PALLADIUM_USD', 'Global', 'MICRO', 'J', 'Palladium Spot', 'yfinance', 'Daily', '{"ticker": "PA=F"}', 'Palladium, Auto Catalyst'),
        ('ASSET_USDJPY', 'Global', 'MICRO', 'J', 'USD/JPY (Main Anchor)', 'yfinance', 'Daily', '{"ticker": "USDJPY=X"}', 'USD/JPY, Yen, Carry Trade'),
        ('ASSET_EURUSD', 'Global', 'MICRO', 'J', 'EUR/USD', 'yfinance', 'Daily', '{"ticker": "EURUSD=X"}', 'EUR/USD, Euro, Dollar'),
        ('ASSET_GBPUSD', 'Global', 'MICRO', 'J', 'GBP/USD', 'yfinance', 'Daily', '{"ticker": "GBPUSD=X"}', 'Pound, Sterling, Dollar'),
        ('ASSET_AUDJPY', 'Global', 'MICRO', 'J', 'AUD/JPY (Risk Proxy)', 'yfinance', 'Daily', '{"ticker": "AUDJPY=X"}', 'AUD/JPY, Risk, Commodity'),
        ('ASSET_NZDJPY', 'Global', 'MICRO', 'J', 'NZD/JPY (Carry Trade)', 'yfinance', 'Daily', '{"ticker": "NZDJPY=X"}', 'NZD/JPY, Carry, Kiwi'),
        ('ASSET_USDCNY', 'Global', 'MICRO', 'J', 'USD/CNY (CNH)', 'yfinance', 'Daily', '{"ticker": "CNY=X"}', 'USD/CNY, Yuan, China'),

        # 5. 个股 (Stocks)
        ('STOCK_PRICE_NVDA', 'US', 'MICRO', 'J', 'NVIDIA (Price)', 'yfinance', 'Daily', '{"ticker": "NVDA"}', 'NVIDIA, AI, Semiconductor'),
        ('STOCK_PRICE_MSFT', 'US', 'MICRO', 'J', 'Microsoft (Price)', 'yfinance', 'Daily', '{"ticker": "MSFT"}', 'Microsoft, Software, Cloud'),
        ('STOCK_PRICE_TSLA', 'US', 'MICRO', 'J', 'Tesla Inc (Price)', 'yfinance', 'Daily', '{"ticker": "TSLA"}', 'Tesla, EV, Autonomous'),
        ('STOCK_PRICE_8035', 'JP', 'MICRO', 'J', 'Tokyo Electron', 'yfinance', 'Daily', '{"ticker": "8035.T"}', 'TEL, Semi Equipment, Japan'),
        ('STOCK_PRICE_4063', 'JP', 'MICRO', 'J', 'Shin-Etsu Chemical', 'yfinance', 'Daily', '{"ticker": "4063.T"}', 'Shin-Etsu, Chemical, Japan'),
        ('STOCK_PRICE_4188', 'JP', 'MICRO', 'J', 'Mitsubishi Chemical', 'yfinance', 'Daily', '{"ticker": "4188.T"}', 'Mitsubishi, Materials, Japan'),
        ('STOCK_PRICE_7203', 'JP', 'MICRO', 'J', 'Toyota Motor', 'yfinance', 'Daily', '{"ticker": "7203.T"}', 'Toyota, Automotive, Japan'),
        ('STOCK_PRICE_7267', 'JP', 'MICRO', 'J', 'Honda Motor', 'yfinance', 'Daily', '{"ticker": "7267.T"}', 'Honda, Automotive, Japan'),

        # 6. 情报流种子 (Validation News)
        ('NEWS_MACRO_US_MARKETS', 'US', 'MACRO', 'V', 'US Markets News', 'RSS', 'Hourly', '{"url": "https://finance.yahoo.com/rss/markets"}', 'Fed, Powell, Yields, S&P 500'),
        ('NEWS_MACRO_US_ECON', 'US', 'MACRO', 'V', 'US Economy News', 'RSS', 'Hourly', '{"url": "https://finance.yahoo.com/rss/economy"}', 'GDP, Inflation, PCE, Jobs'),
        ('NEWS_MACRO_US_TECH', 'US', 'MACRO', 'V', 'US Tech Industry', 'RSS', 'Hourly', '{"url": "https://finance.yahoo.com/rss/category-tech"}', 'AI, Big Tech, Chips'),
        ('NEWS_MACRO_US_POLICY', 'US', 'MACRO', 'V', 'US Policy/Politics', 'RSS', 'Hourly', '{"url": "https://finance.yahoo.com/rss/politics"}', 'Congress, Tax, Regulations'),
        ('NEWS_MACRO_JP_MARKETS', 'JP', 'MACRO', 'V', 'JP Markets News', 'RSS', 'Hourly', '{"url": "https://finance.yahoo.com/rss/business"}', 'Nikkei 225, JGB, Yen, BOJ, Ueda'),

        # YFINANCE ASSET NEWS
        ('NEWS_ASSET_GOLD', 'Global', 'MICRO', 'V', 'Gold Intel', 'yfinance', 'Hourly', '{"ticker": "GC=F"}', 'Gold, Inflation Hedge, Safe Haven'),
        ('NEWS_ASSET_COPPER', 'Global', 'MICRO', 'V', 'Copper Intel', 'yfinance', 'Hourly', '{"ticker": "HG=F"}', 'Dr. Copper, Economic Growth'),
        ('NEWS_ASSET_USDJPY', 'Global', 'MICRO', 'V', 'USD/JPY Intel', 'yfinance', 'Hourly', '{"ticker": "USDJPY=X"}', 'Yen, Carry Trade, Intervention'),
        ('NEWS_ASSET_AUDJPY', 'Global', 'MICRO', 'V', 'AUD/JPY Intel', 'yfinance', 'Hourly', '{"ticker": "AUDJPY=X"}', 'Risk-on, Commodity Currency'),
        ('NEWS_STOCK_NVDA', 'US', 'MICRO', 'V', 'NVIDIA Intel', 'yfinance', 'Hourly', '{"ticker": "NVDA"}', 'GPU, Blackwell, AI'),
        ('NEWS_STOCK_MSFT', 'US', 'MICRO', 'V', 'Microsoft Intel', 'yfinance', 'Hourly', '{"ticker": "MSFT"}', 'Azure, Copilot, Cloud'),
        ('NEWS_STOCK_TSLA', 'US', 'MICRO', 'V', 'Tesla Intel', 'yfinance', 'Hourly', '{"ticker": "TSLA"}', 'EV, FSD, Elon Musk'),
        ('NEWS_STOCK_8035', 'JP', 'MICRO', 'V', 'Tokyo Electron Intel', 'yfinance', 'Hourly', '{"ticker": "8035.T"}', 'Semi, SPE, Tokyo Electron'),
        ('NEWS_STOCK_4063', 'JP', 'MICRO', 'V', 'Shin-Etsu Intel', 'yfinance', 'Hourly', '{"ticker": "4063.T"}', 'PVC, Silicon Wafer'),
        ('NEWS_STOCK_4188', 'JP', 'MICRO', 'V', 'Mitsubishi Chem Intel', 'yfinance', 'Hourly', '{"ticker": "4188.T"}', 'Chemical, LS, Materials'),
        ('NEWS_STOCK_7203', 'JP', 'MICRO', 'V', 'Toyota Intel', 'yfinance', 'Hourly', '{"ticker": "7203.T"}', 'Hybrid, EV, Toyota'),
        ('NEWS_STOCK_7267', 'JP', 'MICRO', 'V', 'Honda Intel', 'yfinance', 'Hourly', '{"ticker": "7267.T"}', 'Auto, Honda, Motorcycle'),
    ]

    # Seed data_catalog
    cursor.executemany('''
    INSERT INTO data_catalog (
        catalog_key, country, scope, role, entity_name, source_api, update_frequency, config_params, search_keywords, is_active
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
    ''', seed_data)

    # Seed sync_watermarks with catalog_keys (watermarks start as NULL)
    catalog_keys = [(row[0],) for row in seed_data]
    cursor.executemany('''
    INSERT INTO sync_watermarks (catalog_key) VALUES (?)
    ''', catalog_keys)

    logger.info(f"Successfully seeded {len(seed_data)} entries into data_catalog and sync_watermarks.")


def main():
    """Initialize SQLite database and data directory."""
    data_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'local', 'data')
    db_path = os.path.join(data_dir, 'heimdall.db')

    # Create data directory if it doesn't exist
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
        logger.info(f"Created data directory: {data_dir}")
    else:
        logger.info(f"Data directory already exists: {data_dir}")

    # Connect to database (creates it if it doesn't exist)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        # Enable WAL mode for better concurrency
        cursor.execute("PRAGMA journal_mode=WAL;")

        # Try document parsing first, fallback to hardcoded if needed
        if not create_tables_from_doc(cursor):
            logger.warning("Document parsing failed, falling back to hardcoded schema")
            create_tables(cursor)

        # Try document-based seeding first, fallback to hardcoded
        try:
            seed_catalog_from_doc(cursor)
        except Exception as e:
            logger.warning(f"Document seeding failed: {e}, falling back to hardcoded data")
            seed_catalog(cursor)

        # Always try to seed from document as well (for new assets)
        try:
            seed_catalog_from_doc(cursor)
        except Exception as e:
            logger.debug(f"Document update seeding failed: {e}")

        # Commit all changes
        conn.commit()

        logger.info(f"SQLite database initialized successfully at: {db_path}")
        print("✓ Data directory and SQLite database initialized successfully.")
        catalog_count = cursor.execute('SELECT COUNT(*) FROM data_catalog').fetchone()[0]
        watermarks_count = cursor.execute('SELECT COUNT(*) FROM sync_watermarks').fetchone()[0]
        print(f"✓ Tables created and seeded: {catalog_count} catalog entries, {watermarks_count} watermarks entries.")

    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        conn.rollback()
        print(f"✗ Error: {e}")
        exit(1)

    finally:
        conn.close()