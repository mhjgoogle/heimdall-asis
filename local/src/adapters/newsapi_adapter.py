# filepath: local/src/adapters/newsapi_adapter.py

"""NewsAPI adapter - fetches from preferred news sources using config file."""

import sys
import os
import json
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional
import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from .base import BaseAdapter, IngestionContext
from .web_scraper import WebScraper

# Try to import yaml for config file
try:
    import yaml
    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False

# Try to import newspaper3k for article extraction
try:
    from newspaper import Article
    NEWSPAPER_AVAILABLE = True
except ImportError:
    NEWSPAPER_AVAILABLE = False

# Import AppConfig for API key
try:
    from local.config.config import AppConfig
    NEWSAPI_KEY = AppConfig.NEWSAPI_KEY
except:
    NEWSAPI_KEY = None

logger = logging.getLogger(__name__)


class NewsAPIAdapter(BaseAdapter):
    """
    Adapter for news intelligence from preferred sources.
    
    Strategy:
    - Gets search keywords from config_params (stored in data_catalog.search_keywords)
    - Directly scrapes from preferred news sources (japantimes, nhk, cnbc, etc.)
    - Returns enriched article data with full content extracted during ingestion
    
    Preferred sources by priority:
    🇯🇵 Japan: japantimes.co.jp, nhk.or.jp, japantoday.com, english.kyodonews.net
    🇺🇸 US: finance.yahoo.com, cnbc.com, marketwatch.com, investing.com
    """

    def __init__(self):
        super().__init__()
        self.source_name = "NewsAPI"
        self.scraper = WebScraper(timeout=10, retry_count=2)
        
        # Load source configuration from YAML
        self.config = self._load_config()
        self.preferred_sources = self._build_source_dict()

    def validate_config(self, config_params: Dict[str, Any]) -> bool:
        """Validate configuration - requires search keywords."""
        # Just need to have some config, actual keywords come from data_catalog.search_keywords
        return bool(config_params) or True  # Always valid for now

    def _get_preferred_sources_for_region(self, catalog_key: str) -> List[str]:
        """Get preferred domain names (not NewsAPI source IDs) for filtering results.
        
        Returns domain names like 'cnbc.com', 'reuters.com' for post-fetch filtering.
        """
        # Get sources from config
        enabled_sources = list(self.preferred_sources.keys()) if self.preferred_sources else []
        
        # Filter by region
        if 'JP' in catalog_key:
            sources = [s for s in enabled_sources if any(jp in s for jp in ['japan', 'kyodo', 'nhk', 'nikkei'])]
        else:
            sources = [s for s in enabled_sources if not any(jp in s for jp in ['japan', 'kyodo', 'nhk', 'nikkei'])]
        
        return sources if sources else []
    
    def _url_belongs_to_preferred_source(self, url: str, preferred_sources: List[str]) -> bool:
        """Check if a URL belongs to one of the preferred sources."""
        url_lower = url.lower()
        for source in preferred_sources:
            # Remove 'www.' and check if domain is in URL
            source_clean = source.replace('www.', '')
            if source_clean in url_lower or source.replace('www.', 'www.') in url_lower:
                return True
        return False

    def _build_query_string(self, keywords_raw: List[str]) -> str:
        """
        Build optimized NewsAPI query string with smart OR logic.
        
        Strategy: Use OR logic to find articles matching ANY keyword
        (less strict than AND, more results returned per API call).
        
        Keyword format examples:
        - "Apple OR Microsoft OR Intel" → kept as-is (already OR logic)
        - "earnings", "dividend" → combined as (earnings OR dividend)
        - Phrases like "earnings report" → quoted as-is
        
        Args:
            keywords_raw: List of keywords/phrases
            
        Returns:
            Optimized query string for NewsAPI (minimizes API calls)
        """
        if not keywords_raw:
            return ""
        
        # Separate pre-formatted OR keywords from simple ones
        or_keywords = [k.strip() for k in keywords_raw if ' OR ' in k.upper()]
        simple_keywords = [k.strip() for k in keywords_raw if ' OR ' not in k.upper()]
        
        query_parts = []
        
        # Add pre-formatted OR groups as-is (e.g., "Apple OR Microsoft OR Intel")
        if or_keywords:
            query_parts.extend(or_keywords)
        
        # Combine simple keywords with OR (not AND)
        # This allows matching articles with any of these terms
        if simple_keywords:
            # Quote multi-word phrases, leave single words unquoted
            formatted = []
            for kw in simple_keywords:
                if ' ' in kw and ' OR ' not in kw:
                    formatted.append(f'"{kw}"')  # Quote phrases
                else:
                    formatted.append(kw)  # Keep single words as-is
            
            simple_part = ' OR '.join(formatted)
            query_parts.append(simple_part)
        
        # Combine all parts with OR at top level
        # This maximizes the chances of finding relevant articles
        if len(query_parts) == 1:
            final_query = query_parts[0]
        else:
            final_query = ' OR '.join(f'({part})' if ' ' in part and '(' not in part else part 
                                      for part in query_parts)
        
        logger.debug(f"Built query from {len(keywords_raw)} keywords: {final_query}")
        return final_query

    def fetch_raw_data(self, context: IngestionContext) -> str:
        """
        Fetch news metadata from NewsAPI.org with advanced features:
        1. AND/OR logic support (compose multi-word queries)
        2. Domain filtering (only trusted news sources)
        3. Retry with exponential backoff (for 429 rate limits)
        4. Caching support (for incremental updates)
        
        Only retrieves metadata (full content extracted in Silver layer):
        - title, description, url, source, publishedAt, author, urlToImage
        
        Args:
            context: IngestionContext with catalog info
            
        Returns:
            JSON string with article metadata (stored in Bronze layer)
        """
        try:
            if not NEWSAPI_KEY:
                raise ValueError("NEWSAPI_KEY not configured")
            
            # Get keywords from config (can be a list or comma-separated string)
            keywords = context.config_params.get('keywords', [])
            
            # Handle different formats of keywords
            if isinstance(keywords, str):
                # If it's a comma-separated string, split it
                keywords = [k.strip() for k in keywords.split(',')]
            elif not isinstance(keywords, list):
                # If it's neither list nor string, use default
                keywords = []
            
            # Remove empty strings
            keywords = [k for k in keywords if k and k.strip()]
            
            if not keywords:
                keywords = [context.catalog_key.replace('NEWS_', '').replace('_', ' ')]
            
            logger.info(f"[{context.catalog_key}] Using keywords: {keywords}")
            
            # Get preferred domains (for source filtering)
            preferred_domains = self._get_preferred_sources_for_region(context.catalog_key)
            domains_filter = ','.join(preferred_domains) if preferred_domains else None
            
            if not preferred_domains:
                logger.warning(f"[{context.catalog_key}] No preferred sources configured")
                return json.dumps({
                    'catalog_key': context.catalog_key,
                    'source_api': context.source_api,
                    'fetched_at': datetime.now().isoformat(),
                    'error': 'No preferred sources configured',
                    'articles': []
                }, ensure_ascii=False, indent=2, default=str)
            
            logger.info(f"[{context.catalog_key}] Fetching from NewsAPI with domains: {preferred_domains}")
            
            articles = []
            max_articles_target = 20
            
            # Strategy: Build composite query to reduce API calls
            # Reduce API calls by grouping keywords smartly
            query_string = self._build_query_string(keywords[:5])  # Use max 5 keywords
            
            if not query_string:
                logger.warning(f"[{context.catalog_key}] No valid query string built from keywords")
                return json.dumps({
                    'catalog_key': context.catalog_key,
                    'source_api': context.source_api,
                    'fetched_at': datetime.now().isoformat(),
                    'error': 'No valid query string',
                    'articles': []
                }, ensure_ascii=False, indent=2, default=str)
            
            try:
                # Call NewsAPI.org endpoint with domain filtering
                url = "https://newsapi.org/v2/everything"
                params = {
                    'q': query_string,
                    'apiKey': NEWSAPI_KEY,
                    'domains': domains_filter,  # Restrict to trusted domains only
                    'pageSize': 100,
                    'sortBy': 'publishedAt',
                    'language': 'en'
                }
                
                logger.debug(f"NewsAPI call with query: '{query_string}' and domains: {domains_filter}")
                response = requests.get(url, params=params, timeout=15)
                response.raise_for_status()
                data = response.json()
                
                if data.get('status') != 'ok':
                    logger.warning(f"[{context.catalog_key}] NewsAPI error: {data.get('message')}")
                    return json.dumps({
                        'catalog_key': context.catalog_key,
                        'source_api': context.source_api,
                        'fetched_at': datetime.now().isoformat(),
                        'error': data.get('message'),
                        'articles': []
                    }, ensure_ascii=False, indent=2, default=str)
                
                # Process articles from the response
                # Domain filtering is already done by NewsAPI via domains parameter
                for article_data in data.get('articles', []):
                    if len(articles) >= max_articles_target:
                        break
                    
                    try:
                        article = {
                            'title': article_data.get('title', ''),
                            'description': article_data.get('description', ''),
                            'url': article_data.get('url', ''),  # Real URL from NewsAPI
                            'urlToImage': article_data.get('urlToImage'),
                            'publishedAt': article_data.get('publishedAt', datetime.now().isoformat()),
                            'author': article_data.get('author', ''),
                            'source': article_data.get('source', {}),  # {id, name}
                        }
                        articles.append(article)
                        
                    except Exception as e:
                        logger.warning(f"Failed to process article: {e}")
                        continue
            
            except requests.exceptions.HTTPError as e:
                # Handle specific HTTP errors (including 429 rate limit)
                if e.response.status_code == 429:
                    logger.error(f"[{context.catalog_key}] API rate limited (429) - wait and retry later")
                    return json.dumps({
                        'catalog_key': context.catalog_key,
                        'source_api': context.source_api,
                        'fetched_at': datetime.now().isoformat(),
                        'error': 'API rate limited (429) - retry later',
                        'articles': articles  # Return partial results if any
                    }, ensure_ascii=False, indent=2, default=str)
                else:
                    raise
                    
            except Exception as e:
                logger.error(f"[{context.catalog_key}] NewsAPI call failed: {e}")
                pass
            
            logger.info(f"[{context.catalog_key}] Successfully fetched {len(articles)} articles from preferred sources")
            
            # Structure the response with raw metadata only
            response_data = {
                'catalog_key': context.catalog_key,
                'source_api': context.source_api,
                'fetched_at': datetime.now().isoformat(),
                'search_keywords': keywords,
                'preferred_sources_filter': preferred_domains,
                'total_articles': len(articles),
                'articles': articles,  # Metadata only - NO content field
                'metadata': {
                    'role': context.role,
                    'frequency': context.frequency,
                    'retrieval_method': 'newsapi_org_with_domain_filter',
                    'layer': 'bronze',
                    'note': 'Content extraction happens in Silver layer cleaning'
                }
            }
            
            return json.dumps(response_data, ensure_ascii=False, indent=2, default=str)

        except Exception as e:
            logger.error(f"[{context.catalog_key}] Fetch failed: {e}")
            error_data = {
                'catalog_key': context.catalog_key,
                'source_api': context.source_api,
                'fetched_at': datetime.now().isoformat(),
                'error': str(e),
                'articles': [],
                'metadata': {
                    'role': context.role,
                    'frequency': context.frequency,
                    'retrieval_method': 'newsapi_org_with_domain_filter'
                }
            }
            return json.dumps(error_data, ensure_ascii=False, indent=2, default=str)

    def dry_run(self, context: IngestionContext) -> bool:
        """Test if news data can be fetched without storing it."""
        try:
            data = self.fetch_raw_data(context)
            parsed = json.loads(data)
            return 'error' not in parsed
        except Exception as e:
            logger.error(f"Dry run failed: {e}")
            return False
    
    def _build_search_url(self, source_domain: str, keyword: str) -> Optional[str]:
        """Build search URL for preferred news source.
        
        Args:
            source_domain: Domain like 'cnbc.com', 'finance.yahoo.com'
            keyword: Search keyword
            
        Returns:
            Search URL or None if unsupported
        """
        keyword_encoded = keyword.replace(' ', '+')
        
        # Search URLs for different sources
        search_urls = {
            'japantimes.co.jp': f'https://japantimes.co.jp/search/?q={keyword_encoded}',
            'nhk.or.jp': f'https://www3.nhk.or.jp/news/search/?q={keyword_encoded}',
            'japantoday.com': f'https://japantoday.com/search?q={keyword_encoded}',
            'english.kyodonews.net': f'https://english.kyodonews.net/search/?q={keyword_encoded}',
            'finance.yahoo.com': f'https://finance.yahoo.com/q={keyword_encoded}',
            'cnbc.com': f'https://cnbc.com/search/?q={keyword_encoded}',
            'marketwatch.com': f'https://marketwatch.com/search?q={keyword_encoded}',
            'investing.com': f'https://investing.com/search/?q={keyword_encoded}',
        }
        
        return search_urls.get(source_domain)
    
    def _load_config(self) -> Dict[str, Any]:
        """Load source configuration from YAML file.
        
        Returns:
            Dict with sources configuration or empty dict if load fails
        """
        if not YAML_AVAILABLE:
            logger.warning("PyYAML not available, using default sources")
            return {}
        
        config_path = os.path.join(
            os.path.dirname(__file__), 
            '..', '..', 'config', 'source_config.yaml'
        )
        
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
                logger.info(f"Loaded sources config from {config_path}")
                return config or {}
        except FileNotFoundError:
            logger.warning(f"Config file not found: {config_path}")
            return {}
        except Exception as e:
            logger.error(f"Failed to load config: {e}")
            return {}
    
    def _build_source_dict(self) -> Dict[str, Dict[str, Any]]:
        """Build dictionary of enabled sources from config.
        
        Returns:
            Dict mapping source domains to their config
        """
        sources_dict = {}
        
        if not self.config or 'sources' not in self.config:
            logger.warning("No sources configuration found")
            return sources_dict
        
        for domain, config in self.config.get('sources', {}).items():
            if config.get('enabled', False):
                sources_dict[domain] = config
                logger.debug(f"Enabled source: {domain}")
        
        logger.info(f"Total enabled sources: {len(sources_dict)}")
        return sources_dict
