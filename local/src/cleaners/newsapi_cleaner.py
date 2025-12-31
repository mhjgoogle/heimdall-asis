# filepath: local/src/cleaners/newsapi_cleaner.py

"""Cleaner for NewsAPI article data with full-text extraction."""

import hashlib
import re
from datetime import datetime
from typing import Dict, Any, List, Optional
import logging

from .base_cleaner import BaseCleaner
from local.src.adapters.web_scraper import WebScraper, PREFERRED_DOMAINS

try:
    import trafilatura
    TRAFILATURA_AVAILABLE = True
except ImportError:
    TRAFILATURA_AVAILABLE = False

# Try to import newspaper3k for full-text extraction
try:
    from newspaper import Article
    NEWSPAPER_AVAILABLE = True
except ImportError:
    NEWSPAPER_AVAILABLE = False

logger = logging.getLogger(__name__)


class NewsAPICleaner(BaseCleaner):
    """
    Clean NewsAPI article data into news_intel_pool records.
    
    Handles ArticleJSON format from NewsAPI with:
    - title
    - description (short summary)
    - content (full article text, often truncated in free tier)
    - url
    - publishedAt
    - source (name)
    
    Features:
    - Attempts full-text extraction from URL using web scraper
    - Prioritizes scraping from preferred news sources
    - Falls back to description if scraping fails
    - Extracts author and source information
    
    Outputs standardized news_intel_pool records with:
    - fingerprint (URL MD5)
    - title
    - url
    - published_at
    - body (full article content via web scraper, or description)
    - author
    - source_name
    """
    
    def __init__(self, catalog_key: str = None):
        """Initialize cleaner with web scraper."""
        super().__init__(catalog_key)
        self.scraper = WebScraper(timeout=10, retry_count=2) if TRAFILATURA_AVAILABLE else None
    
    def process(self, raw_data: Dict[str, Any], extract_body: bool = True) -> List[Dict[str, Any]]:
        """Process fetched news data - primarily focused on scraping from preferred sources."""
        if not isinstance(raw_data, dict):
            return []
        
        # Check for errors in API response
        if 'error' in raw_data:
            return []
        
        # Extract articles (currently empty from adapter, will be populated by scraping)
        articles = raw_data.get('articles', [])
        keywords = raw_data.get('search_keywords', [])
        
        if not articles and keywords:
            # If no articles from source, we would perform web scraping here
            # This would be the actual scraping implementation
            articles = self._scrape_from_preferred_sources(keywords)
        
        if not articles:
            return []
        
        cleaned = []
        for article in articles:
            try:
                title = (article.get('title') or '').strip()
                url = (article.get('url') or '').strip()
                
                if not (title and url):
                    continue
                
                # Generate fingerprints
                fingerprint = self.generate_fingerprint(url)
                title_hash = self.generate_title_hash(title)
                
                # Parse published date
                published_at = self._parse_iso_date(article.get('publishedAt', ''))
                
                # Get body content
                body = self._extract_body(article, url)
                
                # Extract author and source name
                author = (article.get('author') or '').strip() or None
                source_info = article.get('source', {}) or {}
                source_name = (source_info.get('name') or '').strip() or None
                
                cleaned.append({
                    'fingerprint': fingerprint,
                    'title_hash': title_hash,
                    'catalog_key': self.catalog_key,
                    'published_at': published_at,
                    'title': title,
                    'url': url,
                    'body': body,
                    'author': author,
                    'source_name': source_name,
                })
            except Exception:
                # Skip malformed article but continue with others
                continue
        
        return cleaned
    
    def _parse_iso_date(self, date_str: str) -> str:
        """Parse ISO 8601 date string (e.g., 2025-12-29T12:39:55Z)."""
        if not date_str:
            return None
        
        try:
            # Parse ISO format
            dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            # Return as ISO format string for database
            return dt.isoformat()
        except Exception:
            return None
    
    def _extract_body(self, article: Dict[str, Any], url: str) -> Optional[str]:
        """
        Extract complete body content from article URL using Newspaper3k.
        
        Strategy:
        1. Try Newspaper3k to extract full article text from URL
        2. Fallback to trafilatura if available
        3. Fallback to NewsAPI content field (may be truncated)
        4. Fallback to description
        """
        description = (article.get('description') or '').strip()
        content = (article.get('content') or '').strip()
        
        # Strategy 1: Try Newspaper3k for full-text extraction
        if NEWSPAPER_AVAILABLE and url:
            try:
                logger.debug(f"Extracting with Newspaper3k from {url[:60]}")
                news_article = Article(url)
                news_article.download()
                news_article.parse()
                
                body = (news_article.text or '').strip()
                if body and len(body) > 100:  # Only if we got substantial content
                    logger.debug(f"  → Got {len(body)} chars from Newspaper3k")
                    return body
                    
            except Exception as e:
                logger.debug(f"Newspaper3k extraction failed: {e}")
        
        # Strategy 2: Try trafilatura for full-text extraction
        if TRAFILATURA_AVAILABLE and url:
            try:
                logger.debug(f"Extracting with trafilatura from {url[:60]}")
                downloaded = trafilatura.fetch_url(url)
                if downloaded:
                    body = trafilatura.extract(downloaded)
                    if body and len(body) > 100:
                        logger.debug(f"  → Got {len(body)} chars from trafilatura")
                        return body
                        
            except Exception as e:
                logger.debug(f"Trafilatura extraction failed: {e}")
        
        # Strategy 3: Use NewsAPI content if available and not truncated
        if content and '[+' not in content:  # Not truncated
            if len(content) > 200:  # Substantial content
                logger.debug(f"Using NewsAPI content: {len(content)} chars")
                return content
        
        # Strategy 4: Use description as fallback
        if description:
            if len(description) > 150:  # Description is better than nothing
                logger.debug(f"Using description fallback: {len(description)} chars")
                return description
        
        # Strategy 5: Clean up truncated content and use if available
        if content:
            # Remove truncation indicators
            content = re.sub(r'\s*\[?\+\d+\s+chars?\]?$', '', content).strip()
            content = content.split(' ... [+')[0].strip() if ' ... [+' in content else content
            
            if len(content) > 100:
                logger.debug(f"Using cleaned truncated content: {len(content)} chars")
                return content
        
        logger.warning(f"Could not extract body content from {url[:60]}")
        return None
    
    def _scrape_from_preferred_sources(self, keywords: List[str]) -> List[Dict[str, Any]]:
        """
        Scrape articles from preferred news sources based on keywords.
        
        This is a placeholder for actual web scraping implementation.
        Would search preferred sources for the given keywords.
        """
        # TODO: Implement actual web scraping from preferred sources
        # For now, return empty list - actual implementation would:
        # 1. Query each preferred source with the keywords
        # 2. Extract article metadata
        # 3. Scrape full content from each article
        return []
