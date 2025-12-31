"""Web scraper for news articles from preferred sources."""

import logging
import time
from typing import Optional, Dict, Any
from urllib.parse import urlparse

try:
    import trafilatura
    TRAFILATURA_AVAILABLE = True
except ImportError:
    TRAFILATURA_AVAILABLE = False

logger = logging.getLogger(__name__)

# Preferred news sources for scraping (in priority order)
PREFERRED_SOURCES = {
    'japan': [
        'japantimes.co.jp',
        'nhk.or.jp',
        'japantoday.com',
        'english.kyodonews.net',
    ],
    'us': [
        'finance.yahoo.com',
        'cnbc.com',
        'marketwatch.com',
        'investing.com',
    ],
}

PREFERRED_DOMAINS = []
for sources in PREFERRED_SOURCES.values():
    PREFERRED_DOMAINS.extend(sources)


class WebScraper:
    """Scrape article content from URLs with domain priority."""
    
    def __init__(self, timeout: int = 10, retry_count: int = 2):
        """Initialize scraper.
        
        Args:
            timeout: Request timeout in seconds
            retry_count: Number of retries for failed requests
        """
        self.timeout = timeout
        self.retry_count = retry_count
        
        if not TRAFILATURA_AVAILABLE:
            logger.warning("trafilatura not available - web scraping disabled")
    
    def scrape(self, url: str, preferred_domain_weight: bool = True) -> Optional[str]:
        """Scrape article content from URL.
        
        Args:
            url: Article URL to scrape
            preferred_domain_weight: If True, prefer scraping from preferred domains
            
        Returns:
            Extracted article text or None if scraping failed
        """
        if not TRAFILATURA_AVAILABLE or not url:
            return None
        
        # Check domain priority
        domain = self._extract_domain(url)
        is_preferred = domain in PREFERRED_DOMAINS
        
        if not is_preferred and preferred_domain_weight:
            # Lower priority for non-preferred domains - try once, don't retry
            return self._extract_with_retry(url, retry_count=1)
        
        # Preferred domain or no weight preference - use full retry
        return self._extract_with_retry(url, retry_count=self.retry_count)
    
    def _extract_with_retry(self, url: str, retry_count: int = 2) -> Optional[str]:
        """Extract content from URL with retry logic.
        
        Args:
            url: Article URL
            retry_count: Number of retries
            
        Returns:
            Extracted content or None
        """
        for attempt in range(retry_count):
            try:
                # Download HTML
                downloaded = trafilatura.fetch_url(url, timeout=self.timeout)
                if not downloaded:
                    if attempt < retry_count - 1:
                        logger.debug(f"Retry {attempt + 1}/{retry_count}: {url[:50]}...")
                        time.sleep(1)  # Wait before retry
                    continue
                
                # Extract main content
                extracted = trafilatura.extract(
                    downloaded,
                    include_comments=False,
                    target_language='en'
                )
                
                if extracted:
                    # Normalize whitespace
                    extracted = ' '.join(extracted.split())
                    
                    # Limit to max 5000 chars
                    if len(extracted) > 5000:
                        extracted = extracted[:5000]
                    
                    return extracted if extracted.strip() else None
                    
            except Exception as e:
                if attempt == retry_count - 1:
                    logger.debug(f"Scrape failed after {retry_count} attempts: {type(e).__name__}")
                else:
                    time.sleep(1)  # Wait before retry
        
        return None
    
    @staticmethod
    def _extract_domain(url: str) -> str:
        """Extract domain from URL.
        
        Args:
            url: Full URL
            
        Returns:
            Domain name (e.g., 'finance.yahoo.com')
        """
        try:
            parsed = urlparse(url)
            domain = parsed.netloc.lower()
            # Remove 'www.' prefix if present
            if domain.startswith('www.'):
                domain = domain[4:]
            return domain
        except Exception:
            return ''
    
    @staticmethod
    def get_source_priority(url: str) -> int:
        """Get priority score for URL (higher = better).
        
        Args:
            url: Article URL
            
        Returns:
            Priority score (0-100)
        """
        domain = WebScraper._extract_domain(url)
        
        # Check if in preferred list
        for region, sources in PREFERRED_SOURCES.items():
            if domain in sources:
                # Japan sources get priority 100, US sources get 80
                return 100 if region == 'japan' else 80
        
        # Other domains get 50
        return 50
