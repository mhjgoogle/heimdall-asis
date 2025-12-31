# filepath: local/src/cleaners/rss_cleaner.py

import xml.etree.ElementTree as ET
import json
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import re
import html
from urllib.parse import parse_qs, urlparse

from .base_cleaner import BaseCleaner

from typing import Dict, Any, List, Union, Optional

from datetime import datetime

try:
    import trafilatura
    HAS_TRAFILATURA = True
except ImportError:
    HAS_TRAFILATURA = False
    trafilatura = None

try:
    from bs4 import BeautifulSoup
    HAS_BEAUTIFULSOUP = True
except ImportError:
    HAS_BEAUTIFULSOUP = False
    BeautifulSoup = None

class RssCleaner(BaseCleaner):
    """
    Clean RSS feed data with full-text extraction.
    
    Handles both:
    - Raw XML string (parses to extract items)
    - Pre-parsed items (from adapters that already parse XML)
    
    Outputs standardized news_intel_pool records with:
    - fingerprint (URL MD5)
    - title_hash (16-char title hash for dedup)
    - published_at (ISO timestamp)
    - body (Full text extracted via trafilatura)
    
    Features:
    - Parallel URL fetching for body extraction
    - Timeout handling for unresponsive sources
    - Fallback to empty body if extraction fails
    """
    
    # Class-level thread pool and lock for body extraction
    _url_fetch_pool: Optional[ThreadPoolExecutor] = None
    _pool_lock = Lock()
    
    # Configuration
    FETCH_TIMEOUT = 10  # seconds
    MAX_WORKERS = 4  # parallel threads for body extraction
    FETCH_RETRIES = 2

    def __init__(self, catalog_key: str):
        """Initialize RssCleaner with optional body extraction."""
        super().__init__(catalog_key)
        self._init_fetch_pool()
    
    @classmethod
    def _init_fetch_pool(cls):
        """Initialize thread pool for parallel fetching."""
        if cls._url_fetch_pool is None:
            with cls._pool_lock:
                if cls._url_fetch_pool is None:
                    cls._url_fetch_pool = ThreadPoolExecutor(
                        max_workers=cls.MAX_WORKERS,
                        thread_name_prefix="rss-body-fetch"
                    )

    def process(self, raw_data: Union[str, Dict, List], extract_body: bool = True) -> List[Dict[str, Any]]:
        """
        Process RSS raw data with optional body extraction.
        
        Args:
            raw_data: Either XML string, dict with 'parsed_items', or list of items
            extract_body: Whether to fetch and extract article body text
            
        Returns:
            List of cleaned dicts for news_intel_pool with body field populated if extract_body=True
        """
        # If dict with 'parsed_items', use that
        if isinstance(raw_data, dict):
            if 'parsed_items' in raw_data:
                cleaned = self._process_parsed_items(raw_data['parsed_items'])
            # If dict looks like a single item, wrap in list
            elif 'title' in raw_data:
                cleaned = self._process_parsed_items([raw_data])
            else:
                return []
        # If list of items, process directly
        elif isinstance(raw_data, list):
            cleaned = self._process_parsed_items(raw_data)
        # If string, parse as XML
        elif isinstance(raw_data, str):
            cleaned = self._process_xml(raw_data)
        else:
            return []
        
        # Extract body text if requested and trafilatura is available
        if extract_body and HAS_TRAFILATURA and cleaned:
            cleaned = self._extract_bodies_parallel(cleaned)
        
        return cleaned

    def _process_parsed_items(self, items: List[Dict]) -> List[Dict[str, Any]]:
        """Process pre-parsed items (from adapter)."""
        cleaned = []

        for item in items:
            try:
                title = item.get('title') or ''
                link = item.get('link') or ''
                pubdate = item.get('published_at') or item.get('raw_pub_date') or ''
                description = item.get('description') or ''  # 保存描述用于 fallback

                if not (title and link):
                    continue

                # Generate fingerprints
                fingerprint = item.get('fingerprint') or self.generate_fingerprint(link)
                title_hash = item.get('title_hash') or self.generate_title_hash(title)

                # Parse published_at
                published_at = self._parse_iso_date(pubdate)

                cleaned.append({
                    'fingerprint': fingerprint,
                    'title_hash': title_hash,
                    'catalog_key': self.catalog_key,
                    'published_at': published_at,
                    'title': title,
                    'url': link,
                    'body': None,  # Will be populated by _extract_bodies_parallel
                    'description': description,  # Store for fallback
                })
            except Exception as e:
                # Skip invalid item but continue processing others
                continue

        return cleaned

    def _process_xml(self, raw_xml: str) -> List[Dict[str, Any]]:
        """Parse XML string and extract items."""
        try:
            root = ET.fromstring(raw_xml)
            items = []

            for item in root.findall('.//item'):
                try:
                    title_elem = item.find('title')
                    link_elem = item.find('link')
                    pubdate_elem = item.find('pubDate')

                    title = title_elem.text if title_elem is not None else ''
                    link = link_elem.text if link_elem is not None else ''
                    pubdate = pubdate_elem.text if pubdate_elem is not None else ''

                    if not (title and link):
                        continue

                    fingerprint = self.generate_fingerprint(link)
                    title_hash = self.generate_title_hash(title)
                    published_at = self._parse_rss_date(pubdate)

                    items.append({
                        'fingerprint': fingerprint,
                        'title_hash': title_hash,
                        'catalog_key': self.catalog_key,
                        'published_at': published_at,
                        'title': title,
                        'url': link,
                        'body': None,  # Will be populated by _extract_bodies_parallel
                        'sentiment_score': None,
                        'ai_summary': None
                    })
                except Exception:
                    # Skip malformed item in XML but continue with others
                    continue

            return items

        except ET.ParseError:
            return []
        except Exception:
            return []

    def _parse_iso_date(self, date_str: str) -> str:
        """Parse ISO format date (from pre-parsed items)."""
        if not date_str:
            return datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        
        try:
            # Handle ISO format with Z or +00:00
            if isinstance(date_str, str):
                dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                return dt.strftime('%Y-%m-%d %H:%M:%S')
        except (ValueError, AttributeError):
            pass
        
        return datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

    def _parse_rss_date(self, pubdate: str) -> str:
        """Parse RSS pubDate format (RFC 2822 style)."""
        if not pubdate:
            return datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        
        try:
            # Try common RSS format: 'Wed, 21 Oct 2015 07:28:00 GMT'
            dt = datetime.strptime(pubdate, '%a, %d %b %Y %H:%M:%S %Z')
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        except ValueError:
            # Try without timezone
            try:
                dt = datetime.strptime(pubdate, '%a, %d %b %Y %H:%M:%S')
                return dt.strftime('%Y-%m-%d %H:%M:%S')
            except ValueError:
                # Try ISO format
                try:
                    dt = datetime.fromisoformat(pubdate.replace('Z', '+00:00'))
                    return dt.strftime('%Y-%m-%d %H:%M:%S')
                except ValueError:
                    pass
        
        return datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

    def _extract_bodies_parallel(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Extract article body text from URLs in parallel.
        
        Uses thread pool to fetch multiple URLs concurrently.
        For Google News URLs that fail extraction, fallback to RSS description.
        Updates items in-place with 'body' field populated.
        """
        if not HAS_TRAFILATURA or not items:
            return items
        
        # Create mapping of url to item index for result tracking
        url_to_indices = {}
        for idx, item in enumerate(items):
            url = item.get('url', '')
            if url:
                if url not in url_to_indices:
                    url_to_indices[url] = []
                url_to_indices[url].append(idx)
        
        # Submit fetch tasks for unique URLs
        future_to_url = {}
        for url in url_to_indices.keys():
            future = self._url_fetch_pool.submit(self._fetch_and_extract, url)
            future_to_url[future] = url
        
        # Collect results as they complete
        url_to_body = {}
        for future in as_completed(future_to_url):
            url = future_to_url[future]
            try:
                body = future.result()
                url_to_body[url] = body
            except Exception as e:
                # Log error but continue
                url_to_body[url] = None
        
        # Update items with extracted bodies, with fallback to description for Google News
        for url, indices in url_to_indices.items():
            body = url_to_body.get(url)
            for idx in indices:
                # If extraction failed and it's a Google News URL, use cleaned description
                if not body and 'news.google.com' in url:
                    description = items[idx].get('description', '')
                    if description:
                        # Clean HTML tags and entities from description
                        clean_desc = re.sub(r'<[^>]+>', '', description)  # Remove HTML tags
                        clean_desc = html.unescape(clean_desc)  # Decode HTML entities
                        clean_desc = ' '.join(clean_desc.split())  # Normalize whitespace
                        items[idx]['body'] = clean_desc if len(clean_desc.strip()) > 20 else None
                    else:
                        items[idx]['body'] = None
                else:
                    items[idx]['body'] = body
                
                # Remove temp description field
                if 'description' in items[idx]:
                    del items[idx]['description']
        
        return items

    def _extract_google_news_article(self, gn_url: str) -> Optional[str]:
        """
        Try to extract content from Google News article URL.
        Google News URLs are aggregation pages - try to find real article content.
        
        Strategy:
        1. Try direct extraction from the Google News page
        2. If fails, parse HTML to find real article links
        3. Try fetching from those real article URLs
        4. Return longest text found
        """
        if not HAS_TRAFILATURA:
            return None
        
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept-Language': 'en-US,en;q=0.9',
            }
            
            response = requests.get(gn_url, timeout=5, headers=headers, allow_redirects=True)
            response.raise_for_status()
            
            # Try direct extraction first
            body = trafilatura.extract(
                response.text,
                favor_precision=False,  # Less strict to capture more content
                include_comments=False,
                with_metadata=False,
                output_format='txt'
            )
            
            if body and len(body.strip()) > 100:  # If we got meaningful content
                return body
            
            # If direct extraction failed, try to find real article links
            if HAS_BEAUTIFULSOUP:
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Try to find article links - Google News links often look like:
                # href="/articles/..." or contain full URLs
                real_article_links = set()
                
                for link in soup.find_all('a', href=True):
                    href = link.get('href', '')
                    
                    # Skip google domain links (except redirect links)
                    if 'google' in href.lower() and not '/url?' in href:
                        continue
                    
                    # Collect potential article URLs
                    if ('http' in href) or (href.startswith('/articles')):
                        real_article_links.add(href)
                
                # Try to extract from found links
                best_body = ""
                for article_url in list(real_article_links)[:5]:  # Try first 5 links
                    if article_url.startswith('/'):
                        article_url = 'https://news.google.com' + article_url
                    
                    try:
                        article_response = requests.get(article_url, timeout=3, headers=headers)
                        article_response.raise_for_status()
                        
                        article_body = trafilatura.extract(
                            article_response.text,
                            favor_precision=False,
                            include_comments=False,
                            with_metadata=False,
                            output_format='txt'
                        )
                        
                        # Keep the longest body found
                        if article_body and len(article_body) > len(best_body):
                            best_body = article_body
                            
                            # If we found good content (>500 chars), return it
                            if len(best_body) > 500:
                                return best_body
                    except Exception:
                        continue
                
                if best_body and len(best_body.strip()) > 100:
                    return best_body
            
            return None
            
        except Exception:
            return None

    def _fetch_and_extract(self, url: str, retries: int = FETCH_RETRIES) -> Optional[str]:
        """
        Fetch URL and extract article body using trafilatura.
        
        For Google News URLs, attempts to extract real article from aggregation page.
        For regular URLs, uses standard trafilatura extraction with optimized settings.
        
        Args:
            url: Article URL to fetch
            retries: Number of retry attempts
            
        Returns:
            Extracted body text or None if extraction fails
        """
        if not url or not HAS_TRAFILATURA:
            return None
        
        # Special handling for Google News URLs
        if 'news.google.com' in url:
            return self._extract_google_news_article(url)
        
        for attempt in range(retries):
            try:
                # Enhanced headers for better compatibility
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'DNT': '1',
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1',
                    'Sec-Fetch-Dest': 'document',
                    'Sec-Fetch-Mode': 'navigate',
                    'Sec-Fetch-Site': 'none',
                    'Cache-Control': 'max-age=0',
                }
                
                # Fetch URL with timeout
                response = requests.get(
                    url,
                    timeout=self.FETCH_TIMEOUT,
                    headers=headers,
                    allow_redirects=True
                )
                response.raise_for_status()
                
                # Extract body using trafilatura with optimal settings
                body = trafilatura.extract(
                    response.text,
                    favor_precision=True,
                    include_comments=False,
                    with_metadata=False,
                    output_format='txt'
                )
                
                # Return extracted body (may be None if extraction failed)
                return body
                
            except requests.Timeout:
                # Timeout - try again
                continue
            except requests.RequestException:
                # Other request errors - fail quietly
                return None
            except Exception:
                # Other extraction errors - fail quietly
                return None
        
        return None