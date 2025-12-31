# filepath: local/src/adapters/rss_adapter.py

"""RSS feed adapter for news intelligence with XML parsing and deduplication."""

import sys
import os
import json
import hashlib
from datetime import datetime
from typing import Dict, Any, List
from xml.etree import ElementTree as ET

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from .base import BaseAdapter, IngestionContext
from .http_client import RetryableHTTPClient
from local.config import AppConfig


class RSSAdapter(BaseAdapter):
    """Adapter for RSS feed data with XML parsing and deduplication."""

    def __init__(self):
        super().__init__()
        self.source_name = "RSS"
        self.request_timeout = getattr(AppConfig, 'RSS_REQUEST_TIMEOUT', 30)
        self.http_client = RetryableHTTPClient(timeout=self.request_timeout)

    def validate_config(self, config_params: Dict[str, Any]) -> bool:
        """Validate RSS-specific configuration parameters."""
        if 'url' not in config_params:
            return False

        url = config_params['url']
        if not isinstance(url, str) or not url.strip():
            return False

        # Basic URL validation
        if not (url.startswith('http://') or url.startswith('https://')):
            return False

        return True

    def fetch_raw_data(self, context: IngestionContext) -> str:
        """Fetch and parse RSS data, return as JSON string for Bronze Layer."""
        if not self.validate_config(context.config_params):
            raise ValueError(f"Invalid RSS config for {context.catalog_key}: {context.config_params}")

        url = context.config_params['url']

        try:
            # Fetch RSS feed
            raw_xml = self._fetch_rss_feed(url)

            # Parse XML and extract news items
            news_items = self._parse_rss_xml(raw_xml)

            # Apply deduplication and filtering based on context
            filtered_items = self._filter_news_items(news_items, context)

            # Structure the response
            response_data = {
                'catalog_key': context.catalog_key,
                'source_api': context.source_api,
                'rss_url': url,
                'fetched_at': datetime.now().isoformat(),
                'raw_xml_length': len(raw_xml),
                'parsed_items': filtered_items,
                'metadata': {
                    'role': context.role,
                    'frequency': context.frequency,
                    'total_items': len(news_items),
                    'filtered_items': len(filtered_items)
                }
            }

            return json.dumps(response_data, ensure_ascii=False, indent=2, default=str)

        except Exception as e:
            # Return error information in structured format
            error_data = {
                'catalog_key': context.catalog_key,
                'source_api': context.source_api,
                'rss_url': url,
                'fetched_at': datetime.now().isoformat(),
                'error': str(e),
                'parsed_items': [],
                'metadata': {
                    'role': context.role,
                    'frequency': context.frequency,
                    'total_items': 0,
                    'filtered_items': 0
                }
            }
            return json.dumps(error_data, ensure_ascii=False, indent=2, default=str)

    def dry_run(self, context: IngestionContext) -> bool:
        """Test if RSS data can be fetched without storing it."""
        try:
            data = self.fetch_raw_data(context)
            parsed = json.loads(data)
            parsed_items = parsed.get('parsed_items', [])
            return len(parsed_items) > 0
        except Exception:
            return False

    def _fetch_rss_feed(self, url: str) -> str:
        """Fetch RSS feed with browser-like headers and exponential backoff retry."""
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/rss+xml, application/xml, text/xml, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache'
        }

        response = self.http_client.get(url, headers=headers)
        response.raise_for_status()

        return response.text

    def _parse_rss_xml(self, xml_content: str) -> List[Dict[str, Any]]:
        """Parse RSS XML and extract news items.

        Supports multiple formats:
        - Standard RSS 2.0 (<item> elements)
        - Atom 1.0 (<entry> elements)
        - Google News RSS (Atom format with news:news_item)
        """
        try:
            root = ET.fromstring(xml_content)
        except ET.ParseError as e:
            raise Exception(f"XML parsing error: {e}")

        news_items = []

        # Google News uses Atom format, so check both RSS 2.0 and Atom
        # Priority: Atom entries (Google News, modern feeds) -> RSS items (legacy feeds)
        items = root.findall('.//{http://www.w3.org/2005/Atom}entry')
        if not items:
            items = root.findall('.//item')

        for item in items:
            try:
                news_item = self._extract_item_data(item)
                if news_item:
                    news_items.append(news_item)
            except Exception as e:
                # Log but continue processing other items
                continue

        return news_items

    def _extract_item_data(self, item_element) -> Dict[str, Any]:
        """Extract data from a single RSS item/entry."""
        # Handle both RSS 2.0 and Atom formats
        namespaces = {
            'atom': 'http://www.w3.org/2005/Atom',
            'dc': 'http://purl.org/dc/elements/1.1/',
            'content': 'http://purl.org/rss/1.0/modules/content/'
        }

        # Extract title
        title = self._get_text(item_element, ['title', 'atom:title'])
        if not title:
            return None

        # Extract link/URL
        link = self._get_text(item_element, ['link', 'atom:link/@href', 'guid'])
        if not link and item_element.find('atom:link') is not None:
            link = item_element.find('atom:link').get('href')

        # Extract description/summary
        description = self._get_text(item_element, ['description', 'atom:summary', 'content:encoded'])
        if not description and item_element.find('atom:content') is not None:
            description = item_element.find('atom:content').text

        # Extract publication date
        pub_date = self._get_text(item_element, ['pubDate', 'atom:published', 'dc:date'])
        published_at = self._parse_pub_date(pub_date)

        # Generate fingerprint for deduplication
        url_for_hash = link or title or ""
        fingerprint = hashlib.md5(url_for_hash.encode('utf-8')).hexdigest()

        # Extract additional metadata
        author = self._get_text(item_element, ['author', 'dc:creator', 'atom:author/atom:name'])
        category = self._get_text(item_element, ['category', 'atom:category/@term'])

        return {
            'fingerprint': fingerprint,
            'title': title.strip() if title else "",
            'link': link.strip() if link else "",
            'description': (description or "").strip(),
            'published_at': published_at,
            'author': (author or "").strip(),
            'category': (category or "").strip(),
            'raw_pub_date': pub_date
        }

    def _get_text(self, element, xpath_list: List[str]) -> str:
        """Get text content from element using multiple xpath attempts."""
        for xpath in xpath_list:
            try:
                if '@' in xpath:  # Attribute
                    attr_path, attr_name = xpath.split('/@')
                    found = element.find(attr_path)
                    if found is not None:
                        return found.get(attr_name, "")
                else:  # Element text
                    found = element.find(xpath)
                    if found is not None and found.text:
                        return found.text
            except:
                continue
        return ""

    def _parse_pub_date(self, pub_date_str: str) -> str:
        """Parse publication date string to ISO format."""
        if not pub_date_str:
            return None

        try:
            # Common RSS date formats
            from email.utils import parsedate_to_datetime
            dt = parsedate_to_datetime(pub_date_str.strip())
            return dt.isoformat()
        except:
            try:
                # Fallback to dateutil
                from dateutil import parser
                dt = parser.parse(pub_date_str.strip())
                return dt.isoformat()
            except:
                return None

    def _filter_news_items(self, news_items: List[Dict[str, Any]], context: IngestionContext) -> List[Dict[str, Any]]:
        """Filter news items based on context and deduplication rules."""
        filtered = []

        # For VALIDATION role (news), be more aggressive with recent content
        if context.role == "VALIDATION":
            # Get items from last 24 hours (more frequent updates)
            cutoff_hours = 24
        else:
            # For JUDGMENT role, be more conservative
            cutoff_hours = 168  # 1 week

        cutoff_datetime = datetime.now().timestamp() - (cutoff_hours * 3600)

        for item in news_items:
            # Filter by publication date if available
            if item.get('published_at'):
                try:
                    from dateutil import parser
                    item_dt = parser.parse(item['published_at'])
                    if item_dt.timestamp() < cutoff_datetime:
                        continue  # Too old
                except:
                    pass  # If date parsing fails, include the item

            # Basic quality filters
            if not item.get('title', '').strip():
                continue  # No title

            if not item.get('link', '').strip():
                continue  # No link

            # Length check (avoid very short or very long titles)
            title_len = len(item['title'])
            if title_len < 10 or title_len > 200:
                continue

            filtered.append(item)

        return filtered[:50]  # Limit to 50 most recent items