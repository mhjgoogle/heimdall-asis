# src/adapters/dbnomics_fetcher.py
"""DBnomics API Fetcher Adapter.

Handles ALL I/O operations for DBnomics API.
Follows project architecture: Adapters = ALL I/O.
"""

import logging
import json
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

from src.adapters.api_client import APIClient

logger = logging.getLogger(__name__)


class DbnomicsFetcher:
    """Fetcher for DBnomics API data.
    
    Responsible for all HTTP I/O to DBnomics API.
    """

    def __init__(self, client: APIClient, api_base_url: str, timeout: int, max_workers: int):
        """Initialize fetcher.
        
        Args:
            client: APIClient instance
            api_base_url: DBnomics API base URL
            timeout: Request timeout in seconds
            max_workers: Max concurrent workers for parallel requests
        """
        self.client = client
        self.api_base_url = api_base_url
        self.timeout = timeout
        self.max_workers = max_workers

    def fetch_providers(self) -> Dict[str, dict]:
        """Fetch all providers from DBnomics API.
        
        Returns:
            Dictionary mapping provider_code -> provider_data
        """
        logger.info("Fetching all providers from API")

        try:
            response = self.client.get(
                f'{self.api_base_url}/providers', 
                timeout=self.timeout
            )
            api_data = response.json()

            providers_list = api_data.get('providers', {})
            if isinstance(providers_list, dict):
                providers_list = providers_list.get('docs', [])
            elif not isinstance(providers_list, list):
                providers_list = []

            providers_map = {}
            for provider in providers_list:
                code = provider.get('code')
                if code:
                    providers_map[code] = {
                        'code': code,
                        'name': provider.get('name', ''),
                        'region': provider.get('region', ''),
                        'website': provider.get('website', ''),
                    }

            logger.info(f"Fetched {len(providers_map)} providers from API")
            return providers_map

        except Exception as e:
            logger.error(f"Error fetching API providers: {e}")
            raise

    def fetch_provider_datasets(self, provider_code: str) -> List[Dict]:
        """Fetch ALL datasets for a provider.
        
        Args:
            provider_code: Provider code
            
        Returns:
            List of dataset dicts. Empty list on error (graceful failure).
        """
        try:
            logger.info(f"Fetching datasets from API for {provider_code}")

            provider_url = f"{self.api_base_url}/providers/{provider_code}"
            response = self.client.get(provider_url, timeout=self.timeout)
            api_data = response.json()
            
            category_tree = api_data.get('category_tree', [])
            current_time = datetime.utcnow().isoformat() + 'Z'

            # Extract datasets from category tree
            tree_datasets = self._extract_datasets_from_tree(category_tree)
            logger.info(f"Found {len(tree_datasets)} datasets in category tree for {provider_code}")

            # Fetch nb_series for each dataset
            batch_datasets = []
            for dataset in tree_datasets:
                dataset_code = dataset.get('code', '')
                dataset_name = dataset.get('name', dataset_code)

                nb_series = self._fetch_dataset_nb_series(provider_code, dataset_code)

                batch_datasets.append({
                    'provider_code': provider_code,
                    'dataset_code': dataset_code,
                    'dataset_name': dataset_name,
                    'nb_series': nb_series,
                    'indexed_at': current_time,
                })

            # Fetch dimensions in parallel
            if batch_datasets:
                all_datasets = self._fetch_batch_dimensions_parallel(
                    provider_code, batch_datasets
                )
            else:
                all_datasets = []

            if all_datasets:
                logger.info(f"Successfully fetched {len(all_datasets)} datasets for {provider_code}")
            else:
                logger.warning(f"No datasets returned from API for {provider_code}")

            return all_datasets

        except Exception as e:
            error_msg = str(e)
            if '404' in error_msg:
                logger.warning(f"404 error: /providers/{provider_code} not available")
            else:
                logger.warning(f"Error fetching datasets for {provider_code}: {error_msg}")
            return []

    def _extract_datasets_from_tree(self, nodes: List, parent_path: str = '') -> List[Dict]:
        """Recursively extract dataset codes from category tree."""
        datasets_found = []
        for node in nodes:
            code = node.get('code', '')
            name = node.get('name', code).strip()

            if 'children' in node:
                datasets_found.extend(
                    self._extract_datasets_from_tree(node['children'], f"{parent_path}/{code}")
                )
            elif code:
                datasets_found.append({'code': code, 'name': name})
        return datasets_found

    def _fetch_dataset_nb_series(self, provider_code: str, dataset_code: str) -> int:
        """Fetch nb_series count for a dataset."""
        try:
            dataset_url = f"{self.api_base_url}/datasets/{provider_code}/{dataset_code}"
            response = self.client.get(dataset_url, timeout=self.timeout)
            dataset_detail = response.json()

            dataset_docs = dataset_detail.get('datasets', {}).get('docs', [])
            if dataset_docs:
                return dataset_docs[0].get('nb_series', 0)
            return 0
        except Exception as e:
            logger.debug(f"Could not fetch nb_series for {provider_code}/{dataset_code}: {e}")
            return 0

    def _fetch_batch_dimensions_parallel(
        self, provider_code: str, batch_datasets: List[Dict]
    ) -> List[Dict]:
        """Fetch dimensions for a batch of datasets in parallel."""
        results = []

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(
                    self._fetch_dataset_dimensions, provider_code, d['dataset_code']
                ): d
                for d in batch_datasets
            }

            for future in as_completed(futures):
                dataset_info = futures[future]
                try:
                    dim_values, dim_names, dim_count, frequency = future.result(timeout=20)
                    dataset_info.update({
                        'dimension_values': dim_values,
                        'dimension_names': dim_names,
                        'dim_count': dim_count,
                        'frequency': frequency,
                    })
                except Exception as e:
                    logger.debug(f"Failed to fetch dimensions for {provider_code}/{dataset_info['dataset_code']}: {e}")
                    dataset_info.update({
                        'dimension_values': '{}',
                        'dimension_names': '[]',
                        'dim_count': 0,
                        'frequency': '',
                    })
                results.append(dataset_info)

        return results

    def _fetch_dataset_dimensions(
        self, provider_code: str, dataset_code: str
    ) -> Tuple[str, str, int, str]:
        """Fetch dimension information for a dataset.
        
        Returns:
            Tuple of (dimension_values_json, dimension_names_json, dim_count, frequency)
        """
        try:
            url = f"{self.api_base_url}/datasets/{provider_code}/{dataset_code}"
            response = self.client.get(url, timeout=self.timeout)
            data = response.json()
            
            dataset_info = data.get('datasets', {}).get('docs', [{}])[0]
            dimensions_codes_order = dataset_info.get('dimensions_codes_order', [])
            dimensions_values_labels = dataset_info.get('dimensions_values_labels', {})

            if not dimensions_values_labels:
                return '{}', '[]', 0, ''

            dimension_values = {}
            dimension_names = list(dimensions_values_labels.keys())
            frequency = ''

            for dim_name, codes_labels in dimensions_values_labels.items():
                codes = list(codes_labels.keys()) if isinstance(codes_labels, dict) else []

                if dim_name.lower() in ['freq', 'frequency']:
                    frequency = ','.join(codes) if codes else ''
                    continue

                dimension_values[dim_name] = codes

            # Sort by dimensions_codes_order if available
            if dimensions_codes_order:
                dimension_names = [
                    d for d in dimensions_codes_order
                    if d in dimension_values or d.lower() in ['freq', 'frequency']
                ]
                dimension_names = [
                    d for d in dimension_names 
                    if d.lower() not in ['freq', 'frequency']
                ]
                dimension_values = {
                    d: dimension_values[d] for d in dimension_names if d in dimension_values
                }
            else:
                dimension_names = sorted([
                    d for d in dimension_names 
                    if d.lower() not in ['freq', 'frequency']
                ])
                dimension_values = {d: dimension_values[d] for d in dimension_names}

            return (
                json.dumps(dimension_values),
                json.dumps(dimension_names),
                len(dimension_names),
                frequency,
            )

        except Exception as e:
            logger.debug(f"Could not fetch dimensions for {provider_code}/{dataset_code}: {e}")
            return '{}', '[]', 0, ''
