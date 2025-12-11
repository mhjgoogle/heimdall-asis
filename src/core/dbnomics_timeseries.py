#!/usr/bin/env python3
"""DBnomics TimeSeries Extraction Core Module.

Core business logic for extracting time series data from DBnomics API
for economic indicators (CPI, PPI, CDI, PDI datasets from providers).

Pure business logic: no I/O, no side effects, fully testable.
All I/O operations are handled by infra layer and passed as parameters.
"""

from typing import List, Dict, Any, Set, Optional
import pandas as pd


def get_required_input_columns() -> List[str]:
    """Get list of required columns from input data for dbnomics_timeseries processing.
    
    Only 'flag_eco' is needed for core filtering logic.
    'provider_code' and 'dataset_code' are also needed if available.
    
    Returns:
        List of required column names
    """
    return ['flag_eco', 'provider_code', 'dataset_code']


def filter_input_columns(input_df: pd.DataFrame, required_columns: Optional[List[str]] = None) -> pd.DataFrame:
    """Filter input dataframe to only required columns for dbnomics_timeseries.
    
    Args:
        input_df: Input DataFrame
        required_columns: List of columns to keep (uses get_required_input_columns() if None)
        
    Returns:
        Filtered DataFrame with only required columns
    """
    if required_columns is None:
        required_columns = get_required_input_columns()
    
    # Check which required columns exist in the dataframe
    available_cols = [col for col in required_columns if col in input_df.columns]
    missing_cols = set(required_columns) - set(available_cols)
    
    if 'flag_eco' not in available_cols:
        return pd.DataFrame()
    
    filtered_df = input_df[available_cols].copy()
    
    return filtered_df


def extract_eco_datasets(df: pd.DataFrame) -> List[Dict[str, str]]:
    """Extract dataset codes with flag_eco=1 from input DataFrame.
    
    Filters DataFrame for rows where flag_eco=1 and extracts
    provider_code and dataset_code pairs.
    
    Args:
        df: Input DataFrame with columns: provider_code, dataset_code, flag_eco
        
    Returns:
        List of dicts with 'provider_code' and 'dataset_code' keys
        Example: [
            {'provider_code': 'IMF', 'dataset_code': 'WEO:2024-04'},
            {'provider_code': 'AMECO', 'dataset_code': 'ZUTN'},
            ...
        ]
    """
    if df.empty:
        return []
    
    if 'flag_eco' not in df.columns:
        return []
    
    # Filter for flag_eco == 1
    eco_df = df[df['flag_eco'] == 1]
    
    if eco_df.empty:
        return []
    
    # Extract provider_code and dataset_code
    datasets = []
    for _, row in eco_df.iterrows():
        try:
            dataset = {
                'provider_code': str(row.get('provider_code', '')).strip(),
                'dataset_code': str(row.get('dataset_code', '')).strip(),
            }
            if dataset['provider_code'] and dataset['dataset_code']:
                datasets.append(dataset)
        except Exception:
            continue
    
    return datasets


def extract_input_providers(input_df: pd.DataFrame) -> Set[str]:
    """Extract unique provider codes from input data for dbnomics_timeseries.
    
    Args:
        input_df: Input DataFrame
        
    Returns:
        Set of unique provider codes from eco datasets
    """
    if input_df.empty:
        return set()
    
    if 'provider_code' not in input_df.columns:
        return set()
    
    # Filter for eco datasets and extract provider codes
    eco_df = input_df[input_df['flag_eco'] == 1]
    
    if eco_df.empty:
        return set()
    
    provider_codes = set(eco_df['provider_code'].dropna().unique())
    
    return provider_codes


def deduplicate_datasets(datasets: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """Remove duplicate dataset entries.
    
    Deduplication by (provider_code, dataset_code) pair.
    
    Args:
        datasets: List of dataset dicts
        
    Returns:
        List of unique dataset dicts
    """
    if not datasets:
        return []
    
    seen = set()
    unique = []
    
    for dataset in datasets:
        key = (dataset.get('provider_code', ''), dataset.get('dataset_code', ''))
        if key not in seen and key[0] and key[1]:  # Both codes must be non-empty
            seen.add(key)
            unique.append(dataset)
    
    if len(unique) < len(datasets):
        removed = len(datasets) - len(unique)
    
    return unique


def filter_datasets_by_provider(
    datasets: List[Dict[str, str]], 
    include_providers: List[str] = None,
    exclude_providers: List[str] = None
) -> List[Dict[str, str]]:
    """Filter datasets by provider code.
    
    Apply exclude filter first, then include filter.
    Case-insensitive matching for provider codes.
    
    Args:
        datasets: List of dataset dicts
        include_providers: List of provider codes to include (if None, include all)
        exclude_providers: List of provider codes to exclude
        
    Returns:
        Filtered list of dataset dicts
    """
    if not datasets:
        return []
    
    filtered = datasets.copy()
    original_count = len(filtered)
    
    # Apply exclude filter first
    if exclude_providers:
        exclude_set = set(p.upper() for p in exclude_providers)
        filtered = [d for d in filtered 
                   if d.get('provider_code', '').upper() not in exclude_set]
        excluded_count = original_count - len(filtered)
    
    # Apply include filter
    if include_providers:
        include_set = set(p.upper() for p in include_providers)
        original_filtered = len(filtered)
        filtered = [d for d in filtered 
                   if d.get('provider_code', '').upper() in include_set]
    
    return filtered


def get_extraction_summary(datasets: List[Dict[str, str]]) -> Dict[str, Any]:
    """Generate summary statistics for eco datasets.
    
    Args:
        datasets: List of dataset dicts
        
    Returns:
        Dictionary with summary statistics including:
        - total_datasets: Total number of datasets
        - unique_providers: Number of unique providers
        - providers: Sorted list of provider codes
        - datasets_per_provider: Count of datasets for each provider
    """
    if not datasets:
        return {
            'total_datasets': 0,
            'unique_providers': 0,
            'providers': [],
            'datasets_per_provider': {}
        }
    
    providers = set(d.get('provider_code') for d in datasets if d.get('provider_code'))
    
    summary = {
        'total_datasets': len(datasets),
        'unique_providers': len(providers),
        'providers': sorted(list(providers)),
        'datasets_per_provider': {
            p: len([d for d in datasets if d.get('provider_code') == p])
            for p in sorted(providers)
        }
    }
    
    
    return summary





def build_dbnomics_api_request(
    provider_code: str,
    dataset_code: str,
    api_base_url: str,
    api_params: Dict[str, Any] = None,
) -> tuple[str, dict]:
    """Build DBnomics API request URL and parameters.
    
    Core layer responsibility: build API request parameters based on business logic.
    This is NOT an infra layer concern - infra only executes the request.
    
    Reference: https://db.nomics.world/api/v22/
    
    API request structure (correct format):
    - URL: {api_base_url}/series/{provider_code}/{dataset_code}
    - Query params: limit (optional, default=all)
    
    Args:
        provider_code: DBnomics provider code (e.g., 'AMECO', 'IMF')
        dataset_code: DBnomics dataset code (e.g., 'ZUTN', 'WEO')
        api_base_url: Base URL for DBnomics API (from Config, passed by Pipeline)
        api_params: API parameters dict from Config (limit, observations, etc.)
                   If None, uses defaults: {'limit': 1000, 'observations': True}
        
    Returns:
        Tuple of (url, params_dict) for use with api_client.fetch_dbnomics_series()
        
    Raises:
        ValueError: If provider_code or dataset_code is empty
    """
    if not provider_code or not dataset_code:
        raise ValueError("provider_code and dataset_code must not be empty")
    
    # Correct API format: /series/{provider}/{dataset}
    url = f"{api_base_url}/series/{provider_code.strip()}/{dataset_code.strip()}"
    
    # Query parameters from Config, or use defaults
    if api_params is None:
        api_params = {
            'limit': 1000,              # DBnomics API max limit is 1000 (5000+ returns 400)
            'observations': True,       # Include actual time series observation values
        }
    
    params = api_params.copy()  # Ensure we don't modify original config
    
    
    return url, params





def build_series_ids(dataset: Dict[str, str]) -> List[str]:
    """Build series IDs for a dataset.
    
    Format: provider_code/dataset_code
    Note: Full series_code requires API call to fetch specific series.
    
    Args:
        dataset: Dataset dict with provider_code and dataset_code
        
    Returns:
        List of series ID strings for API query
    """
    provider = dataset.get('provider_code', '').strip()
    dataset_code = dataset.get('dataset_code', '').strip()
    
    if not provider or not dataset_code:
        return []
    
    # Return base series ID pattern for API (actual series_codes from API response)
    series_id = f"{provider}/{dataset_code}"
    return [series_id]


def prepare_api_request(dataset: Dict[str, str]) -> Dict[str, str]:
    """Prepare API request parameters for fetching time series.
    
    Args:
        dataset: Dataset dict with provider_code and dataset_code
        
    Returns:
        Dictionary with API request parameters
    """
    if not dataset:
        return {}
    
    params = {
        'provider_code': dataset.get('provider_code', '').strip(),
        'dataset_code': dataset.get('dataset_code', '').strip(),
    }
    
    # Validate required fields
    if not params['provider_code'] or not params['dataset_code']:
        return {}
    
    return params


def format_dataset_filename(dataset: Dict[str, str]) -> str:
    """Generate filesystem-safe filename from dataset codes.
    
    Args:
        dataset: Dataset dict with provider_code and dataset_code
        
    Returns:
        Formatted filename safe for filesystem use
    """
    provider = dataset.get('provider_code', 'unknown').replace('/', '_').replace('\\', '_')
    dataset_code = dataset.get('dataset_code', 'unknown').replace('/', '_').replace('\\', '_')
    filename = f"dbnomics_timeseries_{provider}_{dataset_code}"
    
    return filename


def extract_observations_from_api_response(
    api_response: Dict[str, Any],
    provider_code: str,
    dataset_code: str,
) -> List[Dict[str, Any]]:
    """Extract time series observation data from DBnomics API response.
    
    DBnomics API v22 response structure:
    {
        "series": {
            "docs": [{"series_code": "...", "observations": [...], ...}, ...],
            "num_found": 123,
            "limit": 1000,
            "offset": 0
        }
    }
    """
    observations = []
    
    if not isinstance(api_response, dict):
        return observations
    
    if 'series' not in api_response:
        return observations
    
    series_data = api_response.get('series', {})
    
    # Handle v22 API format: series is a dict with 'docs' key containing list of series
    if isinstance(series_data, dict):
        series_list = series_data.get('docs', [])
        if not isinstance(series_list, list):
            return observations
    elif isinstance(series_data, list):
        # Fallback: handle old format where series is directly a list
        series_list = series_data
    else:
        return observations
    
    for series in series_list:
        if not isinstance(series, dict):
            continue
        
        series_code = series.get('series_code', '')
        series_name = series.get('series_name', '')
        
        # DBnomics API v22 format: period and value are arrays directly in series doc
        # {"series_code": "...", "period": ["2020", "2021"], "value": [100, 105]}
        period_list = series.get('period', [])
        value_list = series.get('value', [])
        
        if not isinstance(period_list, list) or not isinstance(value_list, list):
            continue
        
        # Zip period and value arrays to create observations
        for period, value in zip(period_list, value_list):
            obs_record = {
                'provider_code': provider_code,
                'dataset_code': dataset_code,
                'series_code': series_code,
                'series_name': series_name,
                'period': period,
                'value': value,
            }
            observations.append(obs_record)
    
    return observations

