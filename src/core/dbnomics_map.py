# src/core/dbnomics_map.py
"""DBnomics Meta Mapping Core Module.

PURE LOGIC ONLY - No I/O, no side effects, fully testable functions.

Note: All I/O operations (file reading/writing, API clients, logging)
are handled by the adapters layer. This module only contains data transformation logic.
"""

import logging
import pandas as pd
from typing import Dict, Set, List, Optional

logger = logging.getLogger(__name__)


def get_required_input_columns() -> List[str]:
    """Get list of required columns from input data for processing.

    Only 'provider_code' is needed - no other columns are used.
    This allows filtering large dataframes early to reduce memory usage.

    Returns:
        List of required column names
    """
    return ['provider_code']


def filter_input_columns(
    input_df: pd.DataFrame, 
    required_columns: Optional[List[str]] = None
) -> pd.DataFrame:
    """Filter input dataframe to only required columns.

    Args:
        input_df: Input DataFrame
        required_columns: List of columns to keep (uses get_required_input_columns() if None)

    Returns:
        Filtered DataFrame with only required columns
    """
    if required_columns is None:
        required_columns = get_required_input_columns()

    available_cols = [col for col in required_columns if col in input_df.columns]
    missing_cols = set(required_columns) - set(available_cols)

    if missing_cols:
        logger.warning(f"Missing columns in input data: {missing_cols}")

    if not available_cols:
        logger.error(f"No required columns found. Expected: {required_columns}")
        return pd.DataFrame()

    filtered_df = input_df[available_cols].copy()
    original_size = len(input_df.columns)
    filtered_size = len(available_cols)

    logger.info(f"Filtered input columns: {original_size} → {filtered_size} (keeping: {', '.join(available_cols)})")

    return filtered_df


def extract_input_provider_codes(input_df: pd.DataFrame) -> Set[str]:
    """Extract unique provider codes from input data.

    Args:
        input_df: Input DataFrame

    Returns:
        Set of unique provider codes
    """
    logger.info("Extracting unique provider codes from input data")

    if 'provider_code' not in input_df.columns:
        logger.error("'provider_code' column not found in input data")
        return set()

    provider_codes = set(input_df['provider_code'].dropna().unique())
    logger.info(f"Found {len(provider_codes)} unique provider codes in input")

    return provider_codes


def identify_new_providers(
    api_providers: Dict[str, dict], 
    input_provider_codes: Set[str]
) -> Dict[str, dict]:
    """Identify providers in API but not in input data.

    Args:
        api_providers: Dictionary of all API providers
        input_provider_codes: Set of provider codes in input data

    Returns:
        Dictionary of new providers (API - input)
    """
    api_codes = set(api_providers.keys())
    new_codes = api_codes - input_provider_codes
    new_providers = {code: api_providers[code] for code in new_codes}

    logger.info(f"Provider Analysis:")
    logger.info(f"  API total: {len(api_providers)}")
    logger.info(f"  Input total: {len(input_provider_codes)}")
    logger.info(f"  New (not in input): {len(new_providers)}")
    logger.info(f"  Existing (in input): {len(input_provider_codes)}")

    if new_providers:
        logger.info(f"New providers: {', '.join(sorted(new_codes))}")

    return new_providers


def build_dimension_lookup(input_df: pd.DataFrame) -> Dict[str, Dict[str, dict]]:
    """Build dimension lookup dictionary from input DataFrame.
    
    Args:
        input_df: Input DataFrame with dimension info
        
    Returns:
        Nested dict: {provider_code: {dataset_code: dimension_info}}
    """
    dimension_lookup = {}
    
    if input_df is None or input_df.empty:
        return dimension_lookup
        
    if 'dataset_code' not in input_df.columns:
        logger.info("Input data has no dimension info (only provider_code) - skipping dimension lookup")
        return dimension_lookup

    logger.info(f"Building dimension lookup from {len(input_df)} input records")
    
    for _, row in input_df.iterrows():
        provider = row['provider_code']
        if provider not in dimension_lookup:
            dimension_lookup[provider] = {}

        dataset_code = row['dataset_code']
        dimension_lookup[provider][dataset_code] = {
            'dimension_values': row.get('dimension_values', '[]'),
            'dimension_names': row.get('dimension_names', '[]'),
            'dim_count': row.get('dim_count', 0),
            'frequency': row.get('frequency', ''),
        }

    logger.info(f"Dimension lookup ready: {len(dimension_lookup)} providers")
    return dimension_lookup


def merge_dimension_info(
    datasets: List[Dict], 
    provider_code: str, 
    dimension_lookup: Dict[str, Dict[str, dict]]
) -> List[Dict]:
    """Merge dimension info from lookup into datasets.
    
    Args:
        datasets: List of dataset dicts
        provider_code: Provider code
        dimension_lookup: Pre-built dimension lookup
        
    Returns:
        Datasets with dimension info merged
    """
    if provider_code not in dimension_lookup:
        return datasets
        
    provider_dims = dimension_lookup[provider_code]
    
    for dataset in datasets:
        dataset_code = dataset.get('dataset_code')
        if dataset_code in provider_dims:
            dim_info = provider_dims[dataset_code]
            dataset['dimension_values'] = dim_info['dimension_values']
            dataset['dimension_names'] = dim_info['dimension_names']
            dataset['dim_count'] = dim_info['dim_count']
            if not dataset.get('frequency'):
                dataset['frequency'] = dim_info['frequency']
                
    return datasets


def create_metadata_dataframe(all_rows: List[Dict]) -> pd.DataFrame:
    """Create DataFrame from extracted metadata rows and deduplicate.
    
    Args:
        all_rows: List of dataset metadata dicts
        
    Returns:
        Deduplicated DataFrame
    """
    if not all_rows:
        return pd.DataFrame()
        
    df = pd.DataFrame(all_rows)
    
    if not df.empty and "dataset_code" in df.columns and "provider_code" in df.columns:
        original_count = len(df)
        df = df.drop_duplicates(
            subset=["dataset_code", "provider_code"],
            keep="first",
        )
        after_dedup = len(df)
        if after_dedup < original_count:
            logger.info(f"Deduplication: {original_count} → {after_dedup} (by code)")

    return df


def verify_output(output_df: pd.DataFrame, input_provider_codes: Set[str]) -> bool:
    """Verify that output contains only new providers.

    Args:
        output_df: Output DataFrame
        input_provider_codes: Set of input provider codes

    Returns:
        True if verification passed, False otherwise
    """
    logger.info("Verifying output data")

    try:
        output_providers = set(output_df['provider_code'].unique())

        logger.info(f"Output contains {len(output_providers)} unique providers")

        # Check if all output providers are new (not in input)
        overlap = output_providers & input_provider_codes
        if overlap:
            logger.warning(f"Found {len(overlap)} provider(s) that overlap with input:")
            for p in sorted(overlap):
                logger.warning(f"  - {p}")
            return False

        logger.info("✓ All output providers are new (not in input)")

        # Show statistics
        logger.info(f"✓ Total rows: {len(output_df)}")
        logger.info(f"✓ Unique providers: {output_df['provider_code'].nunique()}")

        if 'dim_count' in output_df.columns:
            logger.info(f"✓ Avg dimensions: {output_df['dim_count'].mean():.1f}")

        return True

    except Exception as e:
        logger.error(f"Error verifying output: {e}")
        return False
