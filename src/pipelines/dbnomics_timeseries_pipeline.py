# src/pipelines/dbnomics_timeseries_pipeline.py
"""DBnomics TimeSeries Pipeline for ASIS system.

Orchestration layer: connects Adapters to Core for time series extraction.
"""

from pathlib import Path
from typing import Dict, Optional, List, Any
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, TimeoutError

import pandas as pd
import dask.dataframe as dd

from src.pipelines.base import BasePipeline
from src.adapters.config import DataPaths, Config
from src.adapters import logging as app_logging
from src.adapters.input_handler import InputHandler
from src.adapters.output_handler import OutputHandler
from src.adapters.api_client import APIClient, fetch_dbnomics_series
from src.adapters.dbnomics_fetcher import DbnomicsFetcher
from src.core.dbnomics_timeseries import (
    get_required_input_columns,
    filter_input_columns,
    extract_eco_datasets,
    extract_input_providers,
    deduplicate_datasets,
    filter_datasets_by_provider,
    get_extraction_summary,
    build_dbnomics_api_request,
    extract_observations_from_api_response,
    format_dataset_filename,
)

logger = logging.getLogger(__name__)


class DbnomicsTimeseriesPipeline(BasePipeline):
    """DBnomics TimeSeries extraction pipeline with orchestration.

    Integrates Adapters (I/O) with Core (logic) to implement timeseries extraction workflow.
    """

    def __init__(
        self,
        provider_limit: Optional[int] = None,
        dataset_limit: Optional[int] = None,
        verbose: bool = False,
        provider_timeout: int = 300,
    ):
        """Initialize DBnomics TimeSeries Pipeline.

        Args:
            provider_limit: Max number of providers to process.
            dataset_limit: Max number of datasets per provider to process.
            verbose: Enable verbose logging
            provider_timeout: Timeout for single provider processing in seconds
        """
        super().__init__(name="dbnomics_timeseries", provider_limit=provider_limit)
        self.dataset_limit = dataset_limit
        self.verbose = verbose
        self.provider_timeout = provider_timeout
        self.config = Config()
        self.data_paths = DataPaths()
        self.input_handler = InputHandler(config=self.config)
        self.output_handler = OutputHandler(config=self.config)
        self.client = APIClient()
        self.fetcher = DbnomicsFetcher(
            client=self.client,
            api_base_url=self.config.DBNOMICS_API_BASE_URL,
            timeout=self.config.REQUEST_TIMEOUT,
            max_workers=self.config.API_MAX_WORKERS,
        )

        self.logger = logging.getLogger(__name__)
        self._setup_logging()

        if verbose:
            logging.getLogger().setLevel(logging.DEBUG)

        self.eco_datasets: List[Dict[str, str]] = []
        self.extracted_observations: List[Dict[str, Any]] = []
        self.processed_data: Dict[str, Path] = {}

    def _setup_logging(self):
        """Configure logging using config settings."""
        app_logging.setup_logging(
            log_dir=self.config.LOG_DIR,
            log_prefix="timeseries_extraction",
            log_level=self.config.LOG_LEVEL,
            enable_file=self.config.ENABLE_FILE_LOGGING,
            enable_console=self.config.ENABLE_CONSOLE_LOGGING,
            log_format=self.config.LOG_FORMAT,
            log_date_format=self.config.LOG_DATE_FORMAT,
        )

    def __del__(self):
        """Cleanup APIClient session."""
        if hasattr(self, 'client') and self.client:
            self.client.close()

    def extract(self) -> Dict[str, Any]:
        """Extract phase: Read input, identify eco datasets."""
        self.logger.info("=== EXTRACT PHASE ===")

        try:
            # Step 1: Read input data using InputHandler (Adapter)
            self.logger.info("Step 1: Reading input data via InputHandler...")
            input_dfs = self.input_handler.read_meta_mapping_input_directory(use_dask=False)

            if input_dfs:
                if len(input_dfs) > 1:
                    dfs_to_concat = []
                    for df in input_dfs:
                        if isinstance(df, dd.DataFrame):
                            dfs_to_concat.append(df.compute())
                        else:
                            dfs_to_concat.append(df)
                    input_df = pd.concat(dfs_to_concat, ignore_index=True)
                else:
                    input_df = input_dfs[0]
                    if isinstance(input_df, dd.DataFrame):
                        input_df = input_df.compute()
            else:
                input_df = pd.DataFrame()

            if len(input_df) == 0:
                self.logger.warning("No input data found")
                return {}

            # Filter columns using Core logic
            input_df = filter_input_columns(input_df, required_columns=get_required_input_columns())

            # Step 2: Extract eco datasets using Core logic
            self.logger.info("Step 2: Extracting eco datasets...")
            self.eco_datasets = extract_eco_datasets(input_df)

            if not self.eco_datasets:
                self.logger.warning("No eco datasets found (flag_eco=1)")
                return {}

            # Deduplicate datasets
            self.eco_datasets = deduplicate_datasets(self.eco_datasets)

            # Apply provider limit if specified
            if self.provider_limit is not None:
                input_providers = extract_input_providers(input_df)
                if len(input_providers) > self.provider_limit:
                    limited_providers = list(input_providers)[:self.provider_limit]
                    self.eco_datasets = filter_datasets_by_provider(
                        self.eco_datasets, include_providers=limited_providers
                    )
                    self.logger.info(f"Provider limit: {len(limited_providers)} providers")

            # Apply dataset limit if specified
            if self.dataset_limit is not None and len(self.eco_datasets) > self.dataset_limit:
                self.eco_datasets = self.eco_datasets[:self.dataset_limit]
                self.logger.info(f"Dataset limit: {len(self.eco_datasets)} datasets")

            # Get summary
            summary = get_extraction_summary(self.eco_datasets)
            self.logger.info(f"Found {summary['total_datasets']} eco datasets from {summary['unique_providers']} providers")

            self.logger.info(f"Extracted {len(self.eco_datasets)} eco datasets")
            return {"datasets": self.eco_datasets}

        except Exception as e:
            self.logger.error(f"Error in extract phase: {str(e)}")
            return {}

    def process(self, input_data: Dict) -> pd.DataFrame:
        """Process phase: Fetch time series data from API."""
        self.logger.info("=== PROCESS PHASE ===")

        try:
            if not input_data or "datasets" not in input_data:
                self.logger.warning("No input datasets to process")
                return pd.DataFrame()

            datasets = input_data["datasets"]

            if not datasets:
                self.logger.warning("Empty datasets list")
                return pd.DataFrame()

            # Step 3: Fetch time series data for each dataset
            self.logger.info("Step 3: Fetching time series data...")
            all_observations = self._fetch_timeseries_with_timeout(datasets)

            if not all_observations:
                self.logger.warning("No observations extracted")
                return pd.DataFrame()

            # Create DataFrame from observations
            observations_df = pd.DataFrame(all_observations)

            # Basic data cleaning
            if not observations_df.empty:
                # Remove rows with null values
                observations_df = observations_df.dropna(subset=['value'])
                # Ensure value is numeric
                observations_df['value'] = pd.to_numeric(observations_df['value'], errors='coerce')
                observations_df = observations_df.dropna(subset=['value'])

            self.logger.info(f"Processed: {len(observations_df)} observations")
            return observations_df

        except Exception as e:
            self.logger.error(f"Error in process phase: {str(e)}")
            return pd.DataFrame()

    def _fetch_timeseries_with_timeout(self, datasets: List[Dict[str, str]]) -> List[Dict[str, Any]]:
        """Fetch time series data from datasets with timeout handling.

        Args:
            datasets: List of dataset dicts to process

        Returns:
            List of observation records
        """
        all_observations = []
        timeout_count = 0
        success_count = 0

        self.logger.info(f"Processing {len(datasets)} datasets")
        self.logger.info(f"Dataset timeout: {self.provider_timeout} seconds")

        for idx, dataset in enumerate(datasets, 1):
            provider_code = dataset.get('provider_code', 'UNKNOWN')
            dataset_code = dataset.get('dataset_code', 'UNKNOWN')

            try:
                self.logger.info(f"[{idx}/{len(datasets)}] Processing {provider_code}/{dataset_code}")

                # Use ThreadPoolExecutor for timeout
                with ThreadPoolExecutor(max_workers=1) as executor:
                    future = executor.submit(self._fetch_single_dataset, dataset)

                    try:
                        observations = future.result(timeout=self.provider_timeout)
                        if observations:
                            all_observations.extend(observations)
                            success_count += 1
                            self.logger.info(f"  ✓ {len(observations)} observations extracted")
                        else:
                            self.logger.warning(f"  ⚠ {provider_code}/{dataset_code}: No observations extracted")
                            success_count += 1

                    except TimeoutError:
                        self.logger.warning(f"  ⏱ TIMEOUT: {provider_code}/{dataset_code} exceeded {self.provider_timeout}s")
                        timeout_count += 1
                        continue

            except Exception as e:
                self.logger.error(f"  ✗ Error processing {provider_code}/{dataset_code}: {e}")

        # Summary
        self.logger.info("=" * 60)
        self.logger.info("TIMESERIES EXTRACTION SUMMARY:")
        self.logger.info(f"  Succeeded: {success_count}/{len(datasets)}")
        self.logger.info(f"  Timeout:   {timeout_count}/{len(datasets)}")
        self.logger.info(f"  Total observations: {len(all_observations)}")
        self.logger.info("=" * 60)

        return all_observations

    def _fetch_single_dataset(self, dataset: Dict[str, str]) -> List[Dict[str, Any]]:
        """Fetch time series data for a single dataset.

        Args:
            dataset: Dataset dict with provider_code and dataset_code

        Returns:
            List of observation records
        """
        provider_code = dataset.get('provider_code', '')
        dataset_code = dataset.get('dataset_code', '')

        if not provider_code or not dataset_code:
            return []

        try:
            # Build API request using Core logic
            url, params = build_dbnomics_api_request(
                provider_code=provider_code,
                dataset_code=dataset_code,
                api_base_url=self.config.DBNOMICS_API_BASE_URL,
                api_params=self.config.DBNOMICS_API_PARAMS,
            )

            # Fetch data using Adapter
            response = fetch_dbnomics_series(self.client, url, params, self.config.REQUEST_TIMEOUT)

            if not response:
                return []

            # Extract observations using Core logic
            observations = extract_observations_from_api_response(
                response, provider_code, dataset_code
            )

            return observations

        except Exception as e:
            self.logger.error(f"Error fetching {provider_code}/{dataset_code}: {e}")
            return []

    def export(self, processed_data: pd.DataFrame) -> Dict[str, Path]:
        """Export phase: Save processed time series data."""
        self.logger.info("=== EXPORT PHASE ===")

        try:
            if processed_data.empty:
                self.logger.warning("No data to export")
                return {}

            self.logger.info(f"Exporting: {len(processed_data)} observations")

            # Save to Timeseries layer in DuckDB
            success = self.output_handler.save_timeseries_to_duckdb(processed_data)
            if success:
                self.logger.info("Saved time series data to DuckDB")
            else:
                self.logger.warning("Failed to save to DuckDB")

            # Also save to file for backward compatibility
            export_path = self.output_handler.save_timeseries(
                processed_data,
                prefix="timeseries_data",
                format="parquet",
            )

            self.logger.info(f"Exported to: {export_path.name}")
            return {"timeseries": export_path}

        except Exception as e:
            self.logger.error(f"Error in export phase: {str(e)}")
            return {}

    def verify(self) -> bool:
        """Verify output using Core verification function."""
        self.logger.info("=== VERIFICATION PHASE ===")

        try:
            if not self.processed_data:
                self.logger.warning("No processed data to verify")
                return False

            # Basic verification: check if we have data
            if len(self.extracted_observations) == 0:
                self.logger.warning("No observations extracted")
                return False

            # Optimize DuckDB after successful verification
            self.output_handler.optimize_duckdb()

            self.logger.info("✓ Verification passed")
            return True

        except Exception as e:
            self.logger.error(f"Error in verification: {str(e)}")
            return False

    def get_summary(self) -> Dict[str, Any]:
        """Get pipeline execution summary."""
        return {
            "eco_datasets": len(self.eco_datasets),
            "extracted_observations": len(self.extracted_observations),
            "processed_files": len(self.processed_data),
        }


def run(
    provider_limit: Optional[int] = None,
    dataset_limit: Optional[int] = None,
    verbose: bool = False,
    provider_timeout: int = 300,
) -> Dict[str, Path]:
    """Convenience function to run DBnomics TimeSeries Pipeline."""
    config = Config()

    pipeline = DbnomicsTimeseriesPipeline(
        provider_limit=provider_limit,
        dataset_limit=dataset_limit,
        verbose=verbose,
        provider_timeout=provider_timeout,
    )
    results = pipeline.run()

    if config.ENABLE_LOG_SUMMARY:
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            summary_log_file = config.LOG_DIR / f"{timestamp}_timeseries_summary.log"
            app_logging.save_logs(summary_log_file)
            print(f"\n✓ Log summary saved to: {summary_log_file}")
        except Exception as e:
            logger.error(f"Failed to save log summary: {e}")

    return results