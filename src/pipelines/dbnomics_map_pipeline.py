# src/pipelines/dbnomics_map_pipeline.py
"""DBnomics Map Pipeline for ASIS system.

Orchestration layer: connects Adapters to Core.
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
from src.adapters.api_client import APIClient
from src.adapters.dbnomics_fetcher import DbnomicsFetcher
from src.core.dbnomics_map import (
    extract_input_provider_codes,
    identify_new_providers,
    get_required_input_columns,
    filter_input_columns,
    build_dimension_lookup,
    merge_dimension_info,
    create_metadata_dataframe,
    verify_output,
)

logger = logging.getLogger(__name__)


class DbnomicsMapPipeline(BasePipeline):
    """DBnomics metadata mapping pipeline with orchestration.

    Integrates Adapters (I/O) with Core (logic) to implement meta_mapping workflow.
    """

    def __init__(
        self,
        provider_limit: Optional[int] = None,
        verbose: bool = False,
        provider_timeout: int = 600,
    ):
        """Initialize DBnomics Map Pipeline.

        Args:
            provider_limit: Max number of providers to process.
            verbose: Enable verbose logging
            provider_timeout: Timeout for single provider processing in seconds
        """
        super().__init__(name="dbnomics_map", provider_limit=provider_limit)
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

        self.input_providers: set = set()
        self.api_providers: dict = {}
        self.new_providers: dict = {}
        self.extracted_rows: List[Dict[str, Any]] = []
        self.extracted_data: Dict[str, Path] = {}
        self.processed_data: Dict[str, Path] = {}

    def _setup_logging(self):
        """Configure logging using config settings."""
        app_logging.setup_logging(
            log_dir=self.config.LOG_DIR,
            log_prefix="meta_mapping",
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

    def migrate_existing_files_to_duckdb(self) -> bool:
        """Migrate existing Parquet files to DuckDB Gold layer.

        Returns:
            True if migration successful
        """
        self.logger.info("=== MIGRATION PHASE ===")

        try:
            # Load existing files
            existing_dfs = self.input_handler.read_meta_mapping_input_directory(use_dask=False)

            if not existing_dfs:
                self.logger.info("No existing files to migrate")
                return True

            # Combine all dataframes
            if len(existing_dfs) > 1:
                dfs_to_concat = []
                for df in existing_dfs:
                    if isinstance(df, dd.DataFrame):
                        dfs_to_concat.append(df.compute())
                    else:
                        dfs_to_concat.append(df)
                combined_df = pd.concat(dfs_to_concat, ignore_index=True)
            else:
                combined_df = existing_dfs[0]
                if isinstance(combined_df, dd.DataFrame):
                    combined_df = combined_df.compute()

            if combined_df.empty:
                self.logger.info("No data in existing files")
                return True

            # Remove duplicates based on provider_code + dataset_code
            if 'provider_code' in combined_df.columns and 'dataset_code' in combined_df.columns:
                original_count = len(combined_df)
                combined_df = combined_df.drop_duplicates(subset=['provider_code', 'dataset_code'], keep='last')
                if len(combined_df) < original_count:
                    self.logger.info(f"Removed duplicates: {original_count} → {len(combined_df)}")

            # Migrate to Gold layer
            success = self.output_handler.save_meta_mapping_to_duckdb(combined_df, layer="gold")
            if success:
                self.logger.info(f"Successfully migrated {len(combined_df)} records to DuckDB Gold layer")
                return True
            else:
                self.logger.error("Failed to migrate data to DuckDB")
                return False

        except Exception as e:
            self.logger.error(f"Error during migration: {str(e)}")
            return False

    def extract(self) -> Dict[str, Any]:
        """Extract phase: Read input, fetch API, identify new, extract metadata."""
        self.logger.info("=== EXTRACT PHASE ===")

        try:
            # Migrate existing files to DuckDB if not already done
            gold_data = self.input_handler.load_meta_mapping_from_duckdb(layer="gold")
            if gold_data is None or gold_data.empty:
                self.logger.info("No existing DuckDB data found, checking for file migration...")
                migration_success = self.migrate_existing_files_to_duckdb()
                if migration_success:
                    self.logger.info("Migration completed successfully")
                else:
                    self.logger.warning("Migration failed, continuing with pipeline")
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
                self.logger.warning("No input data found - will process ALL API providers")
                self.input_df = pd.DataFrame()
                self.input_providers = set()
            else:
                # Filter columns using Core logic
                input_df = filter_input_columns(input_df, required_columns=get_required_input_columns())

                if 'provider_code' in input_df.columns:
                    original_rows = len(input_df)
                    input_df = input_df.drop_duplicates(subset=['provider_code'], keep='first')
                    if len(input_df) < original_rows:
                        self.logger.info(f"Removed duplicate provider_codes: {original_rows} → {len(input_df)}")

                self.input_df = input_df
                self.input_providers = extract_input_provider_codes(input_df)

            # Step 2: Fetch API providers using Adapter
            self.logger.info("Step 2: Fetching API providers...")
            self.api_providers = self.fetcher.fetch_providers()

            if not self.api_providers:
                self.logger.error("No API providers fetched")
                return {}

            # Step 3: Identify new providers using Core logic
            self.logger.info("Step 3: Identifying new providers...")
            if len(self.input_providers) == 0:
                self.new_providers = self.api_providers
            else:
                self.new_providers = identify_new_providers(self.api_providers, self.input_providers)

            if not self.new_providers:
                self.logger.warning("No new providers to extract")
                return {}

            # Step 4: Extract metadata for new providers
            self.logger.info("Step 4: Extracting metadata for new providers...")
            dimension_lookup = build_dimension_lookup(self.input_df)
            
            extracted_df = self._extract_providers_with_timeout(dimension_lookup)

            if extracted_df.empty:
                self.logger.warning("No metadata extracted")
                return {}

            # Save to Bronze layer
            if not extracted_df.empty:
                success = self.output_handler.save_meta_mapping_to_duckdb(extracted_df, layer="bronze")
                if success:
                    self.logger.info("Saved extracted data to Bronze layer")
                else:
                    self.logger.warning("Failed to save to Bronze layer")

            self.logger.info(f"Extracted {len(extracted_df)} datasets")
            return {"metadata": extracted_df}

        except Exception as e:
            self.logger.error(f"Error in extract phase: {str(e)}")
            return {}

    def _extract_providers_with_timeout(self, dimension_lookup: Dict) -> pd.DataFrame:
        """Extract metadata from providers with timeout handling.
        
        Args:
            dimension_lookup: Pre-built dimension lookup from Core
            
        Returns:
            DataFrame with extracted metadata
        """
        providers_to_process = list(self.new_providers.values())

        if self.provider_limit is not None:
            providers_to_process = providers_to_process[:self.provider_limit]
            self.logger.info(f"Provider limit: {len(providers_to_process)}")

        self.logger.info(f"Processing {len(providers_to_process)} providers")
        self.logger.info(f"Provider timeout: {self.provider_timeout} seconds")

        all_rows = []
        timeout_count = 0
        success_count = 0

        for idx, provider in enumerate(providers_to_process, 1):
            provider_code = provider.get('code', 'UNKNOWN')
            try:
                self.logger.info(f"[{idx}/{len(providers_to_process)}] Processing {provider_code}")

                # Use ThreadPoolExecutor for timeout
                with ThreadPoolExecutor(max_workers=1) as executor:
                    future = executor.submit(self.fetcher.fetch_provider_datasets, provider_code)

                    try:
                        provider_datasets = future.result(timeout=self.provider_timeout)
                    except TimeoutError:
                        self.logger.warning(f"  ⏱ TIMEOUT: {provider_code} exceeded {self.provider_timeout}s")
                        timeout_count += 1
                        continue

                if provider_datasets:
                    # Merge dimension info using Core logic
                    provider_datasets = merge_dimension_info(
                        provider_datasets, provider_code, dimension_lookup
                    )
                    all_rows.extend(provider_datasets)
                    success_count += 1
                    self.logger.info(f"  ✓ {len(provider_datasets)} datasets extracted")
                else:
                    self.logger.warning(f"  ⚠ {provider_code}: No datasets extracted")
                    success_count += 1

            except Exception as e:
                self.logger.error(f"  ✗ Error processing {provider_code}: {e}")

        # Summary
        self.logger.info("=" * 60)
        self.logger.info(f"EXTRACTION SUMMARY:")
        self.logger.info(f"  Succeeded: {success_count}/{len(providers_to_process)}")
        self.logger.info(f"  Timeout:   {timeout_count}/{len(providers_to_process)}")
        self.logger.info(f"  Total datasets: {len(all_rows)}")
        self.logger.info("=" * 60)

        # Create DataFrame using Core logic
        return create_metadata_dataframe(all_rows)

    def process(self, input_data: Dict) -> pd.DataFrame:
        """Process phase: Transform extracted data."""
        self.logger.info("=== PROCESS PHASE ===")

        try:
            if not input_data or "metadata" not in input_data:
                self.logger.warning("No input data to process")
                return pd.DataFrame()

            df = input_data["metadata"]

            if "dimension_values" in df.columns:
                df = df.drop(columns=["dimension_values"])
                self.logger.info("Removed dimension_values column")

            # Save to Silver layer
            if not df.empty:
                success = self.output_handler.save_meta_mapping_to_duckdb(df, layer="silver")
                if success:
                    self.logger.info("Saved processed data to Silver layer")
                else:
                    self.logger.warning("Failed to save to Silver layer")

            self.logger.info(f"Processed: {len(df)} rows")
            return df

        except Exception as e:
            self.logger.error(f"Error in process phase: {str(e)}")
            return pd.DataFrame()

    def export(self, processed_data: pd.DataFrame) -> Dict[str, Path]:
        """Export phase: Save final processed data as parquet."""
        self.logger.info("=== EXPORT PHASE ===")

        try:
            if processed_data.empty:
                self.logger.warning("No data to export")
                return {}

            self.logger.info(f"Exporting: {len(processed_data)} rows")

            # Save to Gold layer
            success = self.output_handler.save_meta_mapping_to_duckdb(processed_data, layer="gold")
            if success:
                self.logger.info("Saved final data to Gold layer")
            else:
                self.logger.warning("Failed to save to Gold layer")

            # Also save to file for backward compatibility
            export_path = self.output_handler.save_meta_mapping(
                processed_data,
                prefix="meta_mapping_results",
                format="parquet",
            )

            self.logger.info(f"Exported to: {export_path.name}")
            return {"metadata": export_path}

        except Exception as e:
            self.logger.error(f"Error in export phase: {str(e)}")
            return {}

    def verify(self) -> bool:
        """Verify output using Core verification function."""
        self.logger.info("=== VERIFICATION PHASE ===")

        try:
            if not self.extracted_data:
                self.logger.warning("No extracted data to verify")
                return False

            for key, file_path in self.extracted_data.items():
                self.logger.info(f"Verifying: {file_path}")
                result = verify_output(file_path, self.input_providers)
                if not result:
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
            "input_providers": len(self.input_providers),
            "api_providers": len(self.api_providers),
            "new_providers": len(self.new_providers),
            "extracted_datasets": len(self.extracted_rows),
            "extracted_files": len(self.extracted_data),
            "processed_files": len(self.processed_data),
        }


def run(
    provider_limit: Optional[int] = None,
    verbose: bool = False,
    provider_timeout: int = 600,
) -> Dict[str, Path]:
    """Convenience function to run DBnomics Map Pipeline."""
    config = Config()

    pipeline = DbnomicsMapPipeline(
        provider_limit=provider_limit,
        verbose=verbose,
        provider_timeout=provider_timeout,
    )
    results = pipeline.run()

    if config.ENABLE_LOG_SUMMARY:
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            summary_log_file = config.LOG_DIR / f"{timestamp}_summary.log"
            app_logging.save_logs(summary_log_file)
            print(f"\n✓ Log summary saved to: {summary_log_file}")
        except Exception as e:
            logger.error(f"Failed to save log summary: {e}")

    return results
