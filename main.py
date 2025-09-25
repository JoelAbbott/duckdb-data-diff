#!/usr/bin/env python3
"""
DuckDB Data Diff - Main Entry Point
Production-ready data comparison pipeline.
"""

import argparse
import sys
from pathlib import Path
from datetime import datetime
import duckdb
import traceback

from src import (
    ConfigManager,
    DataStager,
    DataComparator,
    ValidationPipeline,
    get_progress_monitor,
    get_logger
)
from src.core.comparator import ComparisonConfig
from src.pipeline.validators import ValidationReport


logger = get_logger()


class DataDiffPipeline:
    """
    Main pipeline orchestrator.
    """
    
    def __init__(self, config_file: Path, 
                 verbose: bool = True,
                 use_rich: bool = True):
        """
        Initialize pipeline.
        
        Args:
            config_file: Path to configuration file
            verbose: Enable verbose logging
            use_rich: Use Rich for progress bars
        """
        self.config_file = Path(config_file)
        self.verbose = verbose
        self.progress = get_progress_monitor(use_rich)
        
        # Initialize components
        self.config_manager = ConfigManager(self.config_file)
        self.stager = DataStager()
        self.validator = ValidationPipeline()
        
        # DuckDB connection
        self.con = None
        
    def run(self) -> bool:
        """
        Run the complete pipeline.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info("pipeline.starting", 
                       config=str(self.config_file))
            
            # Load configuration
            if hasattr(self.progress, 'task_context'):
                with self.progress.task_context("config", "Loading configuration") as task:
                    self.config_manager.load()
                    task.update(description="Configuration loaded")
            else:
                self.config_manager.load()
                print("✓ Configuration loaded")
            
            # Initialize DuckDB
            self.con = duckdb.connect(":memory:")
            logger.info("pipeline.duckdb.initialized")
            
            # Stage datasets
            self._stage_all_datasets()
            
            # Run comparisons
            self._run_comparisons()
            
            logger.info("pipeline.completed")
            return True
            
        except Exception as e:
            logger.error("pipeline.failed", 
                        error=str(e),
                        traceback=traceback.format_exc())
            if hasattr(self.progress, 'log_error'):
                self.progress.log_error(f"Pipeline failed: {e}")
            else:
                print(f"❌ Pipeline failed: {e}")
            return False
        finally:
            if self.con:
                self.con.close()
    
    def _stage_all_datasets(self):
        """Stage all datasets to Parquet."""
        datasets = self.config_manager.datasets
        
        print(f"Staging {len(datasets)} datasets...")
        
        for name, config in datasets.items():
            print(f"  Staging {name}...")
            
            # Stage dataset
            table_name = self.stager.stage_dataset(
                self.con, config
            )
            
            # Validate dataset
            df = self.con.execute(
                f"SELECT * FROM {table_name} LIMIT 10000"
            ).df()
            
            validation_config = {
                "key_columns": config.key_columns,
                "fail_fast": False
            }
            
            report = self.validator.validate(df, validation_config)
            
            if not report.is_valid:
                errors = report.get_errors()
                logger.warning("pipeline.validation.issues",
                             dataset=name,
                             errors=len(errors))
                
                if self.verbose:
                    for error in errors[:5]:
                        print(f"  ⚠ {name}: {error.message}")
    
    def _run_comparisons(self):
        """Run all configured comparisons."""
        comparisons = self.config_manager.comparisons
        
        if not comparisons:
            logger.warning("pipeline.no_comparisons")
            return
        
        comparator = DataComparator(self.con)
        
        print(f"Running {len(comparisons)} comparisons...")
        
        for comp_config in comparisons:
            print(f"  Comparing {comp_config.left_dataset} vs {comp_config.right_dataset}...")
            
            # Get dataset configs for column mapping (BUG 3 fix)
            left_dataset_config = self.config_manager.get_dataset(comp_config.left_dataset)
            right_dataset_config = self.config_manager.get_dataset(comp_config.right_dataset)
            
            # Debug: Check if column mappings are present
            if right_dataset_config.column_map:
                print(f"  DEBUG: Right dataset has {len(right_dataset_config.column_map)} column mappings")
                # Show first 3 mappings
                for i, (right_col, left_col) in enumerate(right_dataset_config.column_map.items()):
                    if i < 3:
                        print(f"    {right_col} -> {left_col}")
                if len(right_dataset_config.column_map) > 3:
                    print(f"    ... and {len(right_dataset_config.column_map) - 3} more")
            else:
                print(f"  DEBUG: No column mappings found in right dataset config")
            
            # Run comparison
            result = comparator.compare(
                comp_config.left_dataset,
                comp_config.right_dataset,
                comp_config,
                left_dataset_config,
                right_dataset_config
            )
            
            # Update config with detected keys (CLAUDE.md: reproducible results)
            comp_config.comparison_keys = result.key_columns
            
            # Report results
            self._report_results(comp_config, result)
            
            # Export differences
            output_dir = Path("data/reports") / (
                f"{comp_config.left_dataset}_vs_"
                f"{comp_config.right_dataset}_"
                f"{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            )
            
            comparator.export_differences(
                comp_config.left_dataset,
                comp_config.right_dataset,
                comp_config,
                output_dir,
                left_dataset_config,
                right_dataset_config
            )
    
    def _report_results(self, config: ComparisonConfig, result):
        """
        Report comparison results.
        
        Args:
            config: Comparison configuration
            result: Comparison results
        """
        logger.info("comparison.results",
                   left=config.left_dataset,
                   right=config.right_dataset,
                   matched=result.matched_rows,
                   only_left=result.only_in_left,
                   only_right=result.only_in_right,
                   differences=result.value_differences,
                   match_rate=result.summary.get("match_rate"))
        
        if self.verbose:
            print(f"\n{'='*60}")
            print(f"Comparison: {config.left_dataset} vs "
                  f"{config.right_dataset}")
            print(f"{'='*60}")
            print(f"Total rows in {config.left_dataset}: "
                  f"{result.total_left:,}")
            print(f"Total rows in {config.right_dataset}: "
                  f"{result.total_right:,}")
            print(f"Matched rows: {result.matched_rows:,}")
            print(f"Only in {config.left_dataset}: "
                  f"{result.only_in_left:,}")
            print(f"Only in {config.right_dataset}: "
                  f"{result.only_in_right:,}")
            print(f"Value differences: {result.value_differences:,}")
            print(f"Match rate: {result.summary['match_rate']}%")
            print(f"{'='*60}\n")


def create_sample_config(output_path: Path):
    """
    Create a sample configuration file.
    
    Args:
        output_path: Where to save the config
    """
    sample_config = """# DuckDB Data Diff Configuration
# ===============================

datasets:
  # Define your datasets here
  left_dataset:
    path: "data/raw/left.csv"
    type: "csv"
    key_columns: ["id"]
    exclude_columns: []
    normalizers:
      name: "unicode_clean"
    converters:
      amount: "currency_usd"
    
  right_dataset:
    path: "data/raw/right.xlsx"
    type: "excel"
    key_columns: ["id"]
    exclude_columns: []
    normalizers:
      name: "unicode_clean"
    converters:
      amount: "currency_usd"

comparisons:
  # Define comparisons to run
  - left: "left_dataset"
    right: "right_dataset"
    keys: ["id"]
    columns: []  # Empty means compare all columns
    tolerance: 0.01
    ignore_case: true
    ignore_spaces: true
    output_format: "excel"
    max_differences: 1000
"""
    
    output_path.write_text(sample_config)
    print(f"Sample configuration created: {output_path}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="DuckDB Data Diff - Production-ready data comparison"
    )
    
    parser.add_argument(
        "config",
        nargs="?",
        default="datasets.yaml",
        help="Configuration file (default: datasets.yaml)"
    )
    
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose output"
    )
    
    parser.add_argument(
        "--no-rich",
        action="store_true",
        help="Disable Rich progress bars"
    )
    
    parser.add_argument(
        "--create-sample",
        action="store_true",
        help="Create sample configuration file"
    )
    
    parser.add_argument(
        "--version",
        action="version",
        version="DuckDB Data Diff v2.0.0"
    )
    
    args = parser.parse_args()
    
    # Create sample config if requested
    if args.create_sample:
        create_sample_config(Path("datasets_sample.yaml"))
        return 0
    
    # Check if config exists
    config_path = Path(args.config)
    if not config_path.exists():
        print(f"Error: Configuration file not found: {config_path}")
        print("Use --create-sample to create a sample configuration")
        return 1
    
    # Run pipeline
    pipeline = DataDiffPipeline(
        config_path,
        verbose=args.verbose,
        use_rich=not args.no_rich
    )
    
    success = pipeline.run()
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())