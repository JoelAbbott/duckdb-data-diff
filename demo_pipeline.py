#!/usr/bin/env python3
"""
Demo the full pipeline with Rich UI components.
"""

import time
import duckdb
import pandas as pd
from pathlib import Path

from src import (
    ConfigManager, 
    DatasetConfig,
    ComparisonConfig,
    DataStager, 
    DataComparator,
    ValidationPipeline,
    get_logger
)
from src.ui.rich_progress import RichProgressMonitor
from src.utils.metrics import MetricsCollector
from src.core.lineage import DataLineageTracker


def create_demo_data():
    """Create small demo datasets for UI testing."""
    
    # Create demo data directory
    demo_dir = Path("demo_data")
    demo_dir.mkdir(exist_ok=True)
    
    # Left dataset
    left_data = pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice Smith', 'Bob Jones', 'Charlie Brown', 'Diana Prince', 'Eve Wilson'],
        'email': ['alice@example.com', 'bob@example.com', 'charlie@example.com', 'diana@example.com', 'eve@example.com'],
        'amount': [1000.50, 2500.75, 1750.25, 3200.00, 875.90],
        'active': ['true', 'false', 'true', 'true', 'false']
    })
    left_data.to_csv(demo_dir / 'left_demo.csv', index=False)
    
    # Right dataset (with some differences)
    right_data = pd.DataFrame({
        'id': [1, 2, 3, 5, 6],  # Missing 4, added 6
        'name': ['Alice Smith', 'Bob Johnson', 'Charlie Brown', 'Eve Wilson', 'Frank Miller'],  # Bob's name changed
        'email': ['alice@example.com', 'bob@newcompany.com', 'charlie@example.com', 'eve@example.com', 'frank@example.com'],  # Bob's email changed
        'amount': [1000.50, 2600.00, 1750.25, 900.00, 1250.00],  # Some amounts changed
        'active': ['true', 'false', 'true', 'true', 'true']
    })
    right_data.to_csv(demo_dir / 'right_demo.csv', index=False)
    
    return demo_dir / 'left_demo.csv', demo_dir / 'right_demo.csv'


def run_demo_with_ui():
    """Run a full demo with Rich UI."""
    
    # Initialize components
    monitor = RichProgressMonitor()
    metrics = MetricsCollector()
    lineage = DataLineageTracker()
    logger = get_logger()
    
    try:
        # Start the beautiful pipeline UI
        monitor.start_pipeline("🎨 DuckDB Data Diff - Rich UI Demo")
        
        print("\n🎬 Starting Rich UI Demo...")
        print("Watch the beautiful progress bars and status updates!")
        
        # Step 1: Create demo data
        with monitor.task_context("setup", "Creating demo datasets", total=2) as task:
            left_file, right_file = create_demo_data()
            task.update(description="Demo data created")
            time.sleep(1)  # Simulate processing time
        
        # Step 2: Configure datasets
        with monitor.task_context("config", "Setting up configuration", total=1) as task:
            # Create dataset configs
            left_config = DatasetConfig(
                name="demo_left",
                path=str(left_file),
                type="csv",
                key_columns=["id"],
                normalizers={"name": "unicode_clean"},
                converters={"amount": "currency_usd"}
            )
            
            right_config = DatasetConfig(
                name="demo_right", 
                path=str(right_file),
                type="csv",
                key_columns=["id"],
                normalizers={"name": "unicode_clean"},
                converters={"amount": "currency_usd"}
            )
            
            comparison_config = ComparisonConfig(
                left_dataset="demo_left",
                right_dataset="demo_right",
                comparison_keys=["id"],
                value_columns=["name", "email", "amount", "active"],
                tolerance=0.01
            )
            
            task.update(description="Configuration ready")
            time.sleep(0.5)
        
        # Step 3: Initialize DuckDB
        with monitor.task_context("db", "Initializing DuckDB", total=1) as task:
            con = duckdb.connect(":memory:")
            task.update(description="DuckDB ready")
            time.sleep(0.3)
        
        # Step 4: Stage datasets
        stager = DataStager()
        with monitor.task_context("staging", "Staging datasets", total=2) as task:
            
            # Track lineage
            lineage.track_dataset_source("demo_left", left_file)
            lineage.track_dataset_source("demo_right", right_file)
            
            # Stage left dataset
            task.update(description="Staging left dataset...")
            metrics.start_operation("stage_left")
            left_table = stager.stage_dataset(con, left_config)
            metrics.end_operation("stage_left", 5, True)  # 5 rows
            lineage.update_dataset_stats("demo_left", final_rows=5, processing_time=0.1)
            
            task.update(advance=1, description="Left dataset staged")
            time.sleep(0.5)
            
            # Stage right dataset  
            task.update(description="Staging right dataset...")
            metrics.start_operation("stage_right")
            right_table = stager.stage_dataset(con, right_config)
            metrics.end_operation("stage_right", 5, True)  # 5 rows
            lineage.update_dataset_stats("demo_right", final_rows=5, processing_time=0.1)
            
            task.update(description="Both datasets staged")
            time.sleep(0.5)
        
        # Step 5: Validate data
        validator = ValidationPipeline()
        with monitor.task_context("validation", "Validating data quality", total=2) as task:
            
            # Validate left
            left_df = con.execute(f"SELECT * FROM {left_table}").df()
            left_report = validator.validate(left_df, {"key_columns": ["id"]})
            task.update(advance=1, description="Left dataset validated")
            
            # Validate right
            right_df = con.execute(f"SELECT * FROM {right_table}").df()
            right_report = validator.validate(right_df, {"key_columns": ["id"]})
            task.update(description="Both datasets validated")
            
            time.sleep(1)
        
        # Show validation results
        validation_summary = [
            {
                "dataset": "demo_left",
                "is_valid": left_report.is_valid,
                "error_count": len(left_report.get_errors()),
                "warning_count": len(left_report.get_warnings()),
                "row_count": len(left_df)
            },
            {
                "dataset": "demo_right",
                "is_valid": right_report.is_valid, 
                "error_count": len(right_report.get_errors()),
                "warning_count": len(right_report.get_warnings()),
                "row_count": len(right_df)
            }
        ]
        
        monitor.show_validation_summary(validation_summary)
        
        # Step 6: Run comparison
        comparator = DataComparator(con)
        with monitor.task_context("comparison", "Comparing datasets", total=1) as task:
            
            metrics.start_operation("comparison")
            result = comparator.compare(left_table, right_table, comparison_config)
            metrics.end_operation("comparison", result.matched_rows, True)
            
            # Track comparison lineage
            lineage.track_comparison(
                "demo_left", "demo_right", 
                {"key_columns": ["id"], "value_columns": ["name", "email", "amount"]},
                {
                    "matched_rows": result.matched_rows,
                    "only_in_left": result.only_in_left,
                    "only_in_right": result.only_in_right,
                    "value_differences": result.value_differences
                },
                ["demo_output.csv"],
                0.5
            )
            
            task.update(description="Comparison completed")
            time.sleep(1)
        
        # Show beautiful comparison results
        comparison_results = {
            "total_left": result.total_left,
            "total_right": result.total_right,
            "matched_rows": result.matched_rows,
            "only_in_left": result.only_in_left,
            "only_in_right": result.only_in_right,
            "value_differences": result.value_differences,
            "match_rate": result.summary.get("match_rate", 0),
            "difference_rate": result.summary.get("difference_rate", 0)
        }
        
        monitor.show_comparison_results(comparison_results)
        
        # Step 7: Generate reports
        with monitor.task_context("reporting", "Generating reports", total=1) as task:
            # Create output directory
            output_dir = Path("demo_data/results")
            output_dir.mkdir(exist_ok=True)
            
            # Export differences
            outputs = comparator.export_differences(
                left_table, right_table, comparison_config, output_dir
            )
            
            task.update(description=f"Reports saved to {output_dir}")
            time.sleep(1)
        
        # Show performance metrics
        final_metrics = metrics.generate_report()
        
        performance_display = {
            "Total Duration": f"{final_metrics['summary']['total_duration_seconds']:.2f}s",
            "Rows Processed": final_metrics['summary']['total_rows_processed'],
            "Processing Rate": f"{final_metrics['summary']['rows_per_second']:.0f} rows/sec",
            "Memory Peak": f"{final_metrics['summary']['memory_mb_peak']:.1f} MB",
            "Operations": len(final_metrics['summary']),
            "Success Rate": "100%" if final_metrics['summary']['errors_encountered'] == 0 else "Failed"
        }
        
        monitor.show_metrics(performance_display)
        
        # Success messages
        monitor.log_success("🎉 Demo pipeline completed successfully!")
        monitor.log_success(f"📊 Results saved in: {output_dir}")
        monitor.log_success(f"📈 Performance: {final_metrics['summary']['rows_per_second']:.0f} rows/second")
        
        # Generate lineage report
        lineage_file = Path("demo_data/lineage_report.json")
        lineage.save_lineage_report(lineage_file)
        monitor.log_success(f"🗺️  Data lineage saved: {lineage_file}")
        
        con.close()
        
    except Exception as e:
        monitor.log_error("Demo failed", {"error": str(e)})
        raise
    finally:
        monitor.stop()


if __name__ == "__main__":
    print("🎨 Rich UI Demo - Full Pipeline")
    print("=" * 50)
    print("This demonstrates the beautiful Rich terminal UI")
    print("with real data processing and progress tracking.")
    print()
    
    try:
        run_demo_with_ui()
        
        print("\n✨ Demo completed! Check the demo_data/ folder for results.")
        print("\n📁 Generated files:")
        
        demo_dir = Path("demo_data")
        if demo_dir.exists():
            for file in demo_dir.rglob("*"):
                if file.is_file():
                    print(f"   - {file}")
                    
    except KeyboardInterrupt:
        print("\n\n👋 Demo interrupted by user")
    except Exception as e:
        print(f"\n❌ Demo failed: {e}")
        import traceback
        traceback.print_exc()