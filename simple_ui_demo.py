#!/usr/bin/env python3
"""
Simple UI demo to show Rich interface with real data.
"""

import time
from pathlib import Path
from src.ui.rich_progress import RichProgressMonitor

def main():
    """Simple Rich UI demonstration."""
    
    print("🎨 Rich UI Demo - Interactive Data Comparison")
    print("=" * 60)
    print("This shows the beautiful terminal interface!")
    print()
    
    # Initialize Rich monitor
    monitor = RichProgressMonitor()
    
    # Start with beautiful header
    monitor.start_pipeline("🚀 DuckDB Data Diff - Production Pipeline")
    
    # Simulate real pipeline steps
    print("\n📋 Simulating real data comparison...")
    
    # Step 1: Configuration
    task_id = monitor.add_task("config", 1, "Loading configuration")
    time.sleep(1)
    monitor.update_task("config", completed=1, description="✓ Configuration loaded")
    
    # Step 2: File Reading  
    files_task = monitor.add_task("files", 2, "Reading input files")
    monitor.update_task("files", completed=0, description="Reading left_dataset.csv...")
    time.sleep(1)
    monitor.update_task("files", completed=1, description="Reading right_dataset.xlsx...")
    time.sleep(1)  
    monitor.update_task("files", completed=2, description="✓ All files loaded")
    
    # Step 3: Data Staging
    staging_task = monitor.add_task("staging", 2, "Staging data to Parquet")
    for i in range(2):
        monitor.update_task("staging", completed=i, 
                          description=f"Processing dataset {i+1}...")
        time.sleep(1.5)
    monitor.update_task("staging", completed=2, description="✓ Data staged efficiently")
    
    # Step 4: Data Validation
    validation_task = monitor.add_task("validation", 4, "Validating data quality")
    checks = ["Schema validation", "Key uniqueness", "Data types", "Completeness"]
    for i, check in enumerate(checks):
        monitor.update_task("validation", completed=i, description=f"Running {check}...")
        time.sleep(0.8)
    monitor.update_task("validation", completed=4, description="✓ Data validated")
    
    # Step 5: Comparison
    compare_task = monitor.add_task("compare", 100000, "Comparing 100,000 rows")
    for i in range(0, 100001, 10000):
        monitor.update_task("compare", completed=i, 
                          description=f"Processed {i:,} rows...")
        time.sleep(0.3)
    monitor.update_task("compare", completed=100000, description="✓ Comparison complete")
    
    # Show beautiful results
    print("\n📊 Displaying comparison results...")
    
    # Mock realistic results
    results = {
        "total_left": 100000,
        "total_right": 98500,
        "matched_rows": 92000,
        "only_in_left": 8000,
        "only_in_right": 6500,
        "value_differences": 3200,
        "match_rate": 92.0,
        "difference_rate": 3.5
    }
    
    monitor.show_comparison_results(results)
    
    # Show validation summary
    validation_reports = [
        {
            "dataset": "sales_data_2023", 
            "is_valid": True,
            "error_count": 0,
            "warning_count": 3,
            "row_count": 100000
        },
        {
            "dataset": "sales_data_2024",
            "is_valid": True,
            "error_count": 0, 
            "warning_count": 1,
            "row_count": 98500
        }
    ]
    
    monitor.show_validation_summary(validation_reports)
    
    # Show performance metrics
    metrics = {
        "Processing Time": "4.2 seconds",
        "Throughput": "23,809 rows/second", 
        "Memory Usage": "245 MB peak",
        "Cache Efficiency": "94% hit rate",
        "Files Processed": 2,
        "Errors": 0
    }
    
    monitor.show_metrics(metrics)
    
    # Final status messages
    monitor.log_success("🎉 Pipeline completed successfully!")
    monitor.log_success("📁 Reports saved to: data/reports/")
    monitor.log_success("⚡ Performance: 23K rows/second")
    monitor.log_warning("⚠️  3 minor data quality warnings detected")
    
    # Stop with summary
    monitor.stop()
    
    print("\n✨ Demo completed!")
    print("\n🎯 What you just saw:")
    print("  ✅ Beautiful progress bars with live updates")
    print("  ✅ Colored tables for results display")
    print("  ✅ Professional panels and layouts")
    print("  ✅ Real-time performance tracking")
    print("  ✅ Status messages with icons")
    print("\nThis is what users see when running:")
    print("  python main.py datasets.yaml")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n👋 Demo interrupted")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()