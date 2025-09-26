#!/usr/bin/env python3
"""
Run Corrected Inventory Balance Test - Automated execution
"""
import sys
import os
sys.path.append('src')

from config.config_loader import ConfigLoader
from core.comparator import DataComparator
from utils.logger import get_logger
import duckdb

def run_corrected_inventory_test():
    """Execute the corrected inventory balance comparison"""
    print("🔬 FINAL VERIFICATION - CORRECTED INVENTORY BALANCE")
    print("="*60)
    
    config_file = "test_inventory_balance_corrected.yaml"
    
    try:
        # Load configuration
        print(f"📋 Loading config: {config_file}")
        config = ConfigLoader(config_file)
        datasets = config.get_datasets()
        comparisons = config.get_comparisons()
        
        # Show configuration details
        print(f"\n🔧 CONFIGURATION DETAILS:")
        for name, dataset in datasets.items():
            key = dataset.get('key_columns', [])
            print(f"   {name}: key={key}")
        
        # Execute comparison
        print(f"\n⚡ EXECUTING COMPARISON...")
        
        con = duckdb.connect('comparison.duckdb')
        comparator = DataComparator(con, datasets)
        
        for comparison in comparisons:
            left_name = comparison['left']
            right_name = comparison['right']
            keys = comparison['keys']
            
            print(f"\n🎯 Comparing: {left_name} vs {right_name}")
            print(f"🔑 Using key: {keys}")
            
            result = comparator.compare_datasets(
                left_name=left_name,
                right_name=right_name,
                key_columns=keys,
                max_differences=100
            )
            
            # Display results
            print(f"\n📊 COMPARISON RESULTS:")
            print(f"   Total Left Rows: {result.total_left_rows:,}")
            print(f"   Total Right Rows: {result.total_right_rows:,}")
            print(f"   Matched Rows: {result.matched_rows:,}")
            print(f"   Only in Left: {result.only_left_rows:,}")
            print(f"   Only in Right: {result.only_right_rows:,}")
            print(f"   Value Differences: {result.value_diff_rows:,}")
            
            # Calculate match rate
            if result.total_left_rows > 0:
                match_rate = (result.matched_rows / result.total_left_rows) * 100
                print(f"   🎯 MATCH RATE: {match_rate:.1f}%")
                
                # Success criteria
                if match_rate > 90:
                    print(f"   ✅ SUCCESS: Match rate > 90% (vs previous 0%)")
                else:
                    print(f"   ⚠️ PARTIAL: Match rate {match_rate:.1f}% (improvement from 0%)")
            
        con.close()
        return True
        
    except Exception as e:
        print(f"❌ Error executing comparison: {str(e)}")
        return False

if __name__ == "__main__":
    run_corrected_inventory_test()