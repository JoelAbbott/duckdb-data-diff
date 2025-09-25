#!/usr/bin/env python3
"""
Test to verify column mapping is working correctly.
"""

import duckdb
from pathlib import Path
from src.core.comparator import DataComparator
from src.config.manager import DatasetConfig, ComparisonConfig

def test_column_mapping_in_comparator():
    """Test that column mapping works correctly in the comparator."""
    
    # Create connection
    con = duckdb.connect(':memory:')
    
    # Create test tables with different column names
    con.execute("""
        CREATE TABLE left_table AS SELECT 
            1 as id,
            'John' as "From",
            'john@example.com' as "From Email Address",
            'Hello' as "Subject",
            true as "Emailed",
            'Customer1' as "Entity"
    """)
    
    con.execute("""
        CREATE TABLE right_table AS SELECT
            1 as id,
            'John' as author,
            'john@example.com' as author_email,
            'Hello' as subject_line,
            'true' as sent,
            'Customer1' as customer
    """)
    
    # Create dataset configs with column mapping
    left_config = DatasetConfig(
        name="left",
        path="test",
        column_map={}  # No mapping on left
    )
    
    right_config = DatasetConfig(
        name="right", 
        path="test",
        column_map={
            'author': 'From',
            'author_email': 'From Email Address',
            'subject_line': 'Subject',
            'sent': 'Emailed',
            'customer': 'Entity'
        }
    )
    
    # Create comparison config
    comp_config = ComparisonConfig(
        left_dataset="left_table",
        right_dataset="right_table",
        comparison_keys=['id']
    )
    
    # Create comparator and run comparison
    comparator = DataComparator(con)
    comparator.left_dataset_config = left_config
    comparator.right_dataset_config = right_config
    
    result = comparator.compare(
        "left_table", 
        "right_table",
        comp_config,
        left_config,
        right_config
    )
    
    print("Test Results:")
    print(f"  Key columns: {result.key_columns}")
    print(f"  Value columns compared: {len(result.columns_compared)}")
    print(f"  Columns: {result.columns_compared}")
    print(f"  Matched rows: {result.matched_rows}")
    print(f"  Value differences: {result.value_differences}")
    
    # Verify all mapped columns are being compared
    expected_value_columns = ["From", "From Email Address", "Subject", "Emailed", "Entity"]
    actual_value_columns = sorted(result.columns_compared)
    expected_value_columns_sorted = sorted(expected_value_columns)
    
    if actual_value_columns == expected_value_columns_sorted:
        print("\n✅ SUCCESS: All mapped columns are being compared!")
        return True
    else:
        print(f"\n❌ FAILURE: Column mismatch!")
        print(f"  Expected: {expected_value_columns_sorted}")
        print(f"  Actual: {actual_value_columns}")
        return False

if __name__ == "__main__":
    success = test_column_mapping_in_comparator()
    exit(0 if success else 1)