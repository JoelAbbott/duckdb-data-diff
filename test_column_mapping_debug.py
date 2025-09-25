#!/usr/bin/env python3
"""
Debug why column mapping isn't working.
"""

import duckdb
from pathlib import Path
from src.config.manager import DatasetConfig, ComparisonConfig
from src.core.comparator import DataComparator

# Create a simple test
con = duckdb.connect(':memory:')

# Load and stage the real data
print("Loading real data...")
con.execute("""
    CREATE TABLE netsuite_temp AS 
    SELECT * FROM 'data/staging/netsuite_messages_1.parquet'
    LIMIT 100
""")

con.execute("""
    CREATE TABLE qa2_temp AS 
    SELECT * FROM 'data/staging/qa2_netsuite_messages.parquet'
    LIMIT 100
""")

# Check columns
left_cols = [r[0] for r in con.execute("DESCRIBE netsuite_temp").fetchall()]
right_cols = [r[0] for r in con.execute("DESCRIBE qa2_temp").fetchall()]

print(f"\nLeft columns (first 5): {left_cols[:5]}")
print(f"Right columns (first 5): {right_cols[:5]}")

# Create configs with mapping
left_config = DatasetConfig(
    name="netsuite_temp",
    path="test",
    key_columns=["internal_id"],
    column_map={}
)

right_config = DatasetConfig(
    name="qa2_temp",
    path="test",
    key_columns=["message_id"],
    column_map={
        'message_id': 'internal_id',
        'author': 'from',
        'author_email': 'from_email_address',
        'email_subject': 'subject',
        'vendor': 'entity'
    }
)

comp_config = ComparisonConfig(
    left_dataset="netsuite_temp",
    right_dataset="qa2_temp",
    comparison_keys=["internal_id"]
)

# Run comparator
print("\n=== Running comparator ===")
comparator = DataComparator(con)

# Set the dataset configs
comparator.left_dataset_config = left_config
comparator.right_dataset_config = right_config

# Spy on what's happening
print(f"Left config has column_map: {left_config.column_map}")
print(f"Right config has column_map: {right_config.column_map}")

# Now compare
result = comparator.compare(
    "netsuite_temp",
    "qa2_temp",
    comp_config,
    left_config,
    right_config
)

print(f"\n=== Results ===")
print(f"Key columns: {result.key_columns}")
print(f"Value columns compared: {len(result.columns_compared)}")
print(f"Columns: {result.columns_compared[:5]}...")

# Expected: Should compare mapped columns
if len(result.columns_compared) > 2:
    print("\n✅ SUCCESS: Column mapping is working!")
else:
    print("\n❌ FAILURE: Only comparing exact matches")
    print("\nDebugging: Let's check what _get_right_column returns")
    
    # Test the helper method directly
    test_left_col = "from"
    right_col = comparator._get_right_column(test_left_col)
    print(f"  _get_right_column('{test_left_col}') = '{right_col}'")
    print(f"  Expected: 'author'")
    print(f"  Right config column_map: {comparator.right_dataset_config.column_map if comparator.right_dataset_config else 'None'}")