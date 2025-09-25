#!/usr/bin/env python3
"""Test that comparison still works without column mapping (exact matches)."""

import duckdb
from src.core.comparator import DataComparator
from src.config.manager import ComparisonConfig

# Create test data with matching column names
con = duckdb.connect()

con.execute("""
    CREATE TABLE left_test (
        id INT,
        name VARCHAR,
        email VARCHAR,
        is_active VARCHAR
    )
""")

con.execute("""
    INSERT INTO left_test VALUES
    (1, 'John Doe', 'john@example.com', 'true'),
    (2, 'Jane Smith', 'jane@example.com', 'false')
""")

con.execute("""
    CREATE TABLE right_test (
        id INT,
        name VARCHAR,
        email VARCHAR,
        is_active VARCHAR
    )
""")

con.execute("""
    INSERT INTO right_test VALUES
    (1, 'John Doe', 'john@example.com', 't'),
    (2, 'Jane Smith', 'jane@example.com', 'f')
""")

# Create comparator WITHOUT any column mappings
comparator = DataComparator(con)

# No dataset configs (no mappings)
comparator.left_dataset_config = None
comparator.right_dataset_config = None

# Create comparison config
config = ComparisonConfig('left_test', 'right_test')
config.comparison_keys = ['id']
config.value_columns = None

# Test value column determination
value_cols = comparator._determine_value_columns(
    'left_test', 'right_test', config, ['id']
)

print("Testing without column mapping (exact name matches):")
print(f"  Left columns: {comparator._get_columns('left_test')}")
print(f"  Right columns: {comparator._get_columns('right_test')}")
print(f"  Value columns found: {value_cols}")
print(f"  Number of value columns: {len(value_cols)}")

# Expected: Should find all 3 columns since names match exactly
expected_cols = ['name', 'email', 'is_active']
if set(value_cols) == set(expected_cols):
    print("✅ SUCCESS: Exact name matching still works without mappings!")
else:
    print(f"❌ FAILED: Expected {expected_cols}, but got {value_cols}")