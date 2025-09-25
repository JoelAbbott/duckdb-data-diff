#!/usr/bin/env python3
"""Test that column mapping works for value column comparison."""

import duckdb
from src.core.comparator import DataComparator
from src.config.manager import ComparisonConfig

# Create test data
con = duckdb.connect()

# Left table with certain column names
con.execute("""
    CREATE TABLE left_test (
        id INT,
        customer_name VARCHAR,
        email_address VARCHAR,
        is_active VARCHAR
    )
""")

con.execute("""
    INSERT INTO left_test VALUES
    (1, 'John Doe', 'john@example.com', 'true'),
    (2, 'Jane Smith', 'jane@example.com', 'false')
""")

# Right table with different column names
con.execute("""
    CREATE TABLE right_test (
        id INT,
        name VARCHAR,
        email VARCHAR,
        active VARCHAR
    )
""")

con.execute("""
    INSERT INTO right_test VALUES
    (1, 'John Doe', 'john@example.com', 't'),
    (2, 'Jane Smith', 'jane@example.com', 'f')
""")

# Create comparator
comparator = DataComparator(con)

# Set up column mappings (simulating what the interactive menu does)
class MockLeftConfig:
    column_map = {}

class MockRightConfig:
    # The mapping format is {right_column: left_column}
    column_map = {
        'name': 'customer_name',
        'email': 'email_address',
        'active': 'is_active'
    }

comparator.left_dataset_config = MockLeftConfig()
comparator.right_dataset_config = MockRightConfig()

# Create comparison config
config = ComparisonConfig('left_test', 'right_test')
config.comparison_keys = ['id']
config.value_columns = None  # Let it auto-determine

# Test value column determination
value_cols = comparator._determine_value_columns(
    'left_test', 'right_test', config, ['id']
)

print("Testing column mapping for value columns:")
print(f"  Left columns: {comparator._get_columns('left_test')}")
print(f"  Right columns: {comparator._get_columns('right_test')}")
print(f"  Value columns found: {value_cols}")
print(f"  Number of value columns: {len(value_cols)}")

# Expected: Should find all 3 value columns via mapping
expected_cols = ['customer_name', 'email_address', 'is_active']
if set(value_cols) == set(expected_cols):
    print("✅ SUCCESS: All mapped columns were included for comparison!")
else:
    print(f"❌ FAILED: Expected {expected_cols}, but got {value_cols}")
    missing = set(expected_cols) - set(value_cols)
    if missing:
        print(f"  Missing columns: {missing}")
    extra = set(value_cols) - set(expected_cols)
    if extra:
        print(f"  Extra columns: {extra}")