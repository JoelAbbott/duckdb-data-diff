"""
Test script to verify value coercion issues in comparator.py
"""

import duckdb
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from src.core.comparator import DataComparator
from src.config.manager import ComparisonConfig

# Create in-memory DuckDB connection
con = duckdb.connect(":memory:")

# Create test tables with different value formats
con.execute("""
    CREATE TABLE test_left (
        id INTEGER,
        numeric_val VARCHAR,
        currency_val VARCHAR,
        date_val VARCHAR,
        bool_val VARCHAR
    )
""")

con.execute("""
    CREATE TABLE test_right (
        id INTEGER,
        numeric_val VARCHAR,
        currency_val VARCHAR,
        date_val VARCHAR,
        bool_val VARCHAR
    )
""")

# Insert test data with format variations
con.execute("""
    INSERT INTO test_left VALUES
    (1, '0', '$415,000.00', '12/30/2024', 'true'),
    (2, '1', '($41,837.84)', '01/15/2025', 't'),
    (3, '-1', '₪158,634.54', '2025-01-01', '1'),
    (4, '100', '$1,000.50', '2024-12-31 00:00:00', 'yes')
""")

con.execute("""
    INSERT INTO test_right VALUES
    (1, '0.0', '415000.0', '2024-12-30 00:00:00', 'True'),
    (2, '1.0', '-41837.84', '2025-01-15', 'TRUE'),
    (3, '-1.0', '158634.5423', '01/01/2025', 'true'),
    (4, '100.0', '1000.50', '12/31/2024', 'Yes')
""")

# Test with different tolerance settings
comparator = DataComparator(con)

print("Testing value coercion with current implementation...")
print("=" * 60)

# Test 1: Default tolerance (0.01)
config_default = ComparisonConfig(
    left_dataset="test_left",
    right_dataset="test_right",
    tolerance=0.01
)

print("\nTest 1: With tolerance=0.01 (default)")
print("-" * 40)

# Build comparison condition for numeric column
condition = comparator._build_robust_comparison_condition("numeric_val", "numeric_val", config_default)
print(f"Numeric comparison SQL (tolerance=0.01):\n{condition}\n")

# Execute to see results
sql = f"""
    SELECT l.id, 
           l.numeric_val as left_val, 
           r.numeric_val as right_val,
           {condition} as is_different
    FROM test_left l
    JOIN test_right r ON l.id = r.id
"""
result = con.execute(sql).fetchall()
for row in result:
    print(f"ID {row[0]}: '{row[1]}' vs '{row[2]}' -> Different: {row[3]}")

# Test 2: Zero tolerance (exact match)
config_exact = ComparisonConfig(
    left_dataset="test_left",
    right_dataset="test_right",
    tolerance=0.0
)

print("\n\nTest 2: With tolerance=0.0 (exact match)")
print("-" * 40)

condition = comparator._build_robust_comparison_condition("numeric_val", "numeric_val", config_exact)
print(f"Numeric comparison SQL (tolerance=0.0):\n{condition}\n")

sql = f"""
    SELECT l.id, 
           l.numeric_val as left_val, 
           r.numeric_val as right_val,
           {condition} as is_different
    FROM test_left l
    JOIN test_right r ON l.id = r.id
"""
result = con.execute(sql).fetchall()
for row in result:
    print(f"ID {row[0]}: '{row[1]}' vs '{row[2]}' -> Different: {row[3]}")

# Test 3: Currency values
print("\n\nTest 3: Currency values")
print("-" * 40)

condition = comparator._build_robust_comparison_condition("currency_val", "currency_val", config_default)
sql = f"""
    SELECT l.id, 
           l.currency_val as left_val, 
           r.currency_val as right_val,
           {condition} as is_different
    FROM test_left l
    JOIN test_right r ON l.id = r.id
"""
result = con.execute(sql).fetchall()
for row in result:
    print(f"ID {row[0]}: '{row[1]}' vs '{row[2]}' -> Different: {row[3]}")

# Test 4: Date values
print("\n\nTest 4: Date values")
print("-" * 40)

condition = comparator._build_robust_comparison_condition("date_val", "date_val", config_exact)
sql = f"""
    SELECT l.id, 
           l.date_val as left_val, 
           r.date_val as right_val,
           {condition} as is_different
    FROM test_left l
    JOIN test_right r ON l.id = r.id
"""
result = con.execute(sql).fetchall()
for row in result:
    print(f"ID {row[0]}: '{row[1]}' vs '{row[2]}' -> Different: {row[3]}")

# Test 5: Boolean values
print("\n\nTest 5: Boolean values")
print("-" * 40)

condition = comparator._build_robust_comparison_condition("bool_val", "bool_val", config_exact)
sql = f"""
    SELECT l.id, 
           l.bool_val as left_val, 
           r.bool_val as right_val,
           {condition} as is_different
    FROM test_left l
    JOIN test_right r ON l.id = r.id
"""
result = con.execute(sql).fetchall()
for row in result:
    print(f"ID {row[0]}: '{row[1]}' vs '{row[2]}' -> Different: {row[3]}")

print("\n" + "=" * 60)
print("Analysis complete. Issues identified:")
print("- Numeric values: 0 vs 0.0 treated as different when tolerance=0")
print("- Currency values: Not normalized, always different")
print("- Date values: Should work if both can cast to TIMESTAMP")
print("- Boolean values: '1' incorrectly treated as boolean")