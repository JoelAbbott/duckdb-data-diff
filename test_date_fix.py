#!/usr/bin/env python3
"""Test that date column comparisons work with mixed types."""

import duckdb
import pandas as pd
from datetime import datetime

# Create test data with mixed date types
con = duckdb.connect()

# Create left table with date as string
con.execute("""
    CREATE TABLE left_table (
        id INTEGER,
        end_date VARCHAR,
        name VARCHAR
    )
""")

con.execute("""
    INSERT INTO left_table VALUES
    (1, '12/31/2030', 'Test1'),
    (2, '01/01/2025', 'Test2'),
    (3, '06/15/2024', 'Test3')
""")

# Create right table with date as actual date or integer
con.execute("""
    CREATE TABLE right_table (
        id INTEGER,
        end_date DATE,
        name VARCHAR
    )
""")

con.execute("""
    INSERT INTO right_table VALUES
    (1, DATE '2030-12-31', 'Test1'),
    (2, DATE '2025-01-01', 'Test2'),
    (3, DATE '2024-06-15', 'Test3_changed')
""")

print("Testing date comparison with type casting...")

# This would fail without casting
try:
    result = con.execute("""
        SELECT COUNT(*)
        FROM left_table l
        INNER JOIN right_table r ON l.id = r.id
        WHERE l.end_date != r.end_date
    """).fetchone()
    print(f"Without casting: {result[0]} differences found")
except Exception as e:
    print(f"Without casting failed (expected): {e}")

# This should work with casting
try:
    result = con.execute("""
        SELECT COUNT(*)
        FROM left_table l
        INNER JOIN right_table r ON l.id = r.id
        WHERE CAST(l.end_date AS VARCHAR) != CAST(r.end_date AS VARCHAR)
    """).fetchone()
    print(f"With casting: {result[0]} differences found")
    
    # Show the actual values
    diff_rows = con.execute("""
        SELECT 
            l.id,
            l.end_date as left_date,
            r.end_date as right_date,
            CAST(l.end_date AS VARCHAR) as left_str,
            CAST(r.end_date AS VARCHAR) as right_str
        FROM left_table l
        INNER JOIN right_table r ON l.id = r.id
    """).fetchall()
    
    print("\nComparison details:")
    for row in diff_rows:
        print(f"  ID {row[0]}: '{row[1]}' vs '{row[2]}' -> '{row[3]}' vs '{row[4]}'")
        
except Exception as e:
    print(f"With casting failed: {e}")

print("\nTest complete!")