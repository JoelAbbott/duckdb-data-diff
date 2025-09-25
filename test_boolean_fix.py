#!/usr/bin/env python3
"""Test that the boolean normalization fix works."""

import duckdb

con = duckdb.connect()

# Create test tables with different boolean representations
con.execute("""
    CREATE TABLE left_test (
        id INT,
        is_active VARCHAR,
        name VARCHAR
    )
""")

con.execute("""
    INSERT INTO left_test VALUES
    (1, 'true', 'Test1'),
    (2, 'false', 'Test2'),
    (3, 'True', 'Test3'),
    (4, 'False', 'Test4')
""")

con.execute("""
    CREATE TABLE right_test (
        id INT,
        is_active VARCHAR,
        name VARCHAR
    )
""")

con.execute("""
    INSERT INTO right_test VALUES
    (1, 't', 'Test1'),
    (2, 'f', 'Test2'),
    (3, 'T', 'Test3'),
    (4, 'F', 'Test4')
""")

print("Testing boolean normalization in comparison...")

# Test the normalized comparison
sql = """
    SELECT 
        l.id,
        l.is_active as left_val,
        r.is_active as right_val,
        CASE 
            WHEN (
                CASE 
                    WHEN LOWER(CAST(l.is_active AS VARCHAR)) IN ('true', 't', '1', 'yes') THEN 't'
                    WHEN LOWER(CAST(l.is_active AS VARCHAR)) IN ('false', 'f', '0', 'no', '') THEN 'f'
                    ELSE CAST(l.is_active AS VARCHAR)
                END != 
                CASE 
                    WHEN LOWER(CAST(r.is_active AS VARCHAR)) IN ('true', 't', '1', 'yes') THEN 't'
                    WHEN LOWER(CAST(r.is_active AS VARCHAR)) IN ('false', 'f', '0', 'no', '') THEN 'f'
                    ELSE CAST(r.is_active AS VARCHAR)
                END
            ) THEN 'Different'
            ELSE 'Matched'
        END as comparison_result
    FROM left_test l
    INNER JOIN right_test r ON l.id = r.id
"""

result = con.execute(sql).fetchall()

print("\nComparison Results:")
print("ID | Left | Right | Result")
print("---|------|-------|--------")
for row in result:
    print(f"{row[0]:2} | {row[1]:5} | {row[2]:5} | {row[3]}")

# Count differences
diff_count = con.execute("""
    SELECT COUNT(*)
    FROM left_test l
    INNER JOIN right_test r ON l.id = r.id
    WHERE (
        CASE 
            WHEN LOWER(CAST(l.is_active AS VARCHAR)) IN ('true', 't', '1', 'yes') THEN 't'
            WHEN LOWER(CAST(l.is_active AS VARCHAR)) IN ('false', 'f', '0', 'no', '') THEN 'f'
            ELSE CAST(l.is_active AS VARCHAR)
        END != 
        CASE 
            WHEN LOWER(CAST(r.is_active AS VARCHAR)) IN ('true', 't', '1', 'yes') THEN 't'
            WHEN LOWER(CAST(r.is_active AS VARCHAR)) IN ('false', 'f', '0', 'no', '') THEN 'f'
            ELSE CAST(r.is_active AS VARCHAR)
        END
    )
""").fetchone()[0]

print(f"\nDifferences found: {diff_count} (should be 0)")
print("✅ Test passed!" if diff_count == 0 else "❌ Test failed!")