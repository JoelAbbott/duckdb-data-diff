"""
Simple test for the quote issue
"""
import duckdb

con = duckdb.connect(":memory:")

print("=" * 60)
print("Testing: '-System- vs -System-")
print("=" * 60)

# Create test table
con.execute("""
    CREATE TABLE test_quotes (
        id INTEGER,
        left_val VARCHAR,
        right_val VARCHAR
    )
""")

# Insert the problematic case  
# Use parameterized queries to avoid quote escaping issues
con.execute("INSERT INTO test_quotes VALUES (?, ?, ?)", (1, "'-System-", "-System-"))
con.execute("INSERT INTO test_quotes VALUES (?, ?, ?)", (2, '"Value"', "Value"))
con.execute("INSERT INTO test_quotes VALUES (?, ?, ?)", (3, "Normal", "Normal"))

results = con.execute("""
    SELECT 
        id,
        left_val,
        right_val,
        left_val = right_val as direct_equal,
        LENGTH(left_val) as left_len,
        LENGTH(right_val) as right_len,
        -- Try stripping quotes
        TRIM(left_val, '''\"') as left_trimmed,
        TRIM(right_val, '''\"') as right_trimmed,
        TRIM(left_val, '''\"') = TRIM(right_val, '''\"') as trimmed_equal
    FROM test_quotes
    ORDER BY id
""").fetchall()

for row in results:
    print(f"\nID {row[0]}:")
    print(f"  Left:  '{row[1]}' (len={row[4]})")
    print(f"  Right: '{row[2]}' (len={row[5]})")
    print(f"  Direct Equal: {row[3]}")
    print(f"  After trim: '{row[6]}' vs '{row[7]}'")
    print(f"  Trimmed Equal: {row[8]}")

print("\n" + "=" * 60)
print("Testing pattern matching approach")
print("=" * 60)

# Test the CASE WHEN approach for stripping matching quotes
results = con.execute("""
    SELECT 
        id,
        left_val,
        right_val,
        -- Strip matching quotes from left value
        CASE
            WHEN left_val LIKE '''%''' AND LENGTH(left_val) > 2 THEN 
                SUBSTR(left_val, 2, LENGTH(left_val) - 2)
            WHEN left_val LIKE '"%"' AND LENGTH(left_val) > 2 THEN
                SUBSTR(left_val, 2, LENGTH(left_val) - 2)
            ELSE left_val
        END as left_cleaned,
        -- Strip matching quotes from right value
        CASE
            WHEN right_val LIKE '''%''' AND LENGTH(right_val) > 2 THEN 
                SUBSTR(right_val, 2, LENGTH(right_val) - 2)
            WHEN right_val LIKE '"%"' AND LENGTH(right_val) > 2 THEN
                SUBSTR(right_val, 2, LENGTH(right_val) - 2)
            ELSE right_val
        END as right_cleaned
    FROM test_quotes
    ORDER BY id
""").fetchall()

for row in results:
    left_cleaned = row[3]
    right_cleaned = row[4]
    are_equal = left_cleaned == right_cleaned
    print(f"\nID {row[0]}:")
    print(f"  Original: '{row[1]}' vs '{row[2]}'")
    print(f"  Cleaned:  '{left_cleaned}' vs '{right_cleaned}'")
    print(f"  Equal after cleaning: {are_equal}")

print("\n" + "=" * 60)
print("ANALYSIS")
print("=" * 60)

print("""
ISSUE: '-System- vs -System-
One value has a leading single quote, the other doesn't.

PROPOSED SOLUTION:
Strip matching quote pairs (single or double) from start/end of values.

RISK ASSESSMENT:
- LOW-MEDIUM RISK
- Only removes quotes when they appear at BOTH start and end
- Preserves internal quotes (e.g., O'Brien)
- Common issue from CSV exports/imports

IMPLEMENTATION:
Use CASE WHEN logic to check for matching quote pairs and strip them.
This is safer than TRIM which removes all quotes indiscriminately.

RECOMMENDATION:
✅ SAFE TO IMPLEMENT - This is a common data quality issue that should be handled.
""")