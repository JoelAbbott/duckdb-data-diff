"""
Test the quote character issue: "'-System-" vs "-System-"
"""
import duckdb

con = duckdb.connect(":memory:")

print("=" * 60)
print("Issue: Leading quote character")
print("'-System- vs -System-")
print("-" * 60)

# Test the values
result = con.execute("""
    WITH test_data AS (
        SELECT 
            ''''-System-' as val1,  -- Note: four quotes to escape properly
            '-System-' as val2
    )
    SELECT 
        val1,
        val2,
        val1 = val2 as direct_equal,
        LENGTH(val1) as len1,
        LENGTH(val2) as len2,
        -- Try removing leading/trailing quotes
        TRIM(val1, '''') as trimmed_val1,
        TRIM(val2, '''') as trimmed_val2,
        TRIM(val1, '''') = TRIM(val2, '''') as trimmed_equal,
        -- Try more aggressive quote removal
        REGEXP_REPLACE(val1, '^[''\"]+|[''\"]+$', '', 'g') as regex_cleaned1,
        REGEXP_REPLACE(val2, '^[''\"]+|[''\"]+$', '', 'g') as regex_cleaned2
""").fetchone()

print(f"Value 1: '{result[0]}'")
print(f"Value 2: '{result[1]}'")
print(f"Direct equal: {result[2]}")
print(f"Length 1: {result[3]}")
print(f"Length 2: {result[4]}")
print(f"Trimmed 1: '{result[5]}'")
print(f"Trimmed 2: '{result[6]}'")
print(f"Trimmed equal: {result[7]}")
print(f"Regex cleaned 1: '{result[8]}'")
print(f"Regex cleaned 2: '{result[9]}'")

print("\n" + "=" * 60)
print("Testing various quote scenarios")
print("-" * 60)

test_cases = [
    ("'-System-", "-System-"),
    ("'Value'", "Value"),
    ('"Value"', "Value"),
    ("''Value''", "Value"),
    ('""Value""', "Value"),
    ("'John's'", "John's"),  # Apostrophe inside
    ('"Say "Hi""', 'Say "Hi"'),  # Quotes inside
]

for left, right in test_cases:
    # Need to escape quotes for SQL
    left_escaped = left.replace("'", "''")
    right_escaped = right.replace("'", "''")
    
    sql = f"""
        SELECT 
            '{left_escaped}' as left_val,
            '{right_escaped}' as right_val,
            '{left_escaped}' = '{right_escaped}' as equal,
            TRIM('{left_escaped}', '''\"') = TRIM('{right_escaped}', '''\"') as trim_equal,
            REGEXP_REPLACE('{left_escaped}', '^[''\"]+|[''\"]+$', '', 'g') = 
            REGEXP_REPLACE('{right_escaped}', '^[''\"]+|[''\"]+$', '', 'g') as regex_equal
    """
    
    try:
        result = con.execute(sql).fetchone()
        print(f"{left:20} vs {right:20} -> Equal: {result[2]}, Trim: {result[3]}, Regex: {result[4]}")
    except Exception as e:
        print(f"{left:20} vs {right:20} -> Error: {e}")

print("\n" + "=" * 60)
print("RISK ASSESSMENT")
print("=" * 60)

print("""
Quote Character Issue: "'-System-" vs "-System-"

PROBLEM: 
- Leading/trailing quotes in data
- Could be single quotes (') or double quotes (")
- Sometimes quotes are part of the actual data (e.g., "John's")

SOLUTIONS:

1. Strip Leading/Trailing Quotes Only (MEDIUM RISK)
   - Use TRIM(value, '''"') or regex
   - Risk: Could remove quotes that are meant to be there
   - Example: "'John's Company'" becomes "John's Company" ✓
   - Example: "O'Brien" stays "O'Brien" ✓
   
2. Strip Only Matching Quote Pairs (LOWER RISK)
   - If starts with ' and ends with ', remove both
   - If starts with " and ends with ", remove both
   - Otherwise leave alone
   - More conservative approach

3. Do Nothing (SAFEST)
   - These might be legitimate differences
   - The quote might be intentional in the data
   
RECOMMENDATION:
- Implement Option 2 (Strip matching quote pairs)
- Only remove quotes if they appear at both start AND end
- Preserve internal quotes
- This handles most CSV export issues without data corruption
""")

# Test the proposed solution
print("\n" + "=" * 60)
print("Testing Proposed Solution (Strip Matching Quote Pairs)")
print("-" * 60)

# Create a function-like SQL expression for the solution
solution_sql = """
    WITH test_data AS (
        SELECT column1 as original, column2 as expected
        FROM (VALUES
            (''''-System-', '-System-'),
            ('"-System-"', '-System-'),
            ('''John''s''', 'John''s'),
            ('O''Brien', 'O''Brien'),
            ('Normal Text', 'Normal Text')
        ) AS t(column1, column2)
    )
    SELECT 
        original,
        expected,
        CASE
            -- If wrapped in single quotes
            WHEN original LIKE '''%''' AND LENGTH(original) > 2 THEN 
                SUBSTR(original, 2, LENGTH(original) - 2)
            -- If wrapped in double quotes
            WHEN original LIKE '"%"' AND LENGTH(original) > 2 THEN
                SUBSTR(original, 2, LENGTH(original) - 2)
            -- Otherwise keep as is
            ELSE original
        END as cleaned,
        CASE
            WHEN original LIKE '''%''' AND LENGTH(original) > 2 THEN 
                SUBSTR(original, 2, LENGTH(original) - 2)
            WHEN original LIKE '"%"' AND LENGTH(original) > 2 THEN
                SUBSTR(original, 2, LENGTH(original) - 2)
            ELSE original
        END = expected as matches_expected
    FROM test_data
"""

print("\nProposed solution results:")
results = con.execute(solution_sql).fetchall()
for row in results:
    print(f"Original: {row[0]:20} -> Cleaned: {row[2]:20} Expected: {row[1]:20} Match: {row[3]}")