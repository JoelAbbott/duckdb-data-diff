"""
Test the remaining value coercion issues
"""
import duckdb

con = duckdb.connect(":memory:")

# Test Issue 1: Date with time "0:00" format
print("=" * 60)
print("Issue 1: Date format '1/20/2025' vs '1/20/2025 0:00'")
print("-" * 60)

result = con.execute("""
    SELECT 
        -- Try parsing '1/20/2025'
        TRY_STRPTIME('1/20/2025', '%m/%d/%Y') as date1,
        -- Try parsing '1/20/2025 0:00'
        TRY_STRPTIME('1/20/2025 0:00', '%m/%d/%Y') as date2_fail,
        TRY_STRPTIME('1/20/2025 0:00', '%m/%d/%Y %H:%M') as date2_success,
        -- Check if they're equal when parsed
        TRY_STRPTIME('1/20/2025', '%m/%d/%Y') = 
        TRY_STRPTIME('1/20/2025 0:00', '%m/%d/%Y %H:%M') as are_equal
""").fetchone()

print(f"Date 1: {result[0]}")
print(f"Date 2 (no pattern): {result[1]}")
print(f"Date 2 (with pattern): {result[2]}")
print(f"Are equal: {result[3]}")

# Test Issue 2: Currency with space
print("\n" + "=" * 60)
print("Issue 2: Currency '-€ 30,848.00' vs '-30848.0'")
print("-" * 60)

# Test current regex
result = con.execute("""
    WITH test_data AS (
        SELECT '-€ 30,848.00' as val1, '-30848.0' as val2
    )
    SELECT 
        val1,
        val2,
        -- Current approach (doesn't handle space after currency)
        REGEXP_REPLACE(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(val1, '[$£€¥₪₹¢]', '', 'g'),
                    ',', '', 'g'
                ),
                '^\\(', '-', 'g'
            ),
            '\\)$', '', 'g'
        ) as cleaned_v1,
        -- Better approach: handle currency with optional spaces
        REGEXP_REPLACE(
            REGEXP_REPLACE(
                REGEXP_REPLACE(val1, '[-]?[\\s]*[€$£¥₪₹¢][\\s]*', '', 'g'),
                ',', '', 'g'
            ),
            '^\\s*-\\s*', '-', 'g'
        ) as better_cleaned
    FROM test_data
""").fetchone()

print(f"Original: {result[0]}")
print(f"Expected: {result[1]}")
print(f"Current cleaning: {result[2]}")
print(f"Better cleaning: {result[3]}")

# Test better regex that handles negative currency with space
result = con.execute("""
    SELECT 
        -- Test various currency formats
        TRIM(REGEXP_REPLACE(
            REGEXP_REPLACE('-€ 30,848.00', '[€$£¥₪₹¢\\s]', '', 'g'),
            ',', '', 'g'
        )) as cleaned1,
        TRIM(REGEXP_REPLACE(
            REGEXP_REPLACE('- € 30,848.00', '[€$£¥₪₹¢\\s]', '', 'g'),
            ',', '', 'g'
        )) as cleaned2,
        TRY_CAST(
            TRIM(REGEXP_REPLACE(
                REGEXP_REPLACE('-€ 30,848.00', '[€$£¥₪₹¢\\s]', '', 'g'),
                ',', '', 'g'
            )) AS DOUBLE
        ) as as_number
""").fetchone()

print(f"\nCleaned with space removal: '{result[0]}'")
print(f"Cleaned with extra space: '{result[1]}'")
print(f"As number: {result[2]}")

# Test Issue 3: HTML entities
print("\n" + "=" * 60)
print("Issue 3: HTML entities '>' vs '&gt;'")
print("-" * 60)

result = con.execute("""
    SELECT 
        'DH5-GENA Batteries replacement (>3 years)' as original,
        'DH5-GENA Batteries replacement (&gt;3 years)' as with_entity,
        -- Decode common HTML entities
        REPLACE(
            REPLACE(
                REPLACE(
                    REPLACE(
                        REPLACE('DH5-GENA Batteries replacement (&gt;3 years)',
                            '&gt;', '>'),
                        '&lt;', '<'),
                    '&amp;', '&'),
                '&quot;', '"'),
            '&apos;', '''') as decoded,
        -- Check if they match after decoding
        'DH5-GENA Batteries replacement (>3 years)' = 
        REPLACE('DH5-GENA Batteries replacement (&gt;3 years)', '&gt;', '>') as matches
""").fetchone()

print(f"Original: {result[0]}")
print(f"With entity: {result[1]}")
print(f"Decoded: {result[2]}")
print(f"Matches: {result[3]}")

print("\n" + "=" * 60)
print("RISK ASSESSMENT")
print("=" * 60)

print("""
1. Date format '1/20/2025 0:00': 
   - SAFE FIX: Add pattern '%m/%d/%Y %H:%M' to date parsing
   - LOW RISK: This is a common format, unlikely to cause issues
   
2. Currency with space '-€ 30,848.00':
   - MEDIUM RISK: Need to handle space between negative and currency
   - SAFE APPROACH: Include optional spaces in currency regex
   - Be careful with the order of operations (negative sign handling)
   
3. HTML entities '&gt;' etc:
   - HIGH RISK: Could affect legitimate data containing these strings
   - RECOMMENDATION: DO NOT auto-decode HTML entities
   - This should be handled during data staging/cleaning, not comparison
   - If needed, make it an optional flag
""")