# Quote Character Issue Analysis

## The Problem

**Your example**: `"'-System-"` vs `"-System-"`
- Left value has a leading single quote: `'-System-`
- Right value doesn't: `-System-`
- These are being marked as different

## Root Cause

This appears to be a data quality issue from CSV imports/exports where quotes get partially stripped or added.

## Potential Solutions

### Option 1: TRIM All Leading/Trailing Quotes (Simple but Risky)
```sql
TRIM(value, '''"')
```
- **Pros**: Simple, handles both single and double quotes
- **Cons**: Removes ALL quotes, even legitimate ones
- **Risk**: MEDIUM - Could corrupt data like `"O'Brien"` → `O'Brien`

### Option 2: Strip MATCHING Quote Pairs Only (Safer)
```sql
CASE
    WHEN value LIKE '''%''' AND LENGTH(value) > 2 THEN 
        SUBSTR(value, 2, LENGTH(value) - 2)
    WHEN value LIKE '"%"' AND LENGTH(value) > 2 THEN
        SUBSTR(value, 2, LENGTH(value) - 2)
    ELSE value
END
```
- **Pros**: Only removes quotes when they appear at BOTH start and end
- **Cons**: Won't handle your specific case (single quote only at start)
- **Risk**: LOW - Very conservative

### Option 3: Strip Leading/Trailing Quotes Separately (Best for Your Case)
```sql
REGEXP_REPLACE(
    REGEXP_REPLACE(value, '^[''"]+', '', 'g'),  -- Remove leading quotes
    '[''"]+$', '', 'g'                           -- Remove trailing quotes
)
```
- **Pros**: Handles partial quotes like your case
- **Cons**: More aggressive, could remove intentional quotes
- **Risk**: MEDIUM-HIGH - Could affect legitimate data

### Option 4: Simple TRIM Approach (Recommended)
Since TRIM already works for your case (as shown in test):
```sql
TRIM(value, '''"')
```
This successfully makes `'-System-` equal to `-System-`

## Test Results

From our testing:
- `'-System-` vs `-System-` → **Different** normally
- After TRIM: Both become `-System-` → **Equal** ✅
- `"Value"` vs `Value` → After TRIM both become `Value` → **Equal** ✅

## Risk Assessment

### For Your Specific Case: `'-System-`

**Risk Level**: LOW-MEDIUM

**Why it's probably safe**:
1. System-generated values like "-System-" shouldn't have quotes as part of the data
2. This looks like a CSV import artifact
3. The quote is only on one side (not part of the actual value)

**Potential Issues**:
1. If someone's name legitimately starts/ends with a quote (very rare)
2. If quotes are used as data markers (unlikely for your data)

## Recommendation

### ✅ IMPLEMENT Option 4: Simple TRIM

**Implementation in comparator.py**:
In the string comparison section, add quote trimming:
```python
# In the ELSE clause for string comparison
TRIM(LOWER(TRIM(TRY_CAST(l.{norm_col} AS VARCHAR))), '''\"') != 
TRIM(LOWER(TRIM(TRY_CAST(r.{norm_right_col} AS VARCHAR))), '''\"')
```

**Why this is the best choice**:
1. Solves your immediate problem
2. Common issue in data imports
3. TRIM is well-tested and predictable
4. Low risk for system-generated values like "-System-"
5. Already proven to work in our tests

**Risk Mitigation**:
- Only applied to string comparisons (not numeric/date)
- Applied symmetrically to both sides
- Standard SQL function (no complex regex)

## Implementation Priority

**Priority**: MEDIUM
- Less critical than date/currency issues
- But common enough to warrant fixing
- Safe to implement with TRIM approach