# Remaining Value Coercion Issues - Implementation Plan

## Executive Summary

Three remaining false positive issues identified:
1. Date with time format: "1/20/2025" vs "1/20/2025 0:00" 
2. Currency with space: "-€ 30,848.00" vs "-30848.0"
3. HTML entities: ">" vs "&gt;"

## Risk Assessment & Recommendations

### 1. Date Format Issue: "1/20/2025 0:00" ✅ SAFE TO FIX

**Problem**: The format "1/20/2025 0:00" doesn't match any of our current patterns

**Solution**: Add pattern `'%m/%d/%Y %H:%M'` to the date parsing COALESCE

**Risk Level**: LOW
- This is a standard date-time format
- Won't affect other comparisons
- Only adds an additional pattern to try

**Implementation**:
```sql
COALESCE(
    TRY_CAST(column AS TIMESTAMP),
    TRY_STRPTIME(TRY_CAST(column AS VARCHAR), '%m/%d/%Y'),
    TRY_STRPTIME(TRY_CAST(column AS VARCHAR), '%m/%d/%Y %H:%M'),  -- ADD THIS
    -- ... other patterns
)
```

### 2. Currency with Space: "-€ 30,848.00" ✅ SAFE TO FIX (with care)

**Problem**: Current regex doesn't handle:
- Space between negative sign and currency symbol
- Space after currency symbol

**Current Issue**:
- Input: `-€ 30,848.00`
- Current output: `- 30848.00` (space remains after negative)
- Expected: `-30848.00`

**Solution**: Improve currency regex to handle spaces better
- Remove currency symbols AND adjacent spaces
- Handle negative signs more robustly

**Risk Level**: MEDIUM-LOW
- Need careful regex to avoid removing legitimate spaces
- Should preserve negative sign position

**Implementation Approach**:
```python
# Step 1: Remove currency symbols with any adjacent spaces
REGEXP_REPLACE(value, '[\\s]*[€$£¥₪₹¢][\\s]*', '', 'g')

# Step 2: Clean up any remaining issues with negative signs
# Then remove commas, handle parentheses, etc.
```

### 3. HTML Entities: "&gt;" ❌ DO NOT FIX

**Problem**: HTML encoded characters like `&gt;` (>) appear in data

**Why NOT to fix**:
1. **High Risk**: Could affect legitimate data containing these strings
2. **Data Integrity**: The string "&gt;" might be intentional, not an encoding
3. **Scope Creep**: This is a data quality issue, not a comparison issue
4. **Unpredictable**: Many possible HTML entities, hard to cover all cases

**Recommendation**: 
- DO NOT implement automatic HTML entity decoding
- This should be handled during data import/staging if needed
- If absolutely necessary, make it an optional flag that's OFF by default

## Proposed Changes

### SAFE CHANGES TO IMPLEMENT:

1. **Add date pattern for "M/D/Y H:M" format**
   - Location: `comparator.py` line ~667-672 and ~679-684
   - Add: `TRY_STRPTIME(TRY_CAST(column AS VARCHAR), '%m/%d/%Y %H:%M')`

2. **Improve currency regex to handle spaces**
   - Location: `comparator.py` line ~626-642
   - Modify regex to: `'[\\s]*[€$£¥₪₹¢][\\s]*'` to remove currency with spaces
   - Ensure negative signs are preserved correctly

### CHANGES TO AVOID:

1. **HTML entity decoding** - Too risky, could break legitimate data

## Testing Requirements

Before implementing:
1. Test with actual data samples
2. Ensure no regression in existing comparisons
3. Verify performance impact is minimal
4. Test edge cases:
   - Multiple spaces
   - Different currency positions
   - Various date-time formats

## Decision Needed

Do you want to proceed with:
- [ ] Date format fix only (SAFEST)
- [ ] Date + Currency fixes (RECOMMENDED)
- [ ] All three including HTML entities (NOT RECOMMENDED)

## Implementation Priority

1. **First**: Date format - lowest risk, clear benefit
2. **Second**: Currency with spaces - medium risk, good benefit
3. **Never/Later**: HTML entities - high risk, questionable benefit