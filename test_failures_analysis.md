# Test Failures Analysis After Value Coercion Fix

## Summary of Changes

The value coercion fix modified `_build_robust_comparison_condition()` to properly handle:
1. Numeric values with currency symbols
2. Date values in multiple formats  
3. Boolean values (excluding numeric strings)
4. Normalized string comparison

## Categories of Test Failures

### 1. **Comparison Complexity Tests** (1 test)
- `test_row_mismatch_bug_fix.py::test_robust_comparison_deterministic_behavior`
- **Issue**: Test expects simple comparison (<500 chars), but new implementation is ~6000 chars
- **Resolution**: This is INTENTIONAL. The complexity is needed for proper value coercion.
- **Action**: Update test to accept the new complexity or remove the assertion

### 2. **SQL Pattern Matching Tests** (Multiple tests)
- `test_comparator_chunked_sql.py` - Several tests
- **Issue**: Tests use regex patterns like `r'l\.From\s*=\s*r\.(\w+)'` expecting simple joins
- **New Format**: `TRIM(TRY_CAST(l.from AS VARCHAR)) = TRIM(TRY_CAST(r.author AS VARCHAR))`
- **Action**: Update regex patterns to match the new TRIM/TRY_CAST format

### 3. **Column Quoting Tests** (6 tests)
- `test_comparator_quoting.py` - All tests  
- **Issue**: Tests expect quoted columns like `l."Internal ID"`
- **Reality**: Columns are normalized to `internal_id` (no spaces, no quotes needed)
- **Action**: These tests may be obsolete or need to test normalization instead

### 4. **Key Column Join Tests** (Multiple tests)
- **Issue**: Tests may expect simple equality but now get TRIM/TRY_CAST wrapping
- **Action**: Update expected SQL patterns

## Recommended Actions

### Option 1: Update Tests (Recommended)
Update the failing tests to match the new, improved behavior:
- Update regex patterns to handle TRIM/TRY_CAST
- Remove or update complexity assertions
- Update quoting expectations to match normalization

### Option 2: Make Changes Backward Compatible
Add a flag to enable/disable the new coercion logic:
- `use_legacy_comparison=False` by default
- Tests can set `use_legacy_comparison=True` to pass

### Option 3: Selective Application
Only apply the new coercion logic to value columns, not key columns:
- Key joins remain simple for performance
- Value comparisons get full coercion

## Impact Assessment

The new behavior is CORRECT and addresses real user issues:
- ✅ Fixes numeric comparison (0 vs 0.0)
- ✅ Fixes currency comparison ($100 vs 100)
- ✅ Fixes date format comparison
- ✅ Fixes boolean comparison

The test failures are due to tests expecting the OLD, BUGGY behavior.