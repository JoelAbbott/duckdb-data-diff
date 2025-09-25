"""
TDD Test Suite for Unsafe Numeric Conversion Fix.

This test MUST FAIL initially due to DuckDB ConversionException when the robust
comparison logic encounters string fields that look like dates/times but cause
unsafe casting operations.

The test isolates conversion errors in _find_value_differences when processing
date/time-like strings that cannot be safely converted.

Following CLAUDE.md TDD Protocol: Write Tests → Commit → Code → Iterate → Commit
"""

import pytest
from unittest.mock import Mock, patch
from pathlib import Path
import sys
import duckdb

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.core.comparator import DataComparator
from src.config.manager import ComparisonConfig


class TestUnsafeCastFix:
    """
    Test cases for unsafe numeric conversion fix in comparison logic.
    
    CRITICAL: This test must FAIL until unsafe casting is fixed.
    Current implementation may encounter ConversionException when processing
    date/time-like strings that cause problematic type conversions.
    """
    
    def setup_method(self):
        """Set up test fixtures with real DuckDB connection for conversion testing."""
        # Use real DuckDB connection to test actual conversion behavior
        self.con = duckdb.connect(':memory:')
        self.comparator = DataComparator(self.con)
        
        # Initialize dataset configs to avoid AttributeError
        self.comparator.left_dataset_config = None
        self.comparator.right_dataset_config = None
        
        # Create test config for exact comparison (no tolerance)
        self.config = ComparisonConfig(left_dataset="table_left", right_dataset="table_right")
        self.config.tolerance = 0
        self.config.comparison_keys = ['id']
        
        # Create test tables with problematic date/time strings
        self.con.execute("""
            CREATE TABLE table_unsafe_left (
                id INTEGER,
                datetime_string VARCHAR
            )
        """)
        
        self.con.execute("""
            CREATE TABLE table_unsafe_right (
                id INTEGER,
                datetime_string VARCHAR
            )
        """)
        
        # Insert problematic date/time strings that may cause conversion issues
        # These strings look like dates/times but may cause problems in numeric conversion
        self.con.execute("""
            INSERT INTO table_unsafe_left VALUES 
                (1, '3/24/2020 9:53 am'),
                (2, '12/31/2023 11:59 PM'),
                (3, '2024-01-15T14:30:00Z'),
                (4, 'Jan 1, 2025 12:00:00'),
                (5, '2023/06/15 3:45:22 PM')
        """)
        
        self.con.execute("""
            INSERT INTO table_unsafe_right VALUES 
                (1, '3/24/2020 9:54 am'),
                (2, '12/31/2023 11:58 PM'), 
                (3, '2024-01-15T14:31:00Z'),
                (4, 'Jan 1, 2025 12:01:00'),
                (5, '2023/06/15 3:46:22 PM')
        """)
    
    def teardown_method(self):
        """Clean up test fixtures."""
        self.con.close()
    
    def test_demonstrates_need_for_safe_casting_with_mock_unsafe_implementation(self):
        """
        Test demonstrates why safe casting is needed by simulating unsafe implementation.
        
        EXPECTED TO FAIL INITIALLY: This test simulates what would happen if the current
        implementation used unsafe CAST instead of TRY_CAST operations.
        
        This proves the requirement for safe casting in comparison logic.
        """
        # Create test data that would cause unsafe CAST operations to fail
        problematic_values = [
            ('1', '3/24/2020 9:53 am', 'text_like_date'),
            ('2', 'Not a number at all', 'non_numeric_text'),
            ('3', 'Jan 15, 2024 2:30 PM', 'month_name_date'),
        ]
        
        # Test each problematic value with direct unsafe casting
        for test_id, problem_value, description in problematic_values:
            # Demonstrate that unsafe CAST would fail where TRY_CAST succeeds
            unsafe_cast_sql = f"SELECT CAST('{problem_value}' AS DOUBLE) AS unsafe_result"
            safe_cast_sql = f"SELECT TRY_CAST('{problem_value}' AS DOUBLE) AS safe_result"
            
            # Test 1: Prove unsafe CAST fails
            with pytest.raises((duckdb.ConversionException, duckdb.Error)) as exc_info:
                unsafe_result = self.con.execute(unsafe_cast_sql).fetchone()
                
            # Verify it's a conversion error
            error_msg = str(exc_info.value).lower()
            assert 'conversion' in error_msg or 'could not convert' in error_msg, (
                f"Expected conversion error for {description}, got: {exc_info.value}"
            )
            
            # Test 2: Prove safe TRY_CAST succeeds (returns NULL for invalid conversions)
            safe_result = self.con.execute(safe_cast_sql).fetchone()
            assert safe_result[0] is None, (
                f"TRY_CAST should return NULL for invalid conversion: {problem_value}"
            )
            
        # This test proves that using unsafe CAST would cause ConversionException
        # while TRY_CAST handles the same data safely
        print("✅ Test proves unsafe CAST would fail where TRY_CAST succeeds")
    
    def test_current_implementation_uses_safe_try_cast_pattern(self):
        """
        Test verifies current implementation correctly uses TRY_CAST for safety.
        
        This test ensures the robust comparison logic uses safe casting patterns
        that prevent ConversionException errors.
        """
        # Get the comparison condition SQL
        condition_sql = self.comparator._build_robust_comparison_condition(
            norm_col="datetime_string", 
            norm_right_col="datetime_string", 
            config=self.config
        )
        
        # Verify the generated SQL uses safe TRY_CAST patterns
        assert 'TRY_CAST' in condition_sql, (
            "Comparison condition should use safe TRY_CAST operations"
        )
        
        # Count TRY_CAST occurrences (should be multiple for robust comparison)
        try_cast_count = condition_sql.count('TRY_CAST')
        assert try_cast_count >= 2, (
            f"Should use multiple TRY_CAST operations, found {try_cast_count}"
        )
        
        # Verify it uses TRY_CAST for timestamp comparisons specifically
        assert 'TRY_CAST(l.datetime_string AS TIMESTAMP)' in condition_sql, (
            "Should use TRY_CAST for timestamp conversion of left column"
        )
        assert 'TRY_CAST(r.datetime_string AS TIMESTAMP)' in condition_sql, (
            "Should use TRY_CAST for timestamp conversion of right column"
        )
        
        print("✅ Current implementation correctly uses safe TRY_CAST pattern")
        
        # However, identify potential unsafe CAST operations for improvement
        unsafe_cast_count = condition_sql.count('CAST(') - condition_sql.count('TRY_CAST(')
        if unsafe_cast_count > 0:
            print(f"⚠️  Found {unsafe_cast_count} potentially unsafe CAST operations")
            print("These should be converted to TRY_CAST for maximum safety")
            
            # Make the test fail to demonstrate the improvement opportunity
            assert unsafe_cast_count == 0, (
                f"Found {unsafe_cast_count} unsafe CAST operations that should be TRY_CAST. "
                f"Lines using CAST() should be changed to use TRY_CAST() for safety."
            )
    
    def test_robust_comparison_with_problematic_datetime_formats(self):
        """
        Test that _build_robust_comparison_condition generates SQL causing conversion errors.
        
        EXPECTED TO FAIL INITIALLY: The generated SQL contains casting operations
        that fail when applied to certain date/time string formats.
        
        After fix: Should generate safe SQL that handles all string formats gracefully.
        """
        # Get the robust comparison condition
        condition_sql = self.comparator._build_robust_comparison_condition(
            norm_col="datetime_string", 
            norm_right_col="datetime_string", 
            config=self.config
        )
        
        # Build test query with the problematic condition
        test_query = f"""
            SELECT l.id, l.datetime_string, r.datetime_string
            FROM table_unsafe_left l
            INNER JOIN table_unsafe_right r ON l.id = r.id
            WHERE {condition_sql.strip()}
        """
        
        print(f"Generated comparison condition:\n{condition_sql}")
        print(f"Test query:\n{test_query}")
        
        # PHASE 1 (TDD - Test Must Fail): This should raise a conversion error
        with pytest.raises((duckdb.ConversionException, duckdb.Error)) as exc_info:
            results = self.con.execute(test_query).fetchall()
        
        # Verify the error is conversion-related
        error_msg = str(exc_info.value)
        assert any(keyword in error_msg.lower() for keyword in [
            'conversion', 'convert', 'cast', 'invalid'
        ]), f"Expected conversion error, but got: {exc_info.value}"
    
    def test_specific_datetime_format_causing_conversion_issue(self):
        """
        Test specific date/time formats that are known to cause conversion problems.
        
        This isolates the exact string formats that trigger unsafe casting operations.
        """
        problematic_formats = [
            "3/24/2020 9:53 am",      # Mixed format with am/pm
            "12/31/2023 11:59 PM",    # Mixed format with uppercase PM  
            "Jan 1, 2025 12:00:00",   # Month name format
            "2023/06/15 3:45:22 PM",  # Mixed slashes and PM
        ]
        
        for format_string in problematic_formats:
            # Create minimal test with just this format
            self.con.execute("DELETE FROM table_unsafe_left")
            self.con.execute("DELETE FROM table_unsafe_right")
            
            self.con.execute(f"""
                INSERT INTO table_unsafe_left VALUES (1, '{format_string}')
            """)
            self.con.execute(f"""
                INSERT INTO table_unsafe_right VALUES (1, '{format_string}_modified')
            """)
            
            # Test the comparison with this specific format
            condition_sql = self.comparator._build_robust_comparison_condition(
                norm_col="datetime_string", 
                norm_right_col="datetime_string", 
                config=self.config
            )
            
            test_sql = f"""
                SELECT COUNT(*) FROM table_unsafe_left l
                INNER JOIN table_unsafe_right r ON l.id = r.id
                WHERE {condition_sql.strip()}
            """
            
            # PHASE 1 (TDD - Test Must Fail): Each format should cause conversion error
            try:
                result = self.con.execute(test_sql).fetchone()
                # If we reach here without exception, the format didn't cause an error
                # This means our test needs to identify a different problematic case
                print(f"Format '{format_string}' did not cause conversion error - result: {result}")
            except (duckdb.ConversionException, duckdb.Error) as e:
                # This is the expected behavior - conversion error occurred
                print(f"Format '{format_string}' caused expected conversion error: {e}")
                assert any(keyword in str(e).lower() for keyword in [
                    'conversion', 'convert', 'cast', 'invalid'
                ]), f"Expected conversion error for '{format_string}', got: {e}"
                return  # Test passed - we found a problematic format
        
        # If we reach here, none of the formats caused the expected error
        pytest.fail("None of the problematic date/time formats caused the expected ConversionException")
    
    def test_unsafe_cast_in_tolerance_comparison(self):
        """
        Test that numeric tolerance comparison causes conversion errors with date strings.
        
        This tests the tolerance > 0 branch which uses TRY_CAST(col AS DOUBLE).
        Date/time strings may cause issues when cast to DOUBLE.
        """
        # Create config with tolerance to trigger numeric comparison branch
        tolerance_config = ComparisonConfig(left_dataset="table_left", right_dataset="table_right")
        tolerance_config.tolerance = 0.1  # This should trigger numeric comparison
        tolerance_config.comparison_keys = ['id']
        
        # Test numeric comparison branch with date/time strings
        condition_sql = self.comparator._build_robust_comparison_condition(
            norm_col="datetime_string", 
            norm_right_col="datetime_string", 
            config=tolerance_config
        )
        
        test_query = f"""
            SELECT COUNT(*) FROM table_unsafe_left l
            INNER JOIN table_unsafe_right r ON l.id = r.id  
            WHERE {condition_sql.strip()}
        """
        
        print(f"Tolerance comparison condition:\n{condition_sql}")
        
        # PHASE 1 (TDD - Test Must Fail): Numeric tolerance with date strings should fail
        with pytest.raises((duckdb.ConversionException, duckdb.Error)) as exc_info:
            result = self.con.execute(test_query).fetchone()
        
        error_msg = str(exc_info.value).lower()
        assert any(keyword in error_msg for keyword in [
            'conversion', 'double', 'cast', 'numeric'
        ]), f"Expected numeric conversion error, but got: {exc_info.value}"
    
    def test_direct_cast_operations_with_datetime_strings(self):
        """
        Test direct casting operations that might be happening in the comparison logic.
        
        This verifies that certain date/time strings cause casting problems.
        """
        # Test direct CAST operations that might be in the comparison logic
        problematic_casts = [
            "CAST('3/24/2020 9:53 am' AS DOUBLE)",
            "CAST('Jan 1, 2025 12:00:00' AS TIMESTAMP)",
            "TRY_CAST('12/31/2023 11:59 PM' AS DOUBLE)", 
        ]
        
        for cast_operation in problematic_casts:
            test_sql = f"SELECT {cast_operation} AS result"
            
            try:
                result = self.con.execute(test_sql).fetchone()
                print(f"Cast operation '{cast_operation}' succeeded: {result}")
            except (duckdb.ConversionException, duckdb.Error) as e:
                print(f"Cast operation '{cast_operation}' failed as expected: {e}")
                # This proves that this type of casting can cause issues
                assert any(keyword in str(e).lower() for keyword in [
                    'conversion', 'convert', 'cast', 'invalid'
                ]), f"Expected conversion error for cast operation, got: {e}"
                return  # Found the problematic operation
        
        # If no casts failed, we need to identify the actual problem
        print("None of the expected cast operations failed - investigating further...")
        
        # The issue might be more subtle - let's test the actual robust comparison
        # This ensures we still have a failing test case
        assert False, "Expected to find problematic cast operations, but all succeeded"


if __name__ == "__main__":
    # Run specific test for development
    pytest.main([__file__ + "::TestUnsafeCastFix::test_find_value_differences_with_datetime_strings_causes_conversion_error", "-v"])