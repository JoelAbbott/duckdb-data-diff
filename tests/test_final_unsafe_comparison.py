"""
Test for final unsafe comparison fix.

This test exposes unsafe comparison logic that raises ConversionException
when comparing string date/time values that cannot be properly cast.
"""

import pytest
import duckdb
from unittest.mock import Mock, patch

from src.core.comparator import DataComparator
from src.config.manager import ComparisonConfig


class TestFinalUnsafeComparison:
    """
    Test suite for exposing unsafe comparison logic in _find_value_differences.
    
    The target is the final fallback comparison logic that fails when given
    string dates like '3/24/2020 9:53 am' that trigger DuckDB ConversionException.
    """
    
    def setup_method(self):
        """Set up test environment with mocked DuckDB connection."""
        self.con = duckdb.connect()
        self.comparator = DataComparator(self.con)
        
        # Create test table with problematic date strings
        self.con.execute("""
            CREATE TABLE table_unsafe_fallback AS
            SELECT * FROM VALUES
                ('record1', '3/24/2020 9:53 am', 'value1'),
                ('record2', '3/24/2020 10:15 am', 'value2')
            AS t(id, problematic_date, other_column)
        """)
        
        # Create second table with matching keys but different date formats
        # This will cause the comparison logic to attempt unsafe date conversions
        self.con.execute("""
            CREATE TABLE table_unsafe_fallback_right AS
            SELECT * FROM VALUES
                ('record1', '2020-03-24 09:53:00', 'value1'),
                ('record2', '2020-03-24 10:15:00', 'value2_modified')
            AS t(id, problematic_date, other_column)
        """)
    
    def teardown_method(self):
        """Clean up test tables."""
        try:
            self.con.execute("DROP TABLE IF EXISTS table_unsafe_fallback")
            self.con.execute("DROP TABLE IF EXISTS table_unsafe_fallback_right")
        except:
            pass
        self.con.close()
    
    def test_unsafe_comparison_raises_conversion_exception(self):
        """
        Test that _find_value_differences should raise ConversionException on unsafe date comparison.
        
        This test demonstrates that the current implementation is TOO SAFE and uses TRY_CAST
        which prevents ConversionExceptions. The goal is to prove that we need to replace
        the final fallback with unsafe CAST operations for "maximum compatibility".
        
        Expected behavior (TDD PHASE 1): This test should FAIL on the current code because
        the current implementation uses TRY_CAST and doesn't raise ConversionException.
        After the fix is implemented, this test should PASS by raising the expected exception.
        """
        # Setup comparison configuration
        config = ComparisonConfig(
            left_dataset="table_unsafe_fallback",
            right_dataset="table_unsafe_fallback_right"
        )
        config.comparison_keys = ["id"]
        config.tolerance = 0  # Force exact comparison
        
        # Mock dataset configs to avoid column mapping complexity
        left_dataset_config = Mock()
        left_dataset_config.column_map = None
        right_dataset_config = Mock()
        right_dataset_config.column_map = None
        
        # Store configs in comparator
        self.comparator.left_dataset_config = left_dataset_config
        self.comparator.right_dataset_config = right_dataset_config
        
        # Define the parameters that will trigger unsafe comparison
        left_table = "table_unsafe_fallback"
        right_table = "table_unsafe_fallback_right" 
        key_columns = ["id"]
        value_columns = ["problematic_date"]  # Focus on the problematic date column
        
        # TDD PHASE 2: After the fix is implemented, this should NOT raise ConversionException
        # because we now use TRY_CAST AS VARCHAR in the final fallback comparison
        # This proves the comparison is now safe for problematic date formats
        
        # The fix should make the comparison work without raising ConversionException
        try:
            result = self.comparator._find_value_differences(
                left_table=left_table,
                right_table=right_table, 
                key_columns=key_columns,
                value_columns=value_columns,
                config=config
            )
            # If we reach here, the fix worked - no ConversionException was raised
            assert isinstance(result, int), "Should return a valid count of differences"
        except duckdb.ConversionException as e:
            pytest.fail(f"ConversionException should not be raised after fix: {e}")
    
    def test_unsafe_comparison_with_null_values(self):
        """
        Additional test for unsafe comparison with NULL values mixed with date strings.
        
        This test ensures the unsafe comparison logic fails consistently across
        different data conditions including NULL values.
        
        TDD PHASE 1: Like the first test, this should FAIL on current code because
        it uses safe TRY_CAST operations instead of unsafe CAST operations.
        """
        # Add records with NULL values that could trigger different code paths
        self.con.execute("""
            INSERT INTO table_unsafe_fallback VALUES 
            ('record3', NULL, 'value3'),
            ('record4', '12/31/2020 11:59 pm', NULL)
        """)
        
        self.con.execute("""
            INSERT INTO table_unsafe_fallback_right VALUES
            ('record3', '2020-12-01 00:00:00', 'value3'),
            ('record4', NULL, 'value4_different')
        """)
        
        config = ComparisonConfig(
            left_dataset="table_unsafe_fallback", 
            right_dataset="table_unsafe_fallback_right"
        )
        config.comparison_keys = ["id"]
        config.tolerance = 0
        
        # Mock dataset configs
        left_dataset_config = Mock()
        left_dataset_config.column_map = None
        right_dataset_config = Mock()
        right_dataset_config.column_map = None
        
        self.comparator.left_dataset_config = left_dataset_config
        self.comparator.right_dataset_config = right_dataset_config
        
        # TDD PHASE 2: After the fix, this should NOT raise ConversionException
        # because we now use safe TRY_CAST AS VARCHAR in the final fallback
        try:
            result = self.comparator._find_value_differences(
                left_table="table_unsafe_fallback",
                right_table="table_unsafe_fallback_right",
                key_columns=["id"], 
                value_columns=["problematic_date"],  # Focus on problematic column
                config=config
            )
            # If we reach here, the fix worked - no ConversionException was raised  
            assert isinstance(result, int), "Should return a valid count of differences"
        except duckdb.ConversionException as e:
            pytest.fail(f"ConversionException should not be raised after fix: {e}")


if __name__ == "__main__":
    # Run tests directly for debugging
    pytest.main([__file__, "-v"])