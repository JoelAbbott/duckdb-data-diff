"""
Test suite for key column normalization in SQL generation.
This test validates that key columns with spaces/special characters are properly
normalized to match the staged table's snake_case column names.

CLAUDE.md: TDD approach - Write failing test first, then implement fix.
"""

import pytest
from unittest.mock import Mock, MagicMock, patch, call
import duckdb

from src.core.comparator import DataComparator
from src.config.manager import ComparisonConfig


class TestComparatorNormalizationFix:
    """Test suite for ensuring key columns are normalized in SQL generation."""
    
    def setup_method(self):
        """Set up test fixtures."""
        # Create mock DuckDB connection
        self.mock_con = MagicMock(spec=duckdb.DuckDBPyConnection)
        self.comparator = DataComparator(self.mock_con)
        
        # Create mock config
        self.mock_config = Mock(spec=ComparisonConfig)
        self.mock_config.comparison_keys = None
        self.mock_config.value_columns = None
        self.mock_config.tolerance = 0
        self.mock_config.max_differences = 1000
    
    def test_key_normalization_applied_in_sql_generation(self):
        """
        Test that key columns are normalized to snake_case in SQL generation.
        
        This test validates that when a key column like "Internal ID" is passed,
        the SQL JOIN condition uses the normalized "internal_id" column name
        that exists in the staged table.
        
        Expected behavior:
        - Input key: ["Internal ID"]
        - SQL should contain: l."internal_id" = r."internal_id"
        - SQL should NOT contain: l."Internal ID"
        """
        # Arrange
        left_table = "left_staged"
        right_table = "right_staged"
        
        # Simulate user-selected key with spaces (from Interactive Menu)
        unnormalized_keys = ["Internal ID"]
        
        # Mock the row count check to trigger chunked processing
        self.mock_con.execute.return_value.fetchone.return_value = [30000]  # > 25K threshold
        
        # Act: Call the chunked processing method with unnormalized keys
        with patch.object(self.comparator, '_get_row_count', return_value=30000):
            with patch.object(self.comparator, '_get_right_column', side_effect=lambda x: x):
                # Call _find_matches_chunked directly to inspect SQL generation
                total_matches = self.comparator._find_matches_chunked(
                    left_table, right_table, unnormalized_keys
                )
        
        # Assert: Verify the SQL contains normalized column names
        executed_calls = self.mock_con.execute.call_args_list
        
        # Find the chunk SQL call
        chunk_sql_found = False
        for call_obj in executed_calls:
            sql = call_obj[0][0] if call_obj[0] else None
            if sql and "INNER JOIN" in sql:
                chunk_sql_found = True
                
                # CRITICAL ASSERTION: SQL must use normalized "internal_id"
                assert 'l.internal_id' in sql.lower(), \
                    f"SQL should contain normalized column 'internal_id'. SQL: {sql}"
                
                # CRITICAL ASSERTION: SQL must NOT use original "Internal ID"
                assert 'l."internal id"' not in sql.lower(), \
                    f"SQL should NOT contain original column 'Internal ID'. SQL: {sql}"
                
                # Verify the JOIN condition structure
                assert 'l.internal_id = r.internal_id' in sql.lower(), \
                    f"JOIN should use normalized columns. SQL: {sql}"
        
        assert chunk_sql_found, "No SQL with INNER JOIN was executed"
    
    def test_multiple_keys_all_normalized_in_sql(self):
        """
        Test that multiple key columns are all normalized in SQL generation.
        
        Tests normalization of various column name patterns:
        - Spaces: "Internal ID" -> "internal_id"
        - Mixed case: "CustomerName" -> "customername"
        - Special chars: "Order-Number" -> "order_number"
        """
        # Arrange
        left_table = "left_staged"
        right_table = "right_staged"
        
        # Multiple keys with various naming patterns
        unnormalized_keys = ["Internal ID", "Customer Name", "Order-Date"]
        
        # Expected normalized versions
        expected_normalized = {
            "Internal ID": "internal_id",
            "Customer Name": "customer_name", 
            "Order-Date": "order_date"
        }
        
        # Mock the row count check
        self.mock_con.execute.return_value.fetchone.return_value = [30000]
        
        # Act: Call the method with multiple unnormalized keys
        with patch.object(self.comparator, '_get_row_count', return_value=30000):
            with patch.object(self.comparator, '_get_right_column', side_effect=lambda x: x):
                self.comparator._find_matches_chunked(
                    left_table, right_table, unnormalized_keys
                )
        
        # Assert: Verify all keys are normalized in SQL
        executed_calls = self.mock_con.execute.call_args_list
        
        for call_obj in executed_calls:
            sql = call_obj[0][0] if call_obj[0] else None
            if sql and "INNER JOIN" in sql:
                sql_lower = sql.lower()
                
                # Check each key is normalized
                for original, normalized in expected_normalized.items():
                    # Should contain normalized version
                    assert f'l.{normalized}' in sql_lower, \
                        f"SQL should contain normalized '{normalized}' for '{original}'. SQL: {sql}"
                    
                    # Should NOT contain original version
                    original_lower = original.lower()
                    assert f'l."{original_lower}"' not in sql_lower, \
                        f"SQL should NOT contain original '{original}'. SQL: {sql}"
    
    def test_find_only_in_left_uses_normalized_keys(self):
        """
        Test that _find_only_in_left_chunked uses normalized key columns.
        
        This validates that all comparison methods consistently normalize keys.
        """
        # Arrange
        left_table = "left_staged"
        right_table = "right_staged"
        unnormalized_keys = ["Internal ID"]
        
        # Mock the row count check
        self.mock_con.execute.return_value.fetchone.return_value = [30000]
        
        # Act: Call the left-only method
        with patch.object(self.comparator, '_get_row_count', return_value=30000):
            with patch.object(self.comparator, '_get_right_column', side_effect=lambda x: x):
                self.comparator._find_only_in_left_chunked(
                    left_table, right_table, unnormalized_keys
                )
        
        # Assert: Verify normalized keys in SQL
        executed_calls = self.mock_con.execute.call_args_list
        
        for call_obj in executed_calls:
            sql = call_obj[0][0] if call_obj[0] else None
            if sql and "LEFT JOIN" in sql:
                sql_lower = sql.lower()
                
                # Should use normalized column
                assert 'l.internal_id' in sql_lower, \
                    f"LEFT JOIN should use normalized 'internal_id'. SQL: {sql}"
                assert 'r.internal_id' in sql_lower, \
                    f"LEFT JOIN should use normalized 'internal_id' for right table. SQL: {sql}"
                
                # Should NOT use original
                assert 'l."internal id"' not in sql_lower, \
                    f"LEFT JOIN should NOT use 'Internal ID'. SQL: {sql}"
    
    def test_find_value_differences_uses_normalized_keys(self):
        """
        Test that _find_value_differences_chunked uses normalized key columns.
        """
        # Arrange
        left_table = "left_staged"
        right_table = "right_staged"
        unnormalized_keys = ["Internal ID"]
        value_columns = ["amount", "status"]
        
        # Mock the row count check
        self.mock_con.execute.return_value.fetchone.return_value = [30000]
        
        # Act: Call the value differences method
        with patch.object(self.comparator, '_get_row_count', return_value=30000):
            with patch.object(self.comparator, '_get_right_column', side_effect=lambda x: x):
                self.comparator._find_value_differences_chunked(
                    left_table, right_table, unnormalized_keys, 
                    value_columns, self.mock_config
                )
        
        # Assert: Verify normalized keys in SQL
        executed_calls = self.mock_con.execute.call_args_list
        
        for call_obj in executed_calls:
            sql = call_obj[0][0] if call_obj[0] else None
            if sql and "INNER JOIN" in sql:
                sql_lower = sql.lower()
                
                # Should use normalized key in JOIN
                assert 'l.internal_id = r.internal_id' in sql_lower, \
                    f"Value diff JOIN should use normalized keys. SQL: {sql}"
                
                # Should NOT use original
                assert '"internal id"' not in sql_lower, \
                    f"Value diff should NOT use 'Internal ID'. SQL: {sql}"
    
    def test_non_chunked_methods_also_normalize_keys(self):
        """
        Test that non-chunked comparison methods also normalize keys.
        
        Ensures consistency across all code paths.
        """
        # Arrange
        left_table = "left_staged"
        right_table = "right_staged"
        unnormalized_keys = ["Internal ID"]
        
        # Mock to avoid chunked processing (small dataset)
        self.mock_con.execute.return_value.fetchone.return_value = [1000]  # < 25K threshold
        
        # Act: Call regular _find_matches (non-chunked)
        with patch.object(self.comparator, '_get_row_count', return_value=1000):
            with patch.object(self.comparator, '_get_right_column', side_effect=lambda x: x):
                with patch.object(self.comparator, '_should_use_chunked_processing', return_value=False):
                    self.comparator._find_matches(
                        left_table, right_table, unnormalized_keys
                    )
        
        # Assert: Even non-chunked should normalize
        executed_calls = self.mock_con.execute.call_args_list
        
        for call_obj in executed_calls:
            sql = call_obj[0][0] if call_obj[0] else None
            if sql and "INNER JOIN" in sql:
                sql_lower = sql.lower()
                
                # Should use normalized column
                assert 'l.internal_id' in sql_lower, \
                    f"Non-chunked JOIN should also normalize keys. SQL: {sql}"
                
                # Should NOT use original
                assert '"internal id"' not in sql_lower, \
                    f"Non-chunked should NOT use 'Internal ID'. SQL: {sql}"
    
    def test_export_differences_uses_normalized_keys(self):
        """
        Test that export_differences method uses normalized key columns.
        """
        # Arrange
        left_table = "left_staged"
        right_table = "right_staged"
        output_dir = "data/reports/test"
        
        # Set up config with unnormalized keys
        self.mock_config.comparison_keys = ["Internal ID"]
        
        # Mock execute to avoid actual file operations
        self.mock_con.execute.return_value = None
        
        # Act: Call export_differences
        with patch.object(self.comparator, '_get_right_column', side_effect=lambda x: x):
            self.comparator.export_differences(
                left_table, right_table, self.mock_config, output_dir
            )
        
        # Assert: Verify SQL uses normalized keys
        executed_calls = self.mock_con.execute.call_args_list
        
        for call_obj in executed_calls:
            sql = call_obj[0][0] if call_obj[0] else None
            if sql and "LEFT JOIN" in sql:
                sql_lower = sql.lower()
                
                # Export SQL should also use normalized keys
                assert 'l.internal_id = r.internal_id' in sql_lower or \
                       'internal_id' in sql_lower, \
                    f"Export should use normalized keys. SQL: {sql}"
                
                assert '"internal id"' not in sql_lower, \
                    f"Export should NOT use 'Internal ID'. SQL: {sql}"


if __name__ == "__main__":
    # Run the test to see it fail (TDD approach)
    pytest.main([__file__, "-xvs"])