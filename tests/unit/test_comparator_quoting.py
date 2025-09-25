"""
Tests for DataComparator SQL identifier quoting.
Ensures column names with spaces and special characters are properly quoted in SQL generation.
"""

import pytest
from unittest.mock import Mock, patch
import duckdb
import sys
from pathlib import Path

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.core.comparator import DataComparator
from src.config.manager import ComparisonConfig


class TestComparatorSQLQuoting:
    """Test SQL identifier quoting in DataComparator."""
    
    def setup_method(self):
        """Setup test fixtures."""
        self.mock_con = Mock(spec=duckdb.DuckDBPyConnection)
        self.comparator = DataComparator(self.mock_con)
        
        # Mock configuration
        self.config = Mock(spec=ComparisonConfig)
        self.config.tolerance = 0
        self.config.max_differences = 1000
        
        # Setup dataset configs with column mapping that has spaces
        self.left_dataset_config = Mock()
        self.left_dataset_config.column_map = None
        
        self.right_dataset_config = Mock()
        self.right_dataset_config.column_map = {"message_id": "Internal ID"}  # right 'message_id' maps to left 'Internal ID'
        
        # Store configs on comparator
        self.comparator.left_dataset_config = self.left_dataset_config
        self.comparator.right_dataset_config = self.right_dataset_config

    def test_join_condition_quotes_columns_with_spaces(self):
        """
        Test that JOIN conditions properly quote column names containing spaces.
        
        This reproduces the exact SQL error from the integration test:
        - Key column 'Internal ID' contains a space
        - Generated SQL must quote it as l."Internal ID" = r."message_id"
        - Without quoting, DuckDB fails with 'syntax error at or near "ID"'
        """
        # Arrange
        left_table = "left_test"
        right_table = "right_test"
        key_columns = ["Internal ID"]  # Column with space - the problematic case
        
        # Mock the SQL execution to capture the generated SQL
        mock_result = Mock()
        mock_result.fetchone.return_value = [10]
        self.mock_con.execute.return_value = mock_result
        
        # Act - call _find_matches which generates JOIN SQL
        result = self.comparator._find_matches(left_table, right_table, key_columns)
        
        # Assert - capture and verify the generated SQL
        execute_calls = self.mock_con.execute.call_args_list
        assert len(execute_calls) >= 1, "Expected at least one SQL execute call"
        
        # Find the JOIN SQL (should be the last call)
        generated_sql = execute_calls[-1][0][0]  # Last call contains the JOIN SQL
        
        # CRITICAL ASSERTIONS: SQL must use quoted identifiers
        
        # Assert 1: Left column with space must be quoted
        assert 'l."Internal ID"' in generated_sql, \
            f"Expected 'l.\"Internal ID\"' in generated SQL, but got: {generated_sql}"
        
        # Assert 2: Right column should also be quoted for consistency
        assert 'r."message_id"' in generated_sql, \
            f"Expected 'r.\"message_id\"' in generated SQL, but got: {generated_sql}"
        
        # Assert 3: Must NOT contain unquoted column that would cause SQL error
        assert 'l.Internal ID' not in generated_sql, \
            f"Found unquoted 'l.Internal ID' which would cause SQL syntax error: {generated_sql}"
        
        # Assert 4: Verify the complete JOIN condition pattern
        expected_join_pattern = 'l."Internal ID" = r."message_id"'
        assert expected_join_pattern in generated_sql, \
            f"Expected JOIN condition '{expected_join_pattern}' not found in: {generated_sql}"
        
        # Verify result is returned correctly
        assert result == 10

    def test_chunked_sql_quotes_columns_with_spaces(self):
        """
        Test that chunked processing properly quotes column names with spaces.
        
        This tests the _find_matches_chunked method specifically.
        """
        # Arrange
        # Mock row count to trigger chunked processing
        with patch.object(self.comparator, '_get_row_count') as mock_get_row_count:
            mock_get_row_count.return_value = 30000  # Trigger chunked processing
            
            mock_result = Mock()
            mock_result.fetchone.return_value = [5]
            self.mock_con.execute.return_value = mock_result
            
            left_table = "left_test"
            right_table = "right_test"
            key_columns = ["Internal ID"]
            
            # Act
            result = self.comparator._find_matches_chunked(left_table, right_table, key_columns)
            
            # Assert - verify chunked SQL generation quotes identifiers properly
            execute_calls = self.mock_con.execute.call_args_list
            
            # Should have multiple chunked SQL calls
            assert len(execute_calls) > 1, "Expected multiple chunked SQL executions"
            
            # Check the first chunk's SQL
            first_chunk_sql = execute_calls[0][0][0]
            
            # CRITICAL ASSERTIONS for chunked SQL
            assert 'l."Internal ID"' in first_chunk_sql, \
                f"Expected quoted 'l.\"Internal ID\"' in chunked SQL: {first_chunk_sql}"
            
            assert 'r."message_id"' in first_chunk_sql, \
                f"Expected quoted 'r.\"message_id\"' in chunked SQL: {first_chunk_sql}"
            
            assert 'l.Internal ID' not in first_chunk_sql, \
                f"Found unquoted column causing syntax error in chunked SQL: {first_chunk_sql}"

    def test_left_only_sql_quotes_columns_with_spaces(self):
        """
        Test that find_only_in_left properly quotes column names with spaces.
        """
        # Arrange
        mock_result = Mock()
        mock_result.fetchone.return_value = [3]
        self.mock_con.execute.return_value = mock_result
        
        left_table = "left_test"
        right_table = "right_test" 
        key_columns = ["Internal ID"]
        
        # Act
        result = self.comparator._find_only_in_left(left_table, right_table, key_columns)
        
        # Assert
        execute_calls = self.mock_con.execute.call_args_list
        # Find the JOIN SQL (should be the last call) 
        generated_sql = execute_calls[-1][0][0]
        
        # LEFT JOIN conditions must be quoted
        assert 'l."Internal ID" = r."message_id"' in generated_sql, \
            f"Expected quoted JOIN condition in LEFT JOIN SQL: {generated_sql}"
        
        # WHERE clause column must be quoted
        assert 'r."message_id" IS NULL' in generated_sql, \
            f"Expected quoted column in WHERE clause: {generated_sql}"
        
        assert result == 3

    def test_right_only_sql_quotes_columns_with_spaces(self):
        """
        Test that find_only_in_right properly quotes column names with spaces.
        """
        # Arrange
        mock_result = Mock()
        mock_result.fetchone.return_value = [2]
        self.mock_con.execute.return_value = mock_result
        
        left_table = "left_test"
        right_table = "right_test"
        key_columns = ["Internal ID"]
        
        # Act
        result = self.comparator._find_only_in_right(left_table, right_table, key_columns)
        
        # Assert
        execute_calls = self.mock_con.execute.call_args_list
        # Find the JOIN SQL (should be the last call) 
        generated_sql = execute_calls[-1][0][0]
        
        # JOIN condition must be quoted
        assert 'l."Internal ID" = r."message_id"' in generated_sql, \
            f"Expected quoted JOIN condition: {generated_sql}"
        
        # WHERE clause must use quoted column
        assert 'l."Internal ID" IS NULL' in generated_sql, \
            f"Expected quoted column in WHERE clause: {generated_sql}"
        
        assert result == 2

    def test_composite_key_with_spaces_quotes_all_columns(self):
        """
        Test that composite keys with spaces in column names are properly quoted.
        """
        # Arrange - setup composite key mapping
        self.right_dataset_config.column_map = {
            "message_id": "Internal ID",
            "author": "From Email Address"  # Second column also has spaces
        }
        
        mock_result = Mock()
        mock_result.fetchone.return_value = [15]
        self.mock_con.execute.return_value = mock_result
        
        key_columns = ["Internal ID", "From Email Address"]  # Both have spaces
        
        # Act
        result = self.comparator._find_matches("left_test", "right_test", key_columns)
        
        # Assert
        execute_calls = self.mock_con.execute.call_args_list
        # Find the JOIN SQL (should be the last call) 
        generated_sql = execute_calls[-1][0][0]
        
        # Both key columns must be quoted in JOIN condition
        assert 'l."Internal ID" = r."message_id"' in generated_sql
        assert 'l."From Email Address" = r."author"' in generated_sql
        
        # Must not contain unquoted versions
        assert 'l.Internal ID' not in generated_sql
        assert 'l.From Email Address' not in generated_sql
        
        assert result == 15

    def test_no_spaces_columns_still_quoted_for_consistency(self):
        """
        Test that even columns without spaces are quoted for consistency.
        
        This ensures all identifiers are quoted uniformly, preventing issues
        with reserved words or other special characters.
        """
        # Arrange - normal columns without spaces
        self.right_dataset_config.column_map = {"author": "From"}  # No spaces
        
        mock_result = Mock()
        mock_result.fetchone.return_value = [8]
        self.mock_con.execute.return_value = mock_result
        
        key_columns = ["From"]  # No spaces
        
        # Act
        result = self.comparator._find_matches("left_test", "right_test", key_columns)
        
        # Assert
        execute_calls = self.mock_con.execute.call_args_list
        # Find the JOIN SQL (should be the last call) 
        generated_sql = execute_calls[-1][0][0]
        
        # Even normal columns should be quoted for consistency
        assert 'l."From" = r."author"' in generated_sql, \
            f"Expected quoted identifiers even for columns without spaces: {generated_sql}"
        
        assert result == 8