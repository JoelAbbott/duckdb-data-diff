"""
Unit tests for DataComparator chunked SQL generation consistency.
Following TDD: These tests MUST fail until all chunked methods use column mappings correctly.

CLAUDE.md Active Regression Risk:
"Chunked SQL Consistency: Verify all _*_chunked methods in comparator.py use 
_get_right_column() and mapped column names correctly."
"""

import pytest
from unittest.mock import Mock, MagicMock, patch
from pathlib import Path
import sys
import re

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

# Import existing components
from src.core.comparator import DataComparator, ComparisonResult
from src.config.manager import ComparisonConfig


class TestDataComparatorChunkedSQLConsistency:
    """Test cases for chunked SQL generation column mapping consistency."""
    
    def setup_method(self):
        """Set up test fixtures."""
        # Mock DuckDB connection
        self.mock_con = Mock()
        
        # Create DataComparator instance
        self.comparator = DataComparator(self.mock_con)
        
        # Mock configuration
        self.mock_config = Mock(spec=ComparisonConfig)
        self.mock_config.comparison_keys = []
        self.mock_config.value_columns = ['Subject', 'Date Created']
        self.mock_config.tolerance = 0.01
        
        # Critical test scenario from integration test:
        # Left table has 'From' column, right table has 'author' column
        # Column mapping: 'author' -> 'From' (right -> left mapping)
        
        # Mock left dataset config (no mapping)
        self.mock_left_config = Mock()
        self.mock_left_config.column_map = {}
        
        # Mock right dataset config with column mapping
        self.mock_right_config = Mock() 
        self.mock_right_config.column_map = {
            'author': 'From',  # Right 'author' maps to left 'From'
            'email_subject': 'Subject',
            'message_date': 'Date Created'
        }
        
        # Set dataset configs on comparator
        self.comparator.left_dataset_config = self.mock_left_config
        self.comparator.right_dataset_config = self.mock_right_config
        
        # Mock database responses
        self.mock_con.execute.return_value.fetchone.return_value = [100000]  # Large row count
        self.mock_con.execute.return_value.fetchall.return_value = [
            ('From',), ('Subject',), ('Date Created',)
        ]  # Column list
    
    def test_chunked_sql_uses_mapped_column_for_join_condition(self):
        """
        Test that chunked SQL methods use mapped columns in JOIN conditions.
        
        CRITICAL: This validates the EXACT issue found in integration test where
        SQL was generating 'r.From' instead of 'r.author' (mapped column).
        """
        # Arrange: Set up scenario that triggers chunked processing
        key_columns = ['From']  # Left table column name
        expected_right_column = 'author'  # Mapped right table column name
        
        # Mock chunked processing trigger
        with patch.object(self.comparator, '_should_use_chunked_processing', return_value=True), \
             patch.object(self.comparator, '_get_row_count', return_value=100000):  # Large dataset
            
            # Mock the DuckDB execute to capture SQL
            captured_sqls = []
            
            def capture_sql(sql):
                captured_sqls.append(sql)
                # Mock return for chunked processing
                mock_result = Mock()
                mock_result.fetchone.return_value = [0]  # No matches for test
                return mock_result
            
            self.mock_con.execute.side_effect = capture_sql
            
            # Act: Call chunked method that should use column mapping
            try:
                self.comparator._find_matches_chunked(
                    left_table="left_table",
                    right_table="right_table", 
                    key_columns=key_columns
                )
            except Exception:
                pass  # We expect this to fail during test setup, focus on SQL generation
            
        # Assert: Check that generated SQL uses mapped column name
        sql_statements = " ".join(captured_sqls)
        
        # CRITICAL ASSERTION: Must use mapped column 'author', not 'From'
        assert f"r.{expected_right_column}" in sql_statements, \
               f"SQL must use mapped right column 'r.{expected_right_column}' but got: {sql_statements}"
        
        # CRITICAL ASSERTION: Must NOT use unmapped column name
        assert f"r.From" not in sql_statements, \
               f"SQL must NOT use unmapped column 'r.From' but found it in: {sql_statements}"
        
        # Additional validation: Check JOIN pattern
        join_pattern = re.search(r'l\.From\s*=\s*r\.(\w+)', sql_statements)
        assert join_pattern, f"Could not find JOIN pattern in SQL: {sql_statements}"
        assert join_pattern.group(1) == expected_right_column, \
               f"JOIN should use 'r.{expected_right_column}' but found 'r.{join_pattern.group(1)}'"
    
    def test_chunked_sql_value_differences_uses_mapped_columns(self):
        """
        Test that value differences chunked method uses mapped columns correctly.
        
        This validates that comparison conditions use mapped column names.
        """
        # Arrange
        key_columns = ['From']
        value_columns = ['Subject', 'Date Created'] 
        
        # Expected mappings from mock config
        expected_mappings = {
            'From': 'author',
            'Subject': 'email_subject', 
            'Date Created': 'message_date'
        }
        
        with patch.object(self.comparator, '_should_use_chunked_processing', return_value=True), \
             patch.object(self.comparator, '_get_row_count', return_value=100000):
            
            captured_sqls = []
            
            def capture_sql(sql):
                captured_sqls.append(sql)
                mock_result = Mock()
                mock_result.fetchone.return_value = [0]
                return mock_result
            
            self.mock_con.execute.side_effect = capture_sql
            
            # Act
            try:
                self.comparator._find_value_differences_chunked(
                    left_table="left_table",
                    right_table="right_table",
                    key_columns=key_columns,
                    value_columns=value_columns,
                    config=self.mock_config
                )
            except Exception:
                pass  # Focus on SQL generation
        
        sql_statements = " ".join(captured_sqls)
        
        # Assert: All mapped columns should appear in SQL
        for left_col, expected_right_col in expected_mappings.items():
            assert f"r.{expected_right_col}" in sql_statements, \
                   f"SQL must contain mapped column 'r.{expected_right_col}' for '{left_col}'"
            
            # Should NOT contain unmapped column names on right side
            if left_col != expected_right_col:  # Skip if same name
                assert f"r.{left_col}" not in sql_statements, \
                       f"SQL must NOT contain unmapped column 'r.{left_col}'"
    
    def test_chunked_sql_left_only_uses_mapped_columns(self):
        """
        Test that left-only chunked method uses mapped columns for JOIN conditions.
        """
        key_columns = ['From']
        expected_right_column = 'author'
        
        with patch.object(self.comparator, '_should_use_chunked_processing', return_value=True), \
             patch.object(self.comparator, '_get_row_count', return_value=100000):
            
            captured_sqls = []
            
            def capture_sql(sql):
                captured_sqls.append(sql)
                mock_result = Mock()
                mock_result.fetchone.return_value = [0]
                return mock_result
            
            self.mock_con.execute.side_effect = capture_sql
            
            try:
                self.comparator._find_only_in_left_chunked(
                    left_table="left_table",
                    right_table="right_table",
                    key_columns=key_columns
                )
            except Exception:
                pass
        
        sql_statements = " ".join(captured_sqls)
        
        # Check JOIN condition uses mapped column
        assert f"r.{expected_right_column}" in sql_statements
        assert f"r.From" not in sql_statements
        
        # Check WHERE condition uses mapped column
        where_pattern = re.search(r'WHERE\s+r\.(\w+)\s+IS\s+NULL', sql_statements)
        if where_pattern:
            assert where_pattern.group(1) == expected_right_column, \
                   f"WHERE clause should check 'r.{expected_right_column}' but found 'r.{where_pattern.group(1)}'"
    
    def test_chunked_sql_right_only_uses_mapped_columns(self):
        """
        Test that right-only chunked method uses mapped columns for JOIN conditions.
        """
        key_columns = ['From']
        expected_right_column = 'author'
        
        with patch.object(self.comparator, '_should_use_chunked_processing', return_value=True), \
             patch.object(self.comparator, '_get_row_count', return_value=100000):
            
            captured_sqls = []
            
            def capture_sql(sql):
                captured_sqls.append(sql)
                mock_result = Mock()
                mock_result.fetchone.return_value = [0]
                return mock_result
            
            self.mock_con.execute.side_effect = capture_sql
            
            try:
                self.comparator._find_only_in_right_chunked(
                    left_table="left_table",
                    right_table="right_table",
                    key_columns=key_columns
                )
            except Exception:
                pass
        
        sql_statements = " ".join(captured_sqls)
        
        # Check JOIN condition uses mapped column  
        assert f"r.{expected_right_column}" in sql_statements
        assert f"r.From" not in sql_statements
        
        # Check WHERE condition uses left column (not mapped)
        where_pattern = re.search(r'WHERE\s+l\.(\w+)\s+IS\s+NULL', sql_statements)
        if where_pattern:
            assert where_pattern.group(1) == 'From', \
                   f"WHERE clause should check 'l.From' but found 'l.{where_pattern.group(1)}'"
    
    def test_get_right_column_helper_works_correctly(self):
        """
        Test that the _get_right_column helper method works correctly.
        
        This is the core method that all chunked SQL should use.
        """
        # Test with mapped column
        result = self.comparator._get_right_column('From')
        assert result == 'author', f"Expected 'author' but got '{result}'"
        
        # Test with unmapped column (should return same name)
        result = self.comparator._get_right_column('unmapped_column')
        assert result == 'unmapped_column', f"Expected 'unmapped_column' but got '{result}'"
        
        # Test with mapped value column
        result = self.comparator._get_right_column('Subject')
        assert result == 'email_subject', f"Expected 'email_subject' but got '{result}'"
    
    def test_integration_scenario_sql_generation(self):
        """
        Test SQL generation for exact integration test scenario.
        
        This reproduces the failing scenario: From -> author mapping
        """
        # Exact scenario from integration test
        key_columns = ['From']
        
        # Mock large dataset to trigger chunked processing
        with patch.object(self.comparator, '_get_row_count', return_value=295351):  # Actual size
            
            # Check if chunked processing would be triggered
            should_chunk = self.comparator._should_use_chunked_processing("left_table", "right_table")
            
            if should_chunk:
                captured_sqls = []
                
                def capture_sql(sql):
                    captured_sqls.append(sql)
                    mock_result = Mock()
                    mock_result.fetchone.return_value = [0]
                    return mock_result
                
                self.mock_con.execute.side_effect = capture_sql
                
                # This should generate correct SQL with mapped columns
                try:
                    self.comparator._find_matches_chunked("left_table", "right_table", key_columns)
                except Exception:
                    pass
                
                # Validate no problematic SQL patterns
                all_sql = " ".join(captured_sqls)
                
                # Critical assertion from integration test failure
                assert "r.From" not in all_sql, \
                       f"Integration test showed 'r.From' causes 'column not found' error: {all_sql}"
                assert "r.author" in all_sql, \
                       f"Must use mapped column 'r.author' instead: {all_sql}"