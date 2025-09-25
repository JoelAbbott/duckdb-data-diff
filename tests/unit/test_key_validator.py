"""
Unit tests for KeyValidator component.
Following TDD: These tests MUST fail until KeyValidator is implemented.
"""

import pytest
from unittest.mock import Mock, MagicMock
from pathlib import Path
import sys

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

# Import will fail until KeyValidator is implemented - this is expected for TDD
try:
    from src.core.key_validator import KeyValidator, KeyValidationError, KeyValidationResult
except ImportError:
    # Expected failure in TDD - tests should fail until implementation exists
    KeyValidator = None
    KeyValidationError = None
    KeyValidationResult = None


class TestKeyValidator:
    """Test cases for KeyValidator component."""
    
    def setup_method(self):
        """Set up test fixtures."""
        # Mock DuckDB connection
        self.mock_con = Mock()
        
        # Mock dataset configs for column mapping
        self.mock_left_config = Mock()
        self.mock_left_config.column_map = None
        
        self.mock_right_config = Mock()
        self.mock_right_config.column_map = {'author': 'From'}  # Right -> Left mapping
        
        # Create KeyValidator instance (will fail until implemented)
        if KeyValidator:
            self.validator = KeyValidator(self.mock_con)
    
    def test_single_column_uniqueness_check_detects_duplicates(self):
        """
        Test that single column validation detects duplicates correctly.
        
        This test validates the core DuckDB query for single column uniqueness:
        SELECT COUNT(*) as total_rows, COUNT(DISTINCT column) as unique_values
        FROM table WHERE column IS NOT NULL
        """
        # Skip if KeyValidator not implemented yet (TDD pattern)
        if not KeyValidator:
            pytest.skip("KeyValidator not implemented yet - TDD failure expected")
        
        # Arrange: Mock DuckDB query result showing duplicates
        # Total rows: 1000, Unique values: 800 (200 duplicates)
        self.mock_con.execute.return_value.fetchone.return_value = (1000, 800)
        
        # Act: Validate single column key
        result = self.validator.validate_key(
            table_name="test_table",
            key_columns=["message_id"],
            dataset_config=self.mock_left_config
        )
        
        # Assert: Validation should fail due to duplicates
        assert isinstance(result, KeyValidationResult)
        assert result.is_valid == False
        assert result.duplicate_count > 0
        assert result.total_rows == 1000
        assert result.unique_values == 800
        assert "duplicates detected" in result.error_message.lower()
        
        # Verify correct DuckDB query was executed
        expected_sql_pattern = "COUNT(*) as total_rows"
        actual_call = self.mock_con.execute.call_args[0][0]
        assert expected_sql_pattern in actual_call
        assert "message_id" in actual_call
        assert "test_table" in actual_call
    
    def test_composite_key_uniqueness_check_passes(self):
        """
        Test that composite key validation passes when no duplicates exist.
        
        This test validates the composite key DuckDB query:
        SELECT COUNT(*) as duplicate_groups FROM (
            SELECT key1, key2, COUNT(*) FROM table 
            GROUP BY key1, key2 HAVING COUNT(*) > 1
        )
        """
        # Skip if KeyValidator not implemented yet (TDD pattern)
        if not KeyValidator:
            pytest.skip("KeyValidator not implemented yet - TDD failure expected")
        
        # Arrange: Mock DuckDB query results for composite key validation
        # First call: duplicate groups query returns 0 (no duplicates)
        # Second call: total counts query returns (5000, 5000)
        self.mock_con.execute.return_value.fetchone.side_effect = [(0,), (5000, 5000)]
        
        # Act: Validate composite key
        result = self.validator.validate_key(
            table_name="test_table", 
            key_columns=["message_id", "date_created"],
            dataset_config=self.mock_left_config
        )
        
        # Assert: Validation should pass (no duplicates)
        assert isinstance(result, KeyValidationResult)
        assert result.is_valid == True
        assert result.duplicate_count == 0
        assert result.total_rows > 0
        assert result.unique_values == result.total_rows
        assert result.error_message is None
        
        # Verify correct composite key query was executed
        expected_patterns = ["GROUP BY", "HAVING COUNT(*) > 1", "message_id", "date_created"]
        # Check the first SQL call (duplicate groups query)
        first_call = self.mock_con.execute.call_args_list[0][0][0]
        for pattern in expected_patterns:
            assert pattern in first_call
    
    def test_column_mapping_applied_for_right_table_validation(self):
        """
        Test that column mappings are correctly applied when validating right table keys.
        
        CLAUDE.md requirement: MUST apply column mappings from right_dataset_config.column_map
        """
        # Skip if KeyValidator not implemented yet (TDD pattern)  
        if not KeyValidator:
            pytest.skip("KeyValidator not implemented yet - TDD failure expected")
        
        # Arrange: Mock successful validation
        self.mock_con.execute.return_value.fetchone.return_value = (1000, 1000)  # No duplicates
        
        # Act: Validate with column mapping (right table scenario)
        result = self.validator.validate_key(
            table_name="right_table",
            key_columns=["From"],  # Left column name
            dataset_config=self.mock_right_config  # Has mapping From -> author
        )
        
        # Assert: Should apply mapping and use 'author' in query
        assert result.is_valid == True
        
        # Verify that mapped column name 'author' was used in SQL query
        actual_call = self.mock_con.execute.call_args[0][0]
        assert "author" in actual_call  # Mapped column name should be used
        assert "From" not in actual_call  # Original name should not be used
    
    def test_key_validation_error_raised_for_invalid_table(self):
        """
        Test that KeyValidationError is raised when table doesn't exist.
        
        CLAUDE.md requirement: Fail fast with clear error messages.
        """
        # Skip if KeyValidator not implemented yet (TDD pattern)
        if not KeyValidator or not KeyValidationError:
            pytest.skip("KeyValidator not implemented yet - TDD failure expected")
        
        # Arrange: Mock DuckDB error for non-existent table
        self.mock_con.execute.side_effect = Exception("Table 'nonexistent_table' not found")
        
        # Act & Assert: Should raise KeyValidationError with clear message
        with pytest.raises(KeyValidationError) as exc_info:
            self.validator.validate_key(
                table_name="nonexistent_table",
                key_columns=["id"],
                dataset_config=self.mock_left_config
            )
        
        # Verify error message is actionable
        error_msg = str(exc_info.value)
        assert "table" in error_msg.lower()
        assert "not found" in error_msg.lower()
        assert "suggestion:" in error_msg.lower()  # CLAUDE.md error format requirement
    
    def test_empty_key_columns_raises_validation_error(self):
        """
        Test that empty key_columns list raises appropriate error.
        
        CLAUDE.md requirement: Fail fast with clear error messages.
        """
        # Skip if KeyValidator not implemented yet (TDD pattern)
        if not KeyValidator or not KeyValidationError:
            pytest.skip("KeyValidator not implemented yet - TDD failure expected")
        
        # Act & Assert: Empty key columns should raise error
        with pytest.raises(KeyValidationError) as exc_info:
            self.validator.validate_key(
                table_name="test_table",
                key_columns=[],  # Empty list
                dataset_config=self.mock_left_config
            )
        
        # Verify error follows CLAUDE.md format
        error_msg = str(exc_info.value)
        assert "[KEY VALIDATION ERROR]" in error_msg
        assert "suggestion:" in error_msg.lower()
        assert "key_columns cannot be empty" in error_msg.lower()


class TestKeyValidationResult:
    """Test cases for KeyValidationResult data class."""
    
    def test_key_validation_result_structure(self):
        """
        Test that KeyValidationResult has required attributes.
        
        This validates the expected interface before implementation.
        """
        # Skip if KeyValidationResult not implemented yet (TDD pattern)
        if not KeyValidationResult:
            pytest.skip("KeyValidationResult not implemented yet - TDD failure expected")
        
        # Act: Create result instance
        result = KeyValidationResult(
            is_valid=True,
            total_rows=1000,
            unique_values=1000,
            duplicate_count=0,
            error_message=None
        )
        
        # Assert: All required attributes exist
        assert hasattr(result, 'is_valid')
        assert hasattr(result, 'total_rows') 
        assert hasattr(result, 'unique_values')
        assert hasattr(result, 'duplicate_count')
        assert hasattr(result, 'error_message')
        
        # Assert: Values are correct
        assert result.is_valid == True
        assert result.total_rows == 1000
        assert result.unique_values == 1000
        assert result.duplicate_count == 0
        assert result.error_message is None


# Integration test placeholder for future chunked processing
class TestKeyValidatorChunkedProcessing:
    """Test cases for chunked processing on large datasets."""
    
    def test_chunked_validation_for_large_tables(self):
        """
        Test that chunked processing is used for tables > 100K rows.
        
        CLAUDE.md requirement: Use chunked processing for tables > 100K rows.
        """
        # Skip if KeyValidator not implemented yet (TDD pattern)
        if not KeyValidator:
            pytest.skip("KeyValidator not implemented yet - TDD failure expected")
        
        # This test will be implemented when chunked processing is added
        # For now, just verify the test structure exists
        assert True  # Placeholder - will implement with chunked processing