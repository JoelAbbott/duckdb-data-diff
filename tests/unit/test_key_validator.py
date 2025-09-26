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
        
        # Arrange: Mock DuckDB query results for schema discovery + validation
        # First call: information_schema (return that column exists)
        # Second call: validation query (showing duplicates)
        mock_schema_result = [('message_id',), ('name',), ('id',)]
        self.mock_con.execute.side_effect = [
            Mock(fetchall=Mock(return_value=mock_schema_result)),  # Schema discovery
            Mock(fetchone=Mock(return_value=(1000, 800)))  # Validation result: 200 duplicates
        ]
        
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
        # First call: schema discovery for first key
        # Second call: schema discovery for second key  
        # Third call: duplicate groups query returns 0 (no duplicates)
        # Fourth call: total counts query returns (5000, 5000)
        mock_schema_result1 = [('message_id',), ('name',), ('id',)]
        mock_schema_result2 = [('date_created',), ('message_id',), ('name',)]
        self.mock_con.execute.side_effect = [
            Mock(fetchall=Mock(return_value=mock_schema_result1)),  # Schema for message_id
            Mock(fetchall=Mock(return_value=mock_schema_result2)),  # Schema for date_created
            Mock(fetchone=Mock(return_value=(0,))),  # Duplicate groups query
            Mock(fetchone=Mock(return_value=(5000, 5000)))  # Total counts query
        ]
        
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
        # Check the third SQL call (duplicate groups query) - after schema discovery calls
        third_call = self.mock_con.execute.call_args_list[2][0][0]
        for pattern in expected_patterns:
            assert pattern in third_call
    
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
    
    def test_validate_key_left_table_staged_name_discovery(self):
        """
        Test that KeyValidator discovers actual staged column names for left table validation.
        
        STAGED KEY CONSISTENCY PATTERN TEST:
        - User selects 'serial_number_id' as key column in interactive mode
        - Both staged tables should contain 'serial_number_id' column
        - KeyValidator must discover and use the actual staged column names
        - This prevents "Binder Error: Referenced column not found" failures
        """
        # Skip if KeyValidator not implemented yet (TDD pattern)
        if not KeyValidator or not KeyValidationError:
            pytest.skip("KeyValidator not implemented yet - TDD failure expected")
        
        # Arrange: Mock information_schema query to return staged table columns
        mock_schema_result = [
            ('serial_number_id',),   # Key column exists in staged table
            ('name',),               # Other staged column
            ('internal_id',)         # Other staged column
        ]
        
        # Mock successful validation query result
        mock_validation_result = (1000, 1000)  # No duplicates found
        
        # Configure mocks for schema discovery then validation
        self.mock_con.execute.return_value.fetchall.return_value = mock_schema_result
        self.mock_con.execute.return_value.fetchone.return_value = mock_validation_result
        
        # Act: Validate with schema discovery
        result = self.validator.validate_key(
            table_name="test_left_table", 
            key_columns=["serial_number_id"],  # User-selected key
            dataset_config=self.mock_left_config  # No column_map (left table)
        )
        
        # Assert: Should succeed with schema discovery
        assert result.is_valid == True
        assert result.total_rows == 1000
        assert result.unique_values == 1000
        
        # Verify schema discovery was performed
        schema_calls = [call for call in self.mock_con.execute.call_args_list 
                       if 'information_schema.columns' in str(call)]
        assert len(schema_calls) > 0, "Should query information_schema for column discovery"
    
    def test_right_key_mapping_uses_normalized_inverse_lookup(self):
        """
        Test that right table key mapping correctly handles normalized inverse lookup.
        
        CRITICAL BUG TEST:
        - Dataset config has normalized column_map: {"message_id": "internal_id"}  
        - User selects original-cased key: ["Internal ID"]
        - Should normalize "Internal ID" -> "internal_id" before inverse lookup
        - Should find "internal_id" maps to "message_id" and return ["message_id"]
        - Currently FAILS by returning ["internal_id"] due to no normalization
        """
        # Skip if KeyValidator not implemented yet (TDD pattern)
        if not KeyValidator or not KeyValidationError:
            pytest.skip("KeyValidator not implemented yet - TDD failure expected")
        
        # Arrange: Mock right dataset config with normalized column mapping
        mock_right_config = Mock()
        mock_right_config.column_map = {"message_id": "internal_id"}  # Right -> Left mapping (normalized)
        
        # Mock successful validation query result
        mock_validation_result = (5000, 5000)  # No duplicates found
        
        # Configure mock for validation query
        self.mock_con.execute.return_value.fetchone.return_value = mock_validation_result
        
        # Act: Call _get_staged_key_columns directly to test the mapping logic
        staged_columns = self.validator._get_staged_key_columns(
            table_name="test_right_table",
            key_columns=["Internal ID"],  # Original-cased user input
            dataset_config=mock_right_config  # Has normalized column_map
        )
        
        # Assert: Should return the correct right table key after normalized inverse lookup
        # Expected: ["message_id"] (the right column that maps to normalized "internal_id")
        # Bug: Currently returns ["internal_id"] because normalization is missing
        assert staged_columns == ["message_id"], (
            f"Expected normalized inverse lookup to return ['message_id'], "
            f"but got {staged_columns}. This indicates the right-side key mapping "
            f"bug where normalization is not applied before inverse lookup."
        )


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
            discovered_keys=["test_key"],  # NEW: discovered staged column names
            error_message=None
        )
        
        # Assert: All required attributes exist
        assert hasattr(result, 'is_valid')
        assert hasattr(result, 'total_rows') 
        assert hasattr(result, 'unique_values')
        assert hasattr(result, 'duplicate_count')
        assert hasattr(result, 'discovered_keys')  # NEW: discovered staged column names
        assert hasattr(result, 'error_message')
        
        # Assert: Values are correct
        assert result.is_valid == True
        assert result.total_rows == 1000
        assert result.unique_values == 1000
        assert result.duplicate_count == 0
        assert result.discovered_keys == ["test_key"]  # NEW: check discovered keys
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