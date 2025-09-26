"""
Unit tests for DataComparator component.
Following TDD: These tests MUST fail until DataComparator modifications are implemented.
"""

import pytest
from unittest.mock import Mock, MagicMock, patch
from pathlib import Path
import sys

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

# Import existing components
from src.core.comparator import DataComparator, ComparisonResult
from src.config.manager import ComparisonConfig


class TestDataComparatorValidatedKeys:
    """Test cases for DataComparator accepting pre-validated keys."""
    
    def setup_method(self):
        """Set up test fixtures."""
        # Mock DuckDB connection
        self.mock_con = Mock()
        
        # Create DataComparator instance
        self.comparator = DataComparator(self.mock_con)
        
        # Mock configuration
        self.mock_config = Mock(spec=ComparisonConfig)
        self.mock_config.comparison_keys = []  # Empty to trigger key determination normally
        self.mock_config.value_columns = []
        self.mock_config.tolerance = 0.01
        
        # Mock dataset configs
        self.mock_left_config = Mock()
        self.mock_left_config.column_map = None
        
        self.mock_right_config = Mock()
        self.mock_right_config.column_map = None
        
        # Mock database responses for row counts and columns
        self.mock_con.execute.return_value.fetchone.return_value = [1000]  # Row count
        self.mock_con.execute.return_value.fetchall.return_value = [
            ('user_key',), ('column1',), ('column2',)
        ]  # Column list
    
    def test_compare_skips_key_determination_when_validated_keys_provided(self):
        """
        Test that compare() method skips _determine_keys when validated_keys are provided.
        
        This test validates the CLAUDE.md Step 4 requirement:
        - DataComparator should accept pre-validated keys
        - When validated keys are provided, _determine_keys should NOT be called
        - The provided validated keys should be used directly
        """
        # Arrange: Mock the _determine_keys method to track if it's called
        with patch.object(self.comparator, '_determine_keys', 
                         return_value=['auto_detected_key']) as mock_determine_keys:
            
            # Mock other internal methods to avoid side effects
            with patch.object(self.comparator, '_find_matches', return_value=0), \
                 patch.object(self.comparator, '_find_only_in_left', return_value=0), \
                 patch.object(self.comparator, '_find_only_in_right', return_value=0), \
                 patch.object(self.comparator, '_find_value_differences', return_value=0), \
                 patch.object(self.comparator, '_determine_value_columns', return_value=['column1']):
                
                # Act: Call compare with validated_keys parameter
                validated_keys = ["user_key"]  # Pre-validated key from KeySelector
                
                # This should fail until the validated_keys parameter is implemented
                result = self.comparator.compare(
                    left_table="left_table",
                    right_table="right_table", 
                    config=self.mock_config,
                    left_dataset_config=self.mock_left_config,
                    right_dataset_config=self.mock_right_config,
                    validated_keys=validated_keys  # NEW PARAMETER - will fail until implemented
                )
        
        # Assert: _determine_keys should NOT have been called
        mock_determine_keys.assert_not_called()
        
        # Assert: Result should use the validated keys
        assert isinstance(result, ComparisonResult)
        assert result.key_columns == validated_keys
        assert "user_key" in result.key_columns
        
        # Verify the validated keys were used (not auto-detected)
        assert result.key_columns != ['auto_detected_key']
    
    def test_compare_uses_determine_keys_when_no_validated_keys_provided(self):
        """
        Test that compare() still uses _determine_keys when no validated_keys provided.
        
        This ensures backward compatibility - existing behavior should remain unchanged.
        """
        # Arrange: Mock the _determine_keys method to track calls
        with patch.object(self.comparator, '_determine_keys', 
                         return_value=['auto_detected_key']) as mock_determine_keys:
            
            # Mock other internal methods
            with patch.object(self.comparator, '_find_matches', return_value=0), \
                 patch.object(self.comparator, '_find_only_in_left', return_value=0), \
                 patch.object(self.comparator, '_find_only_in_right', return_value=0), \
                 patch.object(self.comparator, '_find_value_differences', return_value=0), \
                 patch.object(self.comparator, '_determine_value_columns', return_value=['column1']):
                
                # Act: Call compare WITHOUT validated_keys parameter (existing behavior)
                result = self.comparator.compare(
                    left_table="left_table",
                    right_table="right_table",
                    config=self.mock_config,
                    left_dataset_config=self.mock_left_config,
                    right_dataset_config=self.mock_right_config
                    # No validated_keys parameter - should use existing key determination
                )
        
        # Assert: _determine_keys SHOULD have been called (existing behavior)
        mock_determine_keys.assert_called_once_with("left_table", "right_table", self.mock_config)
        
        # Assert: Result should use the auto-detected keys
        assert isinstance(result, ComparisonResult)
        assert result.key_columns == ['auto_detected_key']
    
    def test_validated_keys_parameter_signature_exists(self):
        """
        Test that the compare method accepts validated_keys parameter.
        
        This test validates that the parameter is now implemented and working.
        """
        # Mock internal methods to avoid side effects  
        with patch.object(self.comparator, '_find_matches', return_value=100), \
             patch.object(self.comparator, '_find_only_in_left', return_value=10), \
             patch.object(self.comparator, '_find_only_in_right', return_value=5), \
             patch.object(self.comparator, '_find_value_differences', return_value=2), \
             patch.object(self.comparator, '_determine_value_columns', return_value=['column1']):
            
            # Act: Call with validated_keys parameter - should work now
            result = self.comparator.compare(
                left_table="left_table",
                right_table="right_table",
                config=self.mock_config,
                left_dataset_config=self.mock_left_config,
                right_dataset_config=self.mock_right_config,
                validated_keys=["user_key"]  # This parameter should work now
            )
        
        # Assert: Should work correctly with validated keys
        assert isinstance(result, ComparisonResult)
        assert result.key_columns == ["user_key"]
        assert result.matched_rows == 100
    
    def test_empty_validated_keys_falls_back_to_determine_keys(self):
        """
        Test that empty validated_keys list falls back to _determine_keys.
        
        This ensures robust handling of edge cases.
        """
        # This test will be implemented after the basic functionality is working
        # For now, just verify the test structure exists
        
        # Skip until basic validated_keys implementation is complete
        pytest.skip("Test structure placeholder - will implement after basic validated_keys support")
    
    def test_validated_keys_logging_and_debug_info(self):
        """
        Test that validated keys are properly logged for debugging.
        
        CLAUDE.md requirement: Comprehensive logging for audit trail.
        """
        # Skip until basic validated_keys implementation is complete
        pytest.skip("Test structure placeholder - will implement after basic validated_keys support")


class TestDataComparatorExistingFunctionality:
    """Test cases to ensure existing DataComparator functionality is preserved."""
    
    def setup_method(self):
        """Set up test fixtures for existing functionality tests."""
        self.mock_con = Mock()
        self.comparator = DataComparator(self.mock_con)
        
        # Mock basic responses
        self.mock_con.execute.return_value.fetchone.return_value = [1000]
        self.mock_con.execute.return_value.fetchall.return_value = [
            ('id',), ('name',), ('email',)
        ]
    
    def test_existing_compare_method_still_works(self):
        """
        Test that existing compare method calls work without validated_keys.
        
        This ensures we don't break existing code when adding the new parameter.
        """
        # Arrange
        mock_config = Mock(spec=ComparisonConfig)
        mock_config.comparison_keys = ['id']  # Use configured keys
        mock_config.value_columns = []
        mock_config.tolerance = 0.01
        
        # Mock internal methods
        with patch.object(self.comparator, '_find_matches', return_value=500), \
             patch.object(self.comparator, '_find_only_in_left', return_value=100), \
             patch.object(self.comparator, '_find_only_in_right', return_value=50), \
             patch.object(self.comparator, '_find_value_differences', return_value=25), \
             patch.object(self.comparator, '_determine_value_columns', return_value=['name', 'email']):
            
            # Act: Call existing method without validated_keys
            result = self.comparator.compare(
                left_table="test_left",
                right_table="test_right",
                config=mock_config
            )
        
        # Assert: Should work as before
        assert isinstance(result, ComparisonResult)
        assert result.key_columns == ['id']  # From config.comparison_keys
        assert result.matched_rows == 500
        assert result.only_in_left == 100
        assert result.only_in_right == 50
        assert result.value_differences == 25


class TestDataComparatorStagedKeyPropagation:
    """Test cases for staged key propagation from KeyValidator to DataComparator."""
    
    def setup_method(self):
        """Set up test fixtures."""
        # Mock DuckDB connection
        self.mock_con = Mock()
        
        # Create DataComparator instance
        self.comparator = DataComparator(self.mock_con)
        
        # Mock configuration
        self.mock_config = Mock(spec=ComparisonConfig)
        self.mock_config.comparison_keys = []  # Empty to use provided keys
        self.mock_config.value_columns = []
        self.mock_config.tolerance = 0.01
        
        # Mock dataset configs
        self.mock_left_config = Mock()
        self.mock_left_config.column_map = None  # Left table (no column mapping)
        
        self.mock_right_config = Mock()
        self.mock_right_config.column_map = {'staged_key': 'user_key'}  # Right table mapping
        
    def test_comparator_uses_discovered_staged_keys(self):
        """
        Test that DataComparator uses discovered staged key names for SQL generation.
        
        CRITICAL BUG REPRODUCTION:
        - User selects 'user_key' as key column in interactive mode
        - KeyValidator discovers actual staged column name is 'staged_key'
        - DataComparator must use 'staged_key' for all SQL generation, not 'user_key'
        - This prevents "Binder Error: Referenced column 'user_key' not found"
        
        This test MUST FAIL until staged key propagation is implemented.
        """
        # Arrange: Mock KeyValidator to simulate key discovery behavior
        # KeyValidator should find that 'user_key' maps to 'staged_key' in the staged table
        mock_validation_result = Mock()
        mock_validation_result.is_valid = True
        mock_validation_result.discovered_keys = ['staged_key']  # NEW: discovered staged column names
        
        # Mock row counts
        self.mock_con.execute.return_value.fetchone.return_value = [1000]
        
        # Mock SQL generation methods to capture what keys are actually used
        captured_sql_calls = []
        
        def mock_execute(sql):
            captured_sql_calls.append(sql)
            mock_result = Mock()
            mock_result.fetchone.return_value = [1000]
            mock_result.fetchall.return_value = [('staged_key',), ('column1',), ('column2',)]
            return mock_result
        
        self.mock_con.execute = mock_execute
        
        # Mock KeyValidator to return discovered keys
        with patch('src.core.comparator.KeyValidator') as mock_validator_class:
            mock_validator = Mock()
            mock_validator.validate_key.return_value = mock_validation_result
            mock_validator_class.return_value = mock_validator
            
            # Mock other internal methods to focus on key propagation
            with patch.object(self.comparator, '_determine_value_columns', return_value=['column1']), \
                 patch.object(self.comparator, '_find_matches', return_value=0) as mock_find_matches, \
                 patch.object(self.comparator, '_find_only_in_left', return_value=0), \
                 patch.object(self.comparator, '_find_only_in_right', return_value=0), \
                 patch.object(self.comparator, '_find_value_differences', return_value=0):
                
                # Act: Call compare with user-selected key
                result = self.comparator.compare(
                    left_table="test_left_table",
                    right_table="test_right_table", 
                    config=self.mock_config,
                    left_dataset_config=self.mock_left_config,
                    right_dataset_config=self.mock_right_config,
                    validated_keys=["user_key"]  # User-selected key that needs discovery
                )
        
        # Assert: DataComparator should use discovered staged key names, not user-selected names
        # The _find_matches method should be called with the discovered staged key
        mock_find_matches.assert_called_once_with(
            "test_left_table",
            "test_right_table", 
            ["staged_key"]  # Should use discovered key, not "user_key"
        )
        
        # Assert: Result should contain the discovered staged key names
        assert result.key_columns == ["staged_key"], f"Expected discovered key 'staged_key', got {result.key_columns}"
        
        # This test documents the EXACT failure we need to fix:
        # DataComparator must propagate discovered staged key names from KeyValidator
        # to all subsequent SQL generation methods


class TestDataComparatorFailFast:
    """Test cases for DataComparator fail fast comparison pattern."""
    
    def setup_method(self):
        """Set up test fixtures."""
        # Mock DuckDB connection
        self.mock_con = Mock()
        
        # Create DataComparator instance
        self.comparator = DataComparator(self.mock_con)
        
        # Mock configuration
        self.mock_config = Mock(spec=ComparisonConfig)
        self.mock_config.comparison_keys = []  # Empty to trigger key determination
        self.mock_config.value_columns = []
        self.mock_config.tolerance = 0.01
        
        # Mock dataset configs
        self.mock_left_config = Mock()
        self.mock_left_config.column_map = None
        
        self.mock_right_config = Mock()
        self.mock_right_config.column_map = None
        
        # Mock database responses for row counts and columns
        self.mock_con.execute.return_value.fetchone.return_value = [1000]  # Row count
        self.mock_con.execute.return_value.fetchall.return_value = [
            ('user_key',), ('column1',), ('column2',)
        ]  # Column list
    
    def test_compare_fails_fast_on_key_validation_failure(self):
        """
        Test that DataComparator.compare() immediately raises KeyValidationError
        when key validation fails, implementing the FAIL FAST COMPARISON PATTERN.
        
        This test verifies two scenarios:
        1. Left dataset key validation fails -> immediate KeyValidationError
        2. Right dataset key validation fails -> immediate KeyValidationError
        """
        from src.core.key_validator import KeyValidationError, KeyValidationResult
        
        # Mock _determine_keys to return test keys
        with patch.object(self.comparator, '_determine_keys', return_value=['duplicate_key']):
            
            # Scenario 1: Left validation fails
            mock_validator = Mock()
            mock_left_validation = KeyValidationResult(
                is_valid=False,
                total_rows=100,
                unique_values=95,
                duplicate_count=5,
                discovered_keys=['duplicate_key'],
                error_message="Duplicate keys found"
            )
            mock_right_validation = KeyValidationResult(
                is_valid=True,
                total_rows=100,
                unique_values=100,
                duplicate_count=0,
                discovered_keys=['duplicate_key'],
                error_message=None
            )
            
            mock_validator.validate_key.side_effect = [mock_left_validation, mock_right_validation]
            
            with patch('src.core.comparator.KeyValidator', return_value=mock_validator):
                # This should raise KeyValidationError immediately - no further processing
                with pytest.raises(KeyValidationError) as exc_info:
                    self.comparator.compare(
                        left_table="left_table",
                        right_table="right_table",
                        config=self.mock_config,
                        left_dataset_config=self.mock_left_config,
                        right_dataset_config=self.mock_right_config
                    )
                
                # Verify error message format matches CLAUDE.md mandatory format
                error_msg = str(exc_info.value)
                assert "[KEY VALIDATION ERROR]" in error_msg
                assert "left dataset" in error_msg
                assert "Suggestion:" in error_msg
                
            # Scenario 2: Right validation fails  
            mock_validator.reset_mock()
            mock_left_validation_success = KeyValidationResult(
                is_valid=True,
                total_rows=100,
                unique_values=100,
                duplicate_count=0,
                discovered_keys=['duplicate_key'],
                error_message=None
            )
            mock_right_validation_fail = KeyValidationResult(
                is_valid=False,
                total_rows=100,
                unique_values=97,
                duplicate_count=3,
                discovered_keys=['duplicate_key'],
                error_message="Duplicate keys found"
            )
            
            mock_validator.validate_key.side_effect = [mock_left_validation_success, mock_right_validation_fail]
            
            with patch('src.core.comparator.KeyValidator', return_value=mock_validator):
                # This should also raise KeyValidationError immediately
                with pytest.raises(KeyValidationError) as exc_info:
                    self.comparator.compare(
                        left_table="left_table",
                        right_table="right_table", 
                        config=self.mock_config,
                        left_dataset_config=self.mock_left_config,
                        right_dataset_config=self.mock_right_config
                    )
                
                # Verify error message format for right dataset
                error_msg = str(exc_info.value)
                assert "[KEY VALIDATION ERROR]" in error_msg
                assert "right dataset" in error_msg
                assert "Suggestion:" in error_msg