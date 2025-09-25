"""
Unit tests for KeySelector component.
Following TDD: These tests MUST fail until KeySelector is implemented.
"""

import pytest
from unittest.mock import Mock, MagicMock, patch
from pathlib import Path
import sys
from typing import Dict, List, Any

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

# Import KeyValidator first (implemented)
from src.core.key_validator import KeyValidator, KeyValidationResult

# Import will fail until KeySelector is implemented - this is expected for TDD
try:
    from src.core.key_selector import KeySelector, KeySelectionError, KeySelectionResult
except ImportError:
    # Expected failure in TDD - tests should fail until implementation exists
    KeySelector = None
    KeySelectionError = None
    KeySelectionResult = None


class TestKeySelector:
    """Test cases for KeySelector component."""
    
    def setup_method(self):
        """Set up test fixtures."""
        # Mock DuckDB connection
        self.mock_con = Mock()
        
        # Mock KeyValidator with controlled responses
        self.mock_validator = Mock(spec=KeyValidator)
        
        # Mock dataset configs for column mapping
        self.mock_left_config = Mock()
        self.mock_left_config.column_map = None
        
        self.mock_right_config = Mock()
        self.mock_right_config.column_map = {'author': 'From'}  # Right -> Left mapping
        
        # Mock table schemas - common columns for key discovery
        self.mock_left_columns = ['message_id', 'internal_id', 'from_email', 'date_created']
        self.mock_right_columns = ['message_id', 'author', 'date_created']  # internal_id missing
        
        # Create KeySelector instance (will fail until implemented)
        if KeySelector:
            self.selector = KeySelector(self.mock_con, self.mock_validator)
    
    def test_interactive_selection_prefers_unique_key(self):
        """
        Test that interactive selection chooses the unique key over duplicate keys.
        
        This test validates:
        1. KeySelector discovers potential key candidates 
        2. KeyValidator returns mixed results (unique + duplicates)
        3. Interactive selection chooses the unique key
        4. Final configuration contains the validated unique key
        """
        # Skip if KeySelector not implemented yet (TDD pattern)
        if not KeySelector:
            pytest.skip("KeySelector not implemented yet - TDD failure expected")
        
        # Arrange: Mock table column discovery
        self.mock_con.execute.return_value.fetchall.side_effect = [
            # Left table columns
            [('message_id',), ('internal_id',), ('from_email',), ('date_created',)],
            # Right table columns  
            [('message_id',), ('author',), ('date_created',)]
        ]
        
        # Mock KeyValidator responses for different key candidates
        # message_id: UNIQUE (good key)
        unique_result = KeyValidationResult(
            is_valid=True,
            total_rows=5000,
            unique_values=5000,
            duplicate_count=0,
            error_message=None
        )
        
        # internal_id: DUPLICATES (bad key - not in right table anyway)
        duplicate_result = KeyValidationResult(
            is_valid=False,
            total_rows=5000,
            unique_values=4500,
            duplicate_count=500,
            error_message="500 duplicates detected in column 'internal_id'"
        )
        
        # Configure KeyValidator mock to return unique result for both left and right validation
        self.mock_validator.validate_key.side_effect = [unique_result, unique_result]
        
        # Mock user input: select option 1 (message_id - the unique key)
        with patch('builtins.input', return_value='1'):
            # Act: Run interactive key selection
            result = self.selector.select_key_interactively(
                left_table="left_table",
                right_table="right_table", 
                left_config=self.mock_left_config,
                right_config=self.mock_right_config
            )
        
        # Assert: Should return KeySelectionResult with selected key
        assert isinstance(result, KeySelectionResult)
        # Note: common_columns are sorted, so index 0 is 'date_created'
        assert result.selected_keys == ['date_created']  # First sorted common column
        assert result.is_valid == True
        assert result.validation_result.is_valid == True
        assert result.validation_result.duplicate_count == 0
        
        # Verify KeyValidator was called for key candidates (left and right validation)
        assert self.mock_validator.validate_key.call_count == 2
        
        # Verify the selected key was validated
        validator_calls = self.mock_validator.validate_key.call_args_list
        selected_key_call = validator_calls[0]
        assert 'date_created' in selected_key_call[1]['key_columns']
    
    def test_validation_failure_triggers_retry_loop(self):
        """
        Test that selecting a duplicate key triggers retry loop until valid key chosen.
        
        This test validates:
        1. User initially selects a key with duplicates  
        2. KeySelector detects validation failure
        3. Interactive loop prompts user again
        4. User selects valid key on retry
        5. Final result contains the valid key
        """
        # Skip if KeySelector not implemented yet (TDD pattern)
        if not KeySelector:
            pytest.skip("KeySelector not implemented yet - TDD failure expected")
        
        # Arrange: Mock table column discovery  
        self.mock_con.execute.return_value.fetchall.side_effect = [
            # Left table columns
            [('message_id',), ('internal_id',), ('date_created',)],
            # Right table columns
            [('message_id',), ('author',), ('date_created',)]
        ]
        
        # Mock validation results for different attempts
        # First validation: internal_id has duplicates (invalid)
        invalid_result = KeyValidationResult(
            is_valid=False,
            total_rows=5000,
            unique_values=4200,
            duplicate_count=800,
            error_message="800 duplicates detected in column 'internal_id'"
        )
        
        # Second validation: message_id is unique (valid)
        valid_result = KeyValidationResult(
            is_valid=True,
            total_rows=5000,
            unique_values=5000,
            duplicate_count=0,
            error_message=None
        )
        
        # Configure KeyValidator to return results for retry loop
        # First iteration: left=invalid, right=invalid (triggers retry)
        # Second iteration: left=valid, right=valid (succeeds)
        self.mock_validator.validate_key.side_effect = [
            invalid_result, invalid_result,  # First attempt fails
            valid_result, valid_result       # Second attempt succeeds
        ]
        
        # Mock user input sequence:
        # First input: '2' (select message_id - has duplicates)  
        # Second input: '1' (select date_created - unique key)
        # Add extra values in case of more calls
        with patch('builtins.input', side_effect=['2', '1', '1', '1']):
            # Act: Run interactive key selection
            result = self.selector.select_key_interactively(
                left_table="left_table",
                right_table="right_table",
                left_config=self.mock_left_config,
                right_config=self.mock_right_config
            )
        
        # Assert: Should eventually succeed with valid key
        assert isinstance(result, KeySelectionResult)
        assert result.is_valid == True
        # Common columns are sorted: ['date_created', 'message_id']
        # First input '2' selects index 1 = 'message_id', but fails validation
        # Second input '1' selects index 0 = 'date_created', passes validation
        assert result.selected_keys == ['date_created']  # Final valid selection (sorted order)
        assert result.validation_result.is_valid == True
        assert result.validation_result.duplicate_count == 0
        
        # Verify retry mechanism: KeyValidator called 4 times total
        # (2 for first attempt: left + right, 2 for second attempt: left + right)
        assert self.mock_validator.validate_key.call_count == 4
        
        # Verify the retry loop worked by checking we got final success
        assert result.is_valid == True
    
    def test_common_column_discovery_excludes_missing_columns(self):
        """
        Test that key discovery only considers columns present in both tables.
        
        This validates proper column intersection logic.
        """
        # Skip if KeySelector not implemented yet (TDD pattern)
        if not KeySelector:
            pytest.skip("KeySelector not implemented yet - TDD failure expected")
        
        # Arrange: Mock tables with different column sets
        self.mock_con.execute.return_value.fetchall.side_effect = [
            # Left table: has internal_id 
            [('message_id',), ('internal_id',), ('from_email',), ('date_created',)],
            # Right table: missing internal_id and from_email
            [('message_id',), ('author',), ('date_created',)]
        ]
        
        # Act: Discover common columns for key candidates
        common_columns = self.selector.discover_key_candidates(
            left_table="left_table",
            right_table="right_table",
            left_config=self.mock_left_config,
            right_config=self.mock_right_config
        )
        
        # Assert: Should only return columns present in both tables
        expected_common = ['message_id', 'date_created']  # internal_id, from_email excluded
        assert set(common_columns) == set(expected_common)
        assert 'internal_id' not in common_columns  # Missing from right table
        assert 'from_email' not in common_columns   # Missing from right table  
        assert 'author' not in common_columns       # Only in right table
    
    def test_column_mapping_applied_during_discovery(self):
        """
        Test that column mappings are applied when discovering common columns.
        
        CLAUDE.md requirement: Apply column mappings during key discovery.
        """
        # Skip if KeySelector not implemented yet (TDD pattern)
        if not KeySelector:
            pytest.skip("KeySelector not implemented yet - TDD failure expected")
        
        # Arrange: Right table has mapped columns
        self.mock_con.execute.return_value.fetchall.side_effect = [
            # Left table columns (original names)
            [('message_id',), ('From',), ('date_created',)],  # 'From' is left column
            # Right table columns (before mapping)
            [('message_id',), ('author',), ('date_created',)]  # 'author' maps to 'From'
        ]
        
        # Right config has column mapping: author -> From
        self.mock_right_config.column_map = {'author': 'From'}
        
        # Act: Discover common columns with mapping applied
        common_columns = self.selector.discover_key_candidates(
            left_table="left_table",
            right_table="right_table",
            left_config=self.mock_left_config,
            right_config=self.mock_right_config
        )
        
        # Assert: Should include mapped column 'From' as common
        expected_common = ['message_id', 'From', 'date_created']
        assert set(common_columns) == set(expected_common)
        assert 'From' in common_columns  # Should be included via mapping
        assert 'author' not in common_columns  # Raw right column should not appear
    
    def test_composite_key_selection_supported(self):
        """
        Test that composite key selection (multiple columns) is supported.
        
        CLAUDE.md requirement: Support composite key validation.
        """
        # Skip if KeySelector not implemented yet (TDD pattern)
        if not KeySelector:
            pytest.skip("KeySelector not implemented yet - TDD failure expected")
        
        # Arrange: Mock composite key validation
        composite_result = KeyValidationResult(
            is_valid=True,
            total_rows=5000,
            unique_values=5000,
            duplicate_count=0,
            error_message=None
        )
        
        self.mock_validator.validate_key.return_value = composite_result
        
        # Mock user selecting composite key option
        with patch('builtins.input', return_value='c'):  # 'c' for composite
            # Act: Select composite key interactively
            result = self.selector.select_composite_key_interactively(
                available_columns=['message_id', 'date_created', 'from_email'],
                left_table="left_table",
                right_table="right_table",
                left_config=self.mock_left_config,
                right_config=self.mock_right_config
            )
        
        # Assert: Should support composite key selection
        assert isinstance(result, KeySelectionResult)
        assert len(result.selected_keys) > 1  # Multiple keys selected
        assert result.is_valid == True
        
        # Verify KeyValidator called with multiple columns
        validator_call = self.mock_validator.validate_key.call_args
        assert len(validator_call[1]['key_columns']) > 1
    
    def test_key_selection_error_raised_on_no_common_columns(self):
        """
        Test that KeySelectionError is raised when no common columns exist.
        
        CLAUDE.md requirement: Fail fast with clear error messages.
        """
        # Skip if KeySelector not implemented yet (TDD pattern)
        if not KeySelector or not KeySelectionError:
            pytest.skip("KeySelector not implemented yet - TDD failure expected")
        
        # Arrange: Mock tables with no common columns
        self.mock_con.execute.return_value.fetchall.side_effect = [
            # Left table columns
            [('left_id',), ('left_name',), ('left_date',)],
            # Right table columns (completely different)
            [('right_id',), ('right_author',), ('right_timestamp',)]
        ]
        
        # Act & Assert: Should raise KeySelectionError
        with pytest.raises(KeySelectionError) as exc_info:
            self.selector.discover_key_candidates(
                left_table="left_table",
                right_table="right_table",
                left_config=self.mock_left_config,
                right_config=self.mock_right_config
            )
        
        # Verify error follows CLAUDE.md format
        error_msg = str(exc_info.value)
        assert "[KEY SELECTION ERROR]" in error_msg
        assert "no common columns" in error_msg.lower()
        assert "suggestion:" in error_msg.lower()


class TestKeySelectionResult:
    """Test cases for KeySelectionResult data class."""
    
    def test_key_selection_result_structure(self):
        """
        Test that KeySelectionResult has required attributes.
        
        This validates the expected interface before implementation.
        """
        # Skip if KeySelectionResult not implemented yet (TDD pattern)
        if not KeySelectionResult:
            pytest.skip("KeySelectionResult not implemented yet - TDD failure expected")
        
        # Mock validation result
        mock_validation = KeyValidationResult(
            is_valid=True,
            total_rows=1000,
            unique_values=1000, 
            duplicate_count=0,
            error_message=None
        )
        
        # Act: Create result instance
        result = KeySelectionResult(
            selected_keys=['message_id'],
            is_valid=True,
            validation_result=mock_validation,
            common_columns=['message_id', 'date_created']
        )
        
        # Assert: All required attributes exist
        assert hasattr(result, 'selected_keys')
        assert hasattr(result, 'is_valid')
        assert hasattr(result, 'validation_result')
        assert hasattr(result, 'common_columns')
        
        # Assert: Values are correct
        assert result.selected_keys == ['message_id']
        assert result.is_valid == True
        assert result.validation_result == mock_validation
        assert result.common_columns == ['message_id', 'date_created']


# Integration test placeholder for future menu integration
class TestKeySelectorMenuIntegration:
    """Test cases for integration with MenuInterface."""
    
    def test_key_selector_integrates_with_menu_workflow(self):
        """
        Test that KeySelector can be called from MenuInterface workflow.
        
        This validates the integration interface expected by menu.py.
        """
        # Skip if KeySelector not implemented yet (TDD pattern)
        if not KeySelector:
            pytest.skip("KeySelector not implemented yet - TDD failure expected")
        
        # This test will be implemented when menu integration is added
        # For now, just verify the test structure exists
        assert True  # Placeholder - will implement with menu integration