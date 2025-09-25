"""
TDD Phase 1: Tests for Column Comparison Mismatch Fix

This test file targets the bug where only 2 value columns are compared despite 
user approval of 15+ column mappings. The root cause is that column mappings
are created with original names but lookups use normalized names.

TARGET FUNCTION: menu.py:_create_interactive_config()

EXPECTED BEHAVIOR BEFORE FIX:
- column_map contains original names (e.g., 'Internal ID.1': 'Transaction-Code')
- Tests will FAIL because normalization is missing

EXPECTED BEHAVIOR AFTER FIX:  
- column_map contains normalized names (e.g., 'internal_id_1': 'transaction_code')
- comparison_keys preserve original names for display/error messages
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path
import sys

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.ui.menu import MenuInterface


class TestInteractiveConfigColumnNormalization:
    """
    Test suite for the column normalization fix in _create_interactive_config().
    
    These tests will FAIL before the fix is implemented and PASS after the fix.
    This follows TDD protocol: Write Tests → Commit → Code → Iterate → Commit.
    """
    
    def setup_method(self):
        """Setup test fixtures with mock data directory."""
        self.menu = MenuInterface(data_dir=Path("test_data"))
        
        # Create mock file paths with complex names to test normalization  
        self.left_file = Path("test_data/NetSuite-Export (2024).xlsx")
        self.right_file = Path("test_data/QA2-Messages Data.csv")
        
    def test_column_map_uses_normalized_names_for_staging_consistency(self):
        """
        ASSERTION 1 (The Core Fix): Test that column_map contains normalized names.
        
        This is the primary bug fix. The column mapping must use normalized names
        to match the staged table structure created by stager.py.
        
        BEFORE FIX: column_map = {'Internal ID.1': 'Transaction-Code'}  
        AFTER FIX:  column_map = {'internal_id_1': 'transaction_code'}
        
        This test will FAIL before the fix is implemented.
        """
        # Arrange: Mock matches with unnormalized column names (real-world scenario)
        mock_matches = [
            {
                'left_column': 'Transaction-Code',      # Has dash and mixed case
                'right_column': 'Internal ID.1',       # Has spaces, period, mixed case  
                'confidence': 0.95,
                'match_reason': 'exact_name_match'
            },
            {
                'left_column': 'Email Address', 
                'right_column': 'Recipient-Email',      # Has dash
                'confidence': 0.87,
                'match_reason': 'partial_name_match'
            },
            {
                'left_column': 'Date Created',
                'right_column': 'Message Date/Time',   # Has space and slash
                'confidence': 0.92,
                'match_reason': 'semantic_match'
            }
        ]
        
        # Mock validated keys with original names (preserved for display)
        validated_keys = ['Transaction-Code']  # Original name for error messages
        
        # Act: Create config using current implementation
        with patch('src.ui.menu.Path.exists', return_value=True):
            config = self.menu._create_interactive_config(
                left_file=self.left_file,
                right_file=self.right_file, 
                matches=mock_matches,
                validated_keys=validated_keys
            )
        
        # Assert: Extract the generated column_map from config
        right_dataset_name = list(config["datasets"].keys())[1]  # Second dataset is right
        column_map = config["datasets"][right_dataset_name]["column_map"]
        
        # CRITICAL ASSERTION 1A: Column map keys should be normalized (right columns)
        expected_normalized_keys = {
            'internal_id_1',        # 'Internal ID.1' → normalized
            'recipient_email',      # 'Recipient-Email' → normalized  
            'message_date_time'     # 'Message Date/Time' → normalized
        }
        
        actual_keys = set(column_map.keys())
        assert actual_keys == expected_normalized_keys, (
            f"CORE BUG: column_map keys are not normalized for staging consistency.\n"
            f"Expected normalized keys: {expected_normalized_keys}\n" 
            f"Actual keys: {actual_keys}\n"
            f"Full column_map: {column_map}\n"
            f"This causes _get_right_column() lookup failures in comparator.py"
        )
        
        # CRITICAL ASSERTION 1B: Column map values should be normalized (left columns)  
        expected_mappings = {
            'internal_id_1': 'transaction_code',     # Both sides normalized
            'recipient_email': 'email_address',
            'message_date_time': 'date_created'
        }
        
        assert column_map == expected_mappings, (
            f"CORE BUG: column_map values are not normalized for staging consistency.\n"
            f"Expected normalized mappings: {expected_mappings}\n"
            f"Actual mappings: {column_map}\n" 
            f"This prevents column mapping lookups in _determine_value_columns()"
        )
        
    def test_comparison_keys_preserve_original_names_for_display(self):
        """
        ASSERTION 2 (Preservation): Test that comparison keys maintain original names.
        
        While column_map needs normalized names for staging consistency, the main
        comparison configuration must preserve original names for:
        - User-facing error messages
        - Display in reports  
        - Compatibility with existing error handling
        
        This test ensures we don't break existing functionality.
        """
        # Arrange: Mock matches with complex original names
        mock_matches = [
            {
                'left_column': 'Internal ID.1',        # Complex original name
                'right_column': 'Transaction-Code',
                'confidence': 1.0,
                'match_reason': 'manual_selection'
            },
            {
                'left_column': 'User Name/Display',    # Forward slash  
                'right_column': 'From (Author)',       # Parentheses
                'confidence': 0.9,
                'match_reason': 'semantic_match'
            }
        ]
        
        # Validated keys preserve original format
        validated_keys = ['Internal ID.1', 'User Name/Display']
        
        # Act: Create config
        with patch('src.ui.menu.Path.exists', return_value=True):
            config = self.menu._create_interactive_config(
                left_file=self.left_file,
                right_file=self.right_file,
                matches=mock_matches,
                validated_keys=validated_keys
            )
        
        # Assert: Main comparison keys should preserve original names
        comparison_keys = config["comparisons"][0]["keys"]
        
        expected_original_keys = ['Internal ID.1', 'User Name/Display']
        assert comparison_keys == expected_original_keys, (
            f"REGRESSION: comparison keys should preserve original names for display.\n"
            f"Expected original keys: {expected_original_keys}\n"
            f"Actual keys: {comparison_keys}\n"
            f"Original names needed for user-facing error messages"
        )
        
        # Assert: Left dataset key_columns should be normalized for staging consistency
        left_dataset_name = list(config["datasets"].keys())[0]
        left_key_columns = config["datasets"][left_dataset_name]["key_columns"]
        
        expected_normalized_keys = ['internal_id_1', 'user_name_display']  # Normalized for staging
        assert left_key_columns == expected_normalized_keys, (
            f"Dataset key_columns should be normalized for staging consistency.\n"
            f"Expected normalized keys: {expected_normalized_keys}\n"
            f"Actual keys: {left_key_columns}\n"
            f"Dataset configs reference staged table columns (normalized names)"
        )
        
    def test_end_to_end_column_mapping_flow_reproduces_bug(self):
        """
        ASSERTION 3 (E2E Bug Reproduction): Test complete flow that reproduces the bug.
        
        This test simulates the exact scenario that causes only 2 columns to be compared:
        1. User approves 5+ column mappings with special characters
        2. Config created with original names (current buggy behavior)
        3. Later: staging normalizes column names
        4. Later: comparator._determine_value_columns() fails lookups
        5. Result: Only exact-match columns are compared
        
        This test documents the current broken behavior for regression testing.
        """
        # Arrange: Realistic scenario with NetSuite-style column names
        mock_matches = [
            {'left_column': 'Message ID', 'right_column': 'Internal ID.1', 'confidence': 1.0, 'match_reason': 'exact_match'},
            {'left_column': 'From Email', 'right_column': 'From-Address', 'confidence': 0.95, 'match_reason': 'semantic_match'}, 
            {'left_column': 'To Recipients', 'right_column': 'Recipient(s)', 'confidence': 0.88, 'match_reason': 'partial_match'},
            {'left_column': 'Subject Line', 'right_column': 'Email Subject/Title', 'confidence': 0.92, 'match_reason': 'semantic_match'},
            {'left_column': 'Attachment Count', 'right_column': 'Has Attachments?', 'confidence': 0.75, 'match_reason': 'data_type_match'},
            {'left_column': 'Date Sent', 'right_column': 'Created Date/Time', 'confidence': 0.89, 'match_reason': 'semantic_match'}
        ]
        
        validated_keys = ['Message ID']
        
        # Act: Create config with current buggy implementation
        with patch('src.ui.menu.Path.exists', return_value=True):
            config = self.menu._create_interactive_config(
                left_file=self.left_file,
                right_file=self.right_file,
                matches=mock_matches,
                validated_keys=validated_keys
            )
        
        # Assert: Demonstrate the normalization mismatch that causes the bug
        right_dataset_name = list(config["datasets"].keys())[1]
        column_map = config["datasets"][right_dataset_name]["column_map"]
        
        # Show what staged table columns would look like (normalized by stager.py)
        # These are the column names that _determine_value_columns() will iterate over
        expected_staged_left_columns = {
            'message_id',           # 'Message ID' → normalized
            'from_email',           # 'From Email' → normalized
            'to_recipients',        # 'To Recipients' → normalized  
            'subject_line',         # 'Subject Line' → normalized
            'attachment_count',     # 'Attachment Count' → normalized
            'date_sent'             # 'Date Sent' → normalized
        }
        
        # Show what the current column_map contains (original names - THIS IS THE BUG)
        # These are the names that _get_right_column() will search for 
        current_mapped_left_columns = set(column_map.values())
        expected_original_left_columns = {
            'Message ID', 'From Email', 'To Recipients', 
            'Subject Line', 'Attachment Count', 'Date Sent'
        }
        
        # After the fix: column_map should contain normalized names (FIXED BEHAVIOR)
        expected_normalized_left_columns = {
            'message_id', 'from_email', 'to_recipients',
            'subject_line', 'attachment_count', 'date_sent'
        }
        
        assert current_mapped_left_columns == expected_normalized_left_columns, (
            f"BUG FIXED: column_map now uses normalized names for staging consistency.\n"
            f"Expected normalized columns: {expected_normalized_left_columns}\n"
            f"Actual mapped columns: {current_mapped_left_columns}\n"
            f"This ensures _get_right_column() lookups succeed in comparator.py"
        )
        
        # Verify the fix: normalized mapped columns should match staged columns
        # This assertion should PASS after fix (demonstrates successful fix)
        assert current_mapped_left_columns == expected_staged_left_columns, (
            f"FIX VERIFICATION: Column name formats now match between config and staging.\n"
            f"Staged table columns (what _determine_value_columns iterates): {expected_staged_left_columns}\n"
            f"Column map left columns (what _get_right_column searches): {current_mapped_left_columns}\n"
            f"MATCH ACHIEVED: All {len(current_mapped_left_columns)} mapped columns will be found in lookups\n"
            f"This fixes the bug where only 2 exact-match columns were compared"
        )
        
    def test_fallback_behavior_when_no_validated_keys(self):
        """
        Test fallback to first match when no validated keys are provided.
        Both original names (for display) and normalized mappings should work correctly.
        """
        # Arrange: Matches without validated keys (fallback scenario)
        mock_matches = [
            {
                'left_column': 'Primary Key.1',        # Will become fallback key
                'right_column': 'ID-Number',
                'confidence': 1.0,
                'match_reason': 'manual_selection' 
            },
            {
                'left_column': 'Description Text',
                'right_column': 'Details/Notes',
                'confidence': 0.85,
                'match_reason': 'partial_match'
            }
        ]
        
        validated_keys = None  # Force fallback behavior
        
        # Act: Create config using fallback logic
        with patch('src.ui.menu.Path.exists', return_value=True):
            config = self.menu._create_interactive_config(
                left_file=self.left_file,
                right_file=self.right_file,
                matches=mock_matches, 
                validated_keys=validated_keys
            )
        
        # Assert: Fallback key should preserve original name in comparison config
        fallback_key = config["comparisons"][0]["keys"][0]
        assert fallback_key == 'Primary Key.1', (
            f"Fallback key should preserve original name. Got: {fallback_key}"
        )
        
        # Assert: Column mappings should still be normalized (the core fix)
        right_dataset_name = list(config["datasets"].keys())[1]
        column_map = config["datasets"][right_dataset_name]["column_map"]
        
        expected_normalized_mappings = {
            'id_number': 'primary_key_1',       # Both sides normalized
            'details_notes': 'description_text'
        }
        
        # This assertion will FAIL before fix (original names) and PASS after fix (normalized)
        assert column_map == expected_normalized_mappings, (
            f"CORE BUG: Even fallback scenario should use normalized column mappings.\n"
            f"Expected: {expected_normalized_mappings}\n"
            f"Actual: {column_map}"
        )
        
    def _normalize_column_name_for_test(self, col: str) -> str:
        """
        Test helper to simulate normalize_column_name() for assertions.
        This replicates the logic from src.utils.normalizers.normalize_column_name()
        """
        import re
        
        # Convert to lowercase
        normalized = col.lower()
        
        # Replace spaces and special characters with underscores  
        normalized = re.sub(r'[^\w\s]', '_', normalized)
        normalized = re.sub(r'\s+', '_', normalized)
        
        # Remove leading/trailing underscores
        normalized = normalized.strip('_')
        
        # Collapse multiple underscores
        normalized = re.sub(r'_+', '_', normalized)
        
        return normalized


class TestInteractiveConfigIntegration:
    """
    Integration tests that verify the config works with downstream components.
    These tests simulate the full pipeline to catch integration issues.
    """
    
    def setup_method(self):
        """Setup integration test fixtures."""
        self.menu = MenuInterface(data_dir=Path("test_data"))
    
    def test_config_compatibility_with_comparator_lookup_logic(self):
        """
        Test that generated config is compatible with comparator._get_right_column().
        
        This simulates how the comparator would use the column mapping:
        1. Staged table has normalized column names  
        2. _determine_value_columns() iterates with normalized names
        3. _get_right_column() searches column_map with normalized names
        4. Lookup should succeed with normalized mappings
        """
        # Arrange: Mock the exact scenario from the bug report
        mock_matches = [
            {'left_column': 'Internal ID.1', 'right_column': 'Message ID', 'confidence': 1.0, 'match_reason': 'exact'},
            {'left_column': 'From', 'right_column': 'Author Name', 'confidence': 0.9, 'match_reason': 'semantic'}
        ]
        
        # Act: Generate config
        with patch('src.ui.menu.Path.exists', return_value=True):
            config = self.menu._create_interactive_config(
                left_file=Path("left.xlsx"),
                right_file=Path("right.csv"),
                matches=mock_matches,
                validated_keys=['Internal ID.1']
            )
        
        # Assert: Simulate comparator._get_right_column() lookup logic
        right_dataset_name = list(config["datasets"].keys())[1]
        column_map = config["datasets"][right_dataset_name]["column_map"]
        
        # Simulate staged table column (normalized) being looked up
        staged_left_column = 'internal_id_1'  # What stager.py would create from 'Internal ID.1'
        
        # Simulate _get_right_column() lookup logic
        found_right_column = None
        for right_col, left_col in column_map.items():
            if left_col == staged_left_column:  # This lookup must succeed
                found_right_column = right_col
                break
        
        # This assertion will FAIL before fix (no match found) and PASS after fix
        assert found_right_column is not None, (
            f"INTEGRATION BUG: _get_right_column() lookup failed for staged column '{staged_left_column}'.\n"
            f"Column map: {column_map}\n"
            f"The staged column name doesn't match any left column in the mapping.\n"
            f"This causes _determine_value_columns() to exclude this column from comparison."
        )
        
        # Verify the found mapping is correct  
        expected_right_column = 'message_id'  # Normalized from 'Message ID'
        assert found_right_column == expected_right_column, (
            f"Wrong right column found. Expected: {expected_right_column}, Got: {found_right_column}"
        )


if __name__ == "__main__":
    """
    Run these tests to verify they FAIL before implementing the fix.
    
    Expected test results BEFORE fix:
    - test_column_map_uses_normalized_names_for_staging_consistency: FAIL
    - test_comparison_keys_preserve_original_names_for_display: PASS  
    - test_end_to_end_column_mapping_flow_reproduces_bug: FAIL
    - test_fallback_behavior_when_no_validated_keys: FAIL
    - test_config_compatibility_with_comparator_lookup_logic: FAIL
    
    TDD Protocol Next Steps:
    1. COMMIT these failing tests with message: "TDD Phase 1: Add failing tests for column normalization fix"
    2. Implement the fix in menu.py:_create_interactive_config()  
    3. Re-run tests to verify they PASS
    4. COMMIT the fix with message: "TDD Phase 2: Implement column normalization fix"
    """
    pytest.main([__file__, "-v"])