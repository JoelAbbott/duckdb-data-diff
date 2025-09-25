"""
Tests for MenuInterface column mapping conflict resolution.
Ensures _handle_conflicting_mapping correctly prioritizes existing mappings over new conflicting ones.
"""

import pytest
from unittest.mock import Mock, patch
from pathlib import Path
import sys

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.ui.menu import MenuInterface


class TestMenuMappingConflictResolution:
    """Test column mapping conflict resolution in MenuInterface."""
    
    def setup_method(self):
        """Setup test fixtures."""
        # Create MenuInterface with mock data directory
        self.menu = MenuInterface(data_dir=Path("test_data"))
        
    def test_conflict_resolution_prioritizes_existing_mapping_if_same_confidence(self):
        """
        Test that conflict resolution prioritizes existing mapping over new conflicting mapping.
        
        This reproduces the exact issue from the integration test:
        - 'From' initially maps to 'author' (key column, should be preserved)
        - 'From Email Address' attempts to also map to 'author' 
        - Conflict resolution should KEEP the original 'From' -> 'author' mapping
        - Should REJECT the conflicting 'From Email Address' -> 'author' mapping
        
        This prevents the key column from losing its mapping.
        """
        # Arrange: Set up initial approved mappings with high-priority 'From' mapping
        approved_matches = [
            {
                'left_column': 'From',
                'right_column': 'author', 
                'confidence': 1.0,
                'match_reason': 'exact_name_match'
            },
            {
                'left_column': 'Subject',
                'right_column': 'email_subject',
                'confidence': 1.0, 
                'match_reason': 'exact_name_match'
            }
        ]
        
        # Store initial state for comparison
        initial_matches_count = len(approved_matches)
        original_from_mapping = approved_matches[0].copy()
        
        # Act: Attempt to add conflicting mapping 'From Email Address' -> 'author'
        conflicting_right_column = 'author'
        conflicting_left_column = 'From Email Address'
        
        # Mock print to capture debug output during conflict resolution
        with patch('builtins.print') as mock_print:
            self.menu._handle_conflicting_mapping(
                approved_matches=approved_matches,
                right_column=conflicting_right_column, 
                new_left_column=conflicting_left_column
            )
        
        # Assert: Original 'From' -> 'author' mapping should be preserved
        
        # CRITICAL ASSERTION 1: The original mapping count should be preserved
        # (no mapping should be removed for same-confidence conflicts)
        assert len(approved_matches) == initial_matches_count, \
            f"Expected {initial_matches_count} mappings, but got {len(approved_matches)}. " \
            f"Conflict resolution removed the original mapping instead of rejecting the new one."
        
        # CRITICAL ASSERTION 2: Original 'From' -> 'author' mapping must still exist
        from_mapping_exists = any(
            match['left_column'] == 'From' and match['right_column'] == 'author'
            for match in approved_matches
        )
        assert from_mapping_exists, \
            f"Original 'From' -> 'author' mapping was removed during conflict resolution. " \
            f"Current mappings: {approved_matches}"
        
        # CRITICAL ASSERTION 3: The exact original mapping should be unchanged
        current_from_mapping = next(
            (match for match in approved_matches 
             if match['left_column'] == 'From' and match['right_column'] == 'author'), 
            None
        )
        assert current_from_mapping == original_from_mapping, \
            f"Original 'From' mapping was modified. Expected: {original_from_mapping}, " \
            f"Got: {current_from_mapping}"
        
        # CRITICAL ASSERTION 4: No conflicting 'From Email Address' -> 'author' should be added  
        email_address_mapping_exists = any(
            match['left_column'] == 'From Email Address' and match['right_column'] == 'author'
            for match in approved_matches
        )
        assert not email_address_mapping_exists, \
            f"Conflicting 'From Email Address' -> 'author' mapping should be rejected. " \
            f"Current mappings: {approved_matches}"
        
        # ASSERTION 5: Verify conflict was detected and handled
        # Check that print was called with conflict detection message
        print_calls = [str(call) for call in mock_print.call_args_list]
        conflict_detected = any('Conflict:' in call or 'already mapped to' in call for call in print_calls)
        assert conflict_detected, \
            f"Conflict resolution should have detected and logged the conflict. Print calls: {print_calls}"

    def test_conflict_resolution_removes_lower_confidence_mapping(self):
        """
        Test that conflict resolution removes lower confidence mapping when new mapping has higher confidence.
        
        This tests the expected behavior when confidence scores are different.
        """
        # Arrange: Set up mapping with lower confidence
        approved_matches = [
            {
                'left_column': 'From_Normalized', 
                'right_column': 'author',
                'confidence': 0.7,  # Lower confidence
                'match_reason': 'partial_name_match'
            }
        ]
        
        # Act: Add higher confidence mapping for same right column  
        with patch('builtins.print'):
            self.menu._handle_conflicting_mapping(
                approved_matches=approved_matches,
                right_column='author',
                new_left_column='From'  # This would be a higher confidence match
            )
        
        # Assert: Lower confidence mapping should be removed
        # (This is the current behavior that we want to preserve for different confidence)
        assert len(approved_matches) == 0, \
            "Lower confidence mapping should be removed when higher confidence mapping conflicts"

    def test_no_conflict_when_different_right_columns(self):
        """
        Test that no conflict occurs when mappings target different right columns.
        """
        # Arrange
        approved_matches = [
            {
                'left_column': 'From',
                'right_column': 'author',
                'confidence': 1.0,
                'match_reason': 'exact_name_match' 
            }
        ]
        
        initial_count = len(approved_matches)
        
        # Act: Try to add mapping to different right column (no conflict)
        with patch('builtins.print') as mock_print:
            self.menu._handle_conflicting_mapping(
                approved_matches=approved_matches,
                right_column='sender_email',  # Different right column
                new_left_column='From Email Address'
            )
        
        # Assert: No changes should occur
        assert len(approved_matches) == initial_count
        assert approved_matches[0]['left_column'] == 'From'
        assert approved_matches[0]['right_column'] == 'author'
        
        # Should log that no conflict was found
        print_calls = [str(call) for call in mock_print.call_args_list]
        no_conflict_logged = any('No conflict found' in call for call in print_calls)
        assert no_conflict_logged

    def test_integration_scenario_exact_reproduction(self):
        """
        Test the exact scenario from the integration test that caused the SQL failure.
        
        This reproduces the specific sequence:
        1. 'From' matches to 'author' (100% confidence) - SHOULD BE KEY COLUMN
        2. 'From Email Address' also matches to 'author' (100% confidence)  
        3. Conflict resolution should keep 'From' -> 'author' (first/existing wins)
        4. This prevents the SQL error: "Table 'r' does not have a column named 'From'"
        """
        # Arrange: Exact sequence from integration test
        approved_matches = []
        
        # Step 1: First mapping is accepted (this will become key column)
        first_mapping = {
            'left_column': 'From',
            'right_column': 'author',
            'confidence': 1.0,
            'match_reason': 'exact_name_match'
        }
        approved_matches.append(first_mapping)
        
        # Step 2: Conflicting mapping attempts to overwrite
        with patch('builtins.print') as mock_print:
            self.menu._handle_conflicting_mapping(
                approved_matches=approved_matches,
                right_column='author',  # Same right column  
                new_left_column='From Email Address'  # Conflicting left column
            )
        
        # Assert: Key column mapping must be preserved for SQL generation
        key_mapping_preserved = any(
            match['left_column'] == 'From' and match['right_column'] == 'author'
            for match in approved_matches
        )
        
        assert key_mapping_preserved, \
            "Integration test failure: 'From' -> 'author' mapping was lost. " \
            "This causes SQL error: 'Table r does not have a column named From' " \
            "because the key column 'From' no longer maps to the correct right column 'author'."
        
        # Verify SQL would be generated correctly
        # _get_right_column('From') should find 'author' in the mapping
        column_map = {match['right_column']: match['left_column'] for match in approved_matches}
        expected_right_column = None
        for right_col, left_col in column_map.items():
            if left_col == 'From':
                expected_right_column = right_col
                break
                
        assert expected_right_column == 'author', \
            f"Column mapping broken: 'From' should map to 'author' for SQL generation. " \
            f"Current mapping: {column_map}"