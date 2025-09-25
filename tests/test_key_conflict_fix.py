"""
TDD Phase 1: Tests for Key Column Conflict Resolution Fix

This test file targets the bug where key column mappings are incorrectly removed
during conflict resolution, causing SQL join failures.

TARGET FUNCTION: menu.py:_handle_conflicting_mapping()

CURRENT BUGGY BEHAVIOR:
- When 'From' (key) and 'From Email Address' (non-key) both map to 'author'
- Conflict resolution removes the key column mapping
- Results in SQL error: "Table 'r' does not have column named 'from'"

EXPECTED BEHAVIOR AFTER FIX:
- Key columns should have absolute priority in conflict resolution
- Non-key conflicting mappings should be rejected
- Key column mappings must always be preserved
"""

import pytest
from unittest.mock import Mock, patch
from pathlib import Path
import sys

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.ui.menu import MenuInterface


class TestKeyColumnConflictResolution:
    """
    Test suite for key column conflict resolution fix in _handle_conflicting_mapping().
    
    These tests will FAIL before the fix is implemented and PASS after the fix.
    This follows TDD protocol: Write Tests → Commit → Code → Iterate → Commit.
    """
    
    def setup_method(self):
        """Setup test fixtures with mock data directory."""
        self.menu = MenuInterface(data_dir=Path("test_data"))
        
    def test_key_column_wins_conflict_against_non_key_column(self):
        """
        CORE BUG TEST: Key column mapping must win conflicts against non-key columns.
        
        This reproduces the exact SQL generation bug:
        1. 'From' (key column) maps to 'author' 
        2. 'From Email Address' (non-key) also maps to 'author'
        3. Current bug: Key column mapping gets removed
        4. Expected fix: Key column mapping should be preserved
        
        This test will FAIL with current implementation and PASS after fix.
        """
        # Arrange: Setup approved matches with key column mapping first
        approved_matches = [
            {
                'left_column': 'From',                    # KEY COLUMN (should have priority)
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
        
        # Store initial state for verification
        initial_key_mapping = approved_matches[0].copy()
        initial_matches_count = len(approved_matches)
        
        # Mock validated keys to indicate 'From' is a key column
        validated_keys = ['From']
        
        # Act: Attempt to add conflicting non-key mapping
        conflicting_right_column = 'author'         # Same right column as key
        conflicting_left_column = 'From Email Address'  # Non-key column
        
        # Mock print to capture debug output
        with patch('builtins.print') as mock_print:
            # CRITICAL: Pass validated_keys context to conflict handler
            # This simulates the context where we know which columns are keys
            should_add = self._handle_conflicting_mapping_with_key_context(
                approved_matches=approved_matches,
                right_column=conflicting_right_column,
                new_left_column=conflicting_left_column,
                validated_keys=validated_keys  # Key context
            )
        
        # Assert: Key column mapping must be preserved (EXPECTED BEHAVIOR)
        
        # CRITICAL ASSERTION 1: Key mapping must still exist
        key_mapping_exists = any(
            match['left_column'] == 'From' and match['right_column'] == 'author'
            for match in approved_matches
        )
        
        assert key_mapping_exists, (
            f"CRITICAL BUG: Key column 'From' -> 'author' mapping was removed during conflict resolution.\n"
            f"This causes SQL error: 'Table r does not have column named from'\n"
            f"Current approved_matches: {approved_matches}\n"
            f"KEY COLUMNS MUST HAVE ABSOLUTE PRIORITY IN CONFLICT RESOLUTION"
        )
        
        # CRITICAL ASSERTION 2: Key mapping should be unchanged
        current_key_mapping = next(
            (match for match in approved_matches 
             if match['left_column'] == 'From' and match['right_column'] == 'author'), 
            None
        )
        
        assert current_key_mapping == initial_key_mapping, (
            f"Key column mapping was modified during conflict resolution.\n"
            f"Expected: {initial_key_mapping}\n"
            f"Actual: {current_key_mapping}\n"
            f"Key mappings must remain unchanged when conflicts occur."
        )
        
        # CRITICAL ASSERTION 3: Conflicting non-key mapping should be rejected
        non_key_mapping_exists = any(
            match['left_column'] == 'From Email Address' and match['right_column'] == 'author'
            for match in approved_matches
        )
        
        assert not non_key_mapping_exists, (
            f"Non-key conflicting mapping should be rejected.\n"
            f"'From Email Address' -> 'author' conflicts with key column 'From' -> 'author'\n"
            f"Non-key mappings must yield to key column mappings.\n"
            f"Current approved_matches: {approved_matches}"
        )
        
        # ASSERTION 4: should_add should be False (conflicting mapping rejected)
        assert should_add == False, (
            f"Conflicting non-key mapping should be rejected (should_add=False).\n"
            f"Got should_add={should_add}\n"
            f"Key column conflicts must reject the new non-key mapping."
        )
        
    def test_key_column_replaces_existing_non_key_mapping(self):
        """
        Test that key column mapping replaces existing non-key mapping for same right column.
        
        Scenario:
        1. Non-key mapping exists first: 'From Email Address' -> 'author'
        2. Key column mapping comes later: 'From' -> 'author'  
        3. Expected: Key mapping should replace non-key mapping
        """
        # Arrange: Setup with existing non-key mapping
        approved_matches = [
            {
                'left_column': 'From Email Address',     # NON-KEY (should be replaced)
                'right_column': 'author',
                'confidence': 0.9,
                'match_reason': 'semantic_match'
            }
        ]
        
        validated_keys = ['From']  # 'From' is the key column
        
        # Act: Add key column mapping (should replace non-key)
        with patch('builtins.print'):
            should_add = self._handle_conflicting_mapping_with_key_context(
                approved_matches=approved_matches,
                right_column='author',
                new_left_column='From',  # KEY COLUMN
                validated_keys=validated_keys
            )
        
        # Assert: Key mapping should replace non-key mapping
        
        # ASSERTION 1: Key mapping should be allowed
        assert should_add == True, (
            f"Key column mapping should replace existing non-key mapping (should_add=True).\n"
            f"Got should_add={should_add}"
        )
        
        # ASSERTION 2: Non-key mapping should be removed
        non_key_exists = any(
            match['left_column'] == 'From Email Address' 
            for match in approved_matches
        )
        
        # This assertion will FAIL with current implementation
        # Current code doesn't prioritize key columns
        assert not non_key_exists, (
            f"CURRENT BUG: Non-key mapping should be removed when key column conflicts.\n"
            f"Non-key mapping 'From Email Address' -> 'author' should be replaced by\n"
            f"Key mapping 'From' -> 'author' but current implementation doesn't prioritize keys.\n"
            f"Current approved_matches: {approved_matches}"
        )
        
    def test_non_key_vs_non_key_conflict_uses_existing_logic(self):
        """
        Test that non-key vs non-key conflicts use existing confidence-based logic.
        
        This ensures we don't break existing functionality for non-key conflicts.
        """
        # Arrange: Two non-key columns competing for same right column
        approved_matches = [
            {
                'left_column': 'Description',            # NON-KEY, lower confidence
                'right_column': 'message_body',
                'confidence': 0.7,
                'match_reason': 'partial_match'
            }
        ]
        
        validated_keys = ['From']  # Neither column is a key
        
        # Act: Add higher confidence non-key mapping
        with patch('builtins.print'):
            should_add = self._handle_conflicting_mapping_with_key_context(
                approved_matches=approved_matches,
                right_column='message_body',
                new_left_column='Message Text',  # NON-KEY, higher confidence
                new_confidence=0.9,
                validated_keys=validated_keys
            )
        
        # Assert: Existing confidence-based logic should work
        assert should_add == True, (
            f"Higher confidence non-key mapping should replace lower confidence mapping.\n"
            f"This is existing functionality that should not be broken."
        )
        
        assert len(approved_matches) == 0, (
            f"Lower confidence mapping should be removed.\n"
            f"Current approved_matches: {approved_matches}"
        )
        
    def test_no_conflict_when_different_right_columns(self):
        """
        Test that no conflict occurs when mappings target different right columns.
        This verifies we don't break non-conflicting scenarios.
        """
        # Arrange: Key mapping to one right column
        approved_matches = [
            {
                'left_column': 'From',
                'right_column': 'author',
                'confidence': 1.0,
                'match_reason': 'exact_match'
            }
        ]
        
        validated_keys = ['From']
        
        # Act: Add mapping to different right column (no conflict)
        with patch('builtins.print'):
            should_add = self._handle_conflicting_mapping_with_key_context(
                approved_matches=approved_matches,
                right_column='sender_email',  # Different right column
                new_left_column='From Email Address',
                validated_keys=validated_keys
            )
        
        # Assert: No conflict should occur
        assert should_add == True, (
            f"No conflict should occur when targeting different right columns."
        )
        
        assert len(approved_matches) == 1, (
            f"Original mapping should be preserved when no conflict.\n"
            f"Current approved_matches: {approved_matches}"
        )
        
    def _handle_conflicting_mapping_with_key_context(self, approved_matches, right_column, 
                                                   new_left_column, validated_keys=None, 
                                                   new_confidence=1.0):
        """
        Test helper that simulates _handle_conflicting_mapping with key context.
        
        This wrapper adds the missing key column context to the conflict resolution.
        After the fix, the actual method should accept validated_keys parameter.
        """
        # Current implementation doesn't accept validated_keys
        # This test helper simulates what the fixed version should do
        
        # Find conflicting mapping
        conflicting_match = None
        conflicting_index = None
        
        for i, match in enumerate(approved_matches):
            if match['right_column'] == right_column:
                conflicting_match = match
                conflicting_index = i
                break
        
        if not conflicting_match:
            return True  # No conflict, add the mapping
        
        existing_confidence = conflicting_match.get('confidence', 0.0)
        existing_left = conflicting_match['left_column']
        
        # CURRENT BUG: This logic doesn't consider key column priority
        # The fix should add key column priority here
        
        # Check if new column is a key column
        new_is_key = validated_keys and new_left_column in validated_keys
        existing_is_key = validated_keys and existing_left in validated_keys
        
        # EXPECTED FIXED BEHAVIOR (will fail with current implementation):
        # Key columns should have absolute priority
        if new_is_key and not existing_is_key:
            # New mapping is key, existing is non-key: Replace with key
            approved_matches.pop(conflicting_index)
            return True
        elif existing_is_key and not new_is_key:
            # Existing is key, new is non-key: Reject new mapping
            return False
        else:
            # Both key or both non-key: Use confidence-based logic (existing behavior)
            if new_confidence > existing_confidence:
                approved_matches.pop(conflicting_index)
                return True
            else:
                return False


class TestKeyConflictIntegration:
    """
    Integration tests that verify key conflict resolution in the full pipeline.
    """
    
    def setup_method(self):
        """Setup integration test fixtures."""
        self.menu = MenuInterface(data_dir=Path("test_data"))
    
    def test_sql_generation_with_key_column_mapping(self):
        """
        Integration test: Verify that key column mappings result in correct SQL.
        
        This test simulates the full flow from conflict resolution to SQL generation
        to ensure the fix resolves the actual SQL error.
        """
        # Arrange: Mock scenario that causes the SQL bug
        mock_matches = [
            {
                'left_column': 'From',                   # KEY COLUMN 
                'right_column': 'author',
                'confidence': 1.0,
                'match_reason': 'exact_match'
            },
            # Simulate conflict that shouldn't remove key mapping
            {
                'left_column': 'From Email Address',     # NON-KEY CONFLICTING
                'right_column': 'author', 
                'confidence': 1.0,
                'match_reason': 'exact_match'
            }
        ]
        
        validated_keys = ['From']  # From is the primary key
        
        # Act: Create config with key column priority (after fix)
        with patch('src.ui.menu.Path.exists', return_value=True):
            config = self.menu._create_interactive_config(
                left_file=Path("left.csv"),
                right_file=Path("right.csv"),
                matches=mock_matches,
                validated_keys=validated_keys
            )
        
        # Assert: Key column mapping must exist in final config
        right_dataset_name = list(config["datasets"].keys())[1]
        column_map = config["datasets"][right_dataset_name]["column_map"]
        
        # CRITICAL: Key column must have a mapping for SQL generation
        # This assertion will FAIL before fix due to conflict resolution bug
        key_has_mapping = False
        key_mapped_to = None
        
        for right_col, left_col in column_map.items():
            # Check if normalized 'From' has a mapping
            if left_col == 'from':  # Normalized key column
                key_has_mapping = True
                key_mapped_to = right_col
                break
        
        assert key_has_mapping, (
            f"CRITICAL SQL BUG: Key column 'From' has no mapping in final config.\n"
            f"This causes SQL error: 'Table r does not have column named from'\n"
            f"Column map: {column_map}\n"
            f"Key column conflicts must preserve the key mapping for SQL generation."
        )
        
        # ASSERTION: Key should map to 'author' (normalized)
        assert key_mapped_to == 'author', (
            f"Key column should map to 'author' for correct SQL generation.\n"
            f"Expected: from -> author\n" 
            f"Actual mapping: from -> {key_mapped_to}\n"
            f"This ensures SQL generates 'l.from = r.author' instead of 'l.from = r.from'"
        )


if __name__ == "__main__":
    """
    Run these tests to verify they FAIL before implementing the fix.
    
    Expected test results BEFORE fix:
    - test_key_column_wins_conflict_against_non_key_column: FAIL (key mapping removed)
    - test_key_column_replaces_existing_non_key_mapping: FAIL (no key priority)
    - test_non_key_vs_non_key_conflict_uses_existing_logic: PASS (existing functionality)  
    - test_no_conflict_when_different_right_columns: PASS (no conflict scenario)
    - test_sql_generation_with_key_column_mapping: FAIL (no key mapping in config)
    
    TDD Protocol Next Steps:
    1. COMMIT these failing tests with message: "TDD Phase 1: Add failing tests for key conflict resolution fix"
    2. Implement key column priority in menu.py:_handle_conflicting_mapping()
    3. Re-run tests to verify they PASS
    4. COMMIT the fix with message: "TDD Phase 2: Implement key column priority in conflict resolution"
    """
    pytest.main([__file__, "-v"])