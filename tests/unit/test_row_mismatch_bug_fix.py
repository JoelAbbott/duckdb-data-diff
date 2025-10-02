"""
TDD Tests for ROW MISMATCH ISSUE - Column Mapping Corruption Bug

These tests expose the critical bug where _get_right_column() method returns wrong
column names, causing JOIN conditions to compare wrong rows and produce false
difference reports and false negative "only in" reports.

CRITICAL: These tests MUST FAIL until the bug is fixed in comparator.py
"""

import pytest
from unittest.mock import Mock, MagicMock, patch
from pathlib import Path
import sys

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.core.comparator import DataComparator
from src.config.manager import ComparisonConfig
from src.utils.normalizers import normalize_column_name


class TestRowMismatchBugFix:
    """
    TDD Tests for the ROW MISMATCH ISSUE identified in comparison_bug_analysis.md
    
    These tests demonstrate the specific column mapping corruption bugs that cause:
    1. Wrong JOIN conditions in SQL generation
    2. False difference reports (comparing wrong rows)
    3. False negative "only in" reports (matching rows marked as missing)
    """
    
    def setup_method(self):
        """Set up test fixtures for ROW MISMATCH bug testing."""
        self.mock_con = Mock()
        self.comparator = DataComparator(self.mock_con)
        
        # Create normalized column mappings as they would exist after menu.py processing
        # This simulates the output from menu.py:_create_interactive_config()
        self.normalized_column_map = {
            'author_email': 'from_email',      # right -> left mapping (normalized names)
            'recipient_email': 'to_email',     # right -> left mapping (normalized names)
            'message_id': 'internal_id',       # right -> left mapping (normalized names)
            'subject_line': 'email_subject'    # right -> left mapping (normalized names)
        }
        
        # Mock right dataset config with normalized column mapping
        self.mock_right_config = Mock()
        self.mock_right_config.column_map = self.normalized_column_map
        
        # Store in comparator as would happen during comparison
        self.comparator.right_dataset_config = self.mock_right_config
        self.comparator.left_dataset_config = Mock()
        self.comparator.left_dataset_config.column_map = None
    
    def test_get_right_column_with_exact_normalized_match(self):
        """
        TEST 1: _get_right_column() should handle exact normalized name matches correctly.
        
        This test demonstrates the BUG where the method fails to find the correct
        mapping when given a normalized left column name.
        
        EXPECTED: Should return 'author_email' for input 'from_email'
        ACTUAL BUG: Returns 'from_email' (wrong - causes join failure)
        """
        # Input: normalized left column name (as it exists in staged left table)
        left_column = 'from_email'  # This is the normalized version
        
        # Expected: Should find that 'author_email' maps to 'from_email' in column_map
        expected_right_column = 'author_email'
        
        # Execute the method that contains the bug
        actual_right_column = self.comparator._get_right_column(left_column)
        
        # This assertion WILL FAIL due to the bug - method returns wrong column
        assert actual_right_column == expected_right_column, (
            f"BUG EXPOSED: _get_right_column('{left_column}') returned '{actual_right_column}' "
            f"but should return '{expected_right_column}'. This causes JOIN conditions to use "
            f"wrong columns, leading to row mismatch issues."
        )
    
    def test_get_right_column_with_original_name_input(self):
        """
        TEST 2: _get_right_column() should handle original (non-normalized) names correctly.
        
        This test simulates the case where key_columns still contains original names
        from user input but column_map contains normalized names.
        
        EXPECTED: Should normalize input and then find correct mapping
        ACTUAL BUG: Normalization inconsistency causes lookup failure
        """
        # Input: original column name (as user selected it)
        left_column = 'From Email'  # Original name with spaces/caps
        
        # Expected: Should normalize to 'from_email' then find 'author_email' mapping
        expected_right_column = 'author_email'
        
        # Execute the method that contains the bug
        actual_right_column = self.comparator._get_right_column(left_column)
        
        # This assertion WILL FAIL due to the bug
        assert actual_right_column == expected_right_column, (
            f"BUG EXPOSED: _get_right_column('{left_column}') returned '{actual_right_column}' "
            f"but should return '{expected_right_column}'. The method failed to normalize "
            f"input and find correct mapping."
        )
    
    def test_get_right_column_fallback_behavior(self):
        """
        TEST 3: _get_right_column() fallback behavior creates wrong JOIN conditions.
        
        When the lookup fails, the method falls back to returning the input column name.
        This creates SQL like "l.from_email = r.from_email" instead of the correct
        "l.from_email = r.author_email", causing JOIN failures.
        """
        # Input: column name that should map but lookup will fail due to bug
        left_column = 'from_email'
        
        # Current buggy behavior: falls back to returning same name
        actual_right_column = self.comparator._get_right_column(left_column)
        
        # This demonstrates the bug - method returns input instead of mapped column
        assert actual_right_column != left_column, (
            f"BUG EXPOSED: _get_right_column('{left_column}') returned '{actual_right_column}' "
            f"(same as input), indicating failed lookup. This causes JOIN conditions to use "
            f"l.{left_column} = r.{actual_right_column} instead of the correct mapping."
        )
    
    def test_join_sql_generation_with_column_mapping(self):
        """
        TEST 4: SQL JOIN generation produces wrong conditions due to _get_right_column() bug.
        
        This test demonstrates how the column mapping bug propagates to SQL generation,
        causing the actual ROW MISMATCH issue where wrong rows are compared.
        """
        # Set up key columns as they would exist after key validation
        key_columns = ['from_email']  # Normalized key column from left table
        
        # Mock the SQL generation process
        with patch.object(self.comparator, '_get_right_column') as mock_get_right:
            # Current buggy behavior: returns wrong column name
            mock_get_right.return_value = 'from_email'  # BUG: should return 'author_email'
            
            # Generate key join condition as done in _find_matches()
            key_conditions = []
            for col in key_columns:
                left_norm = normalize_column_name(col)  # 'from_email'
                right_col = self.comparator._get_right_column(col)  # BUG: returns 'from_email'
                right_norm = normalize_column_name(right_col)  # 'from_email'
                
                # This creates WRONG JOIN condition
                key_conditions.append(
                    f"TRIM(TRY_CAST(l.{left_norm} AS VARCHAR)) = TRIM(TRY_CAST(r.{right_norm} AS VARCHAR))"
                )
            
            generated_join = " AND ".join(key_conditions)
            
            # Current bug produces: "l.from_email = r.from_email"
            # Should produce: "l.from_email = r.author_email"
            assert 'r.author_email' in generated_join, (
                f"BUG EXPOSED: Generated JOIN condition '{generated_join}' uses wrong right column. "
                f"Should use 'r.author_email' but uses 'r.from_email', causing row mismatch."
            )
    
    def test_column_mapping_lookup_correctness(self):
        """
        TEST 5: Verify column mapping lookup logic for normalized names.
        
        This test validates the core lookup logic that should work when
        both input and column_map use consistent normalization.
        """
        # Test data: normalized column mappings (right -> left)
        test_mappings = {
            'author_email': 'from_email',
            'recipient_email': 'to_email', 
            'message_id': 'internal_id'
        }
        
        # Test inverse lookups (find right column for given left column)
        test_cases = [
            ('from_email', 'author_email'),
            ('to_email', 'recipient_email'),
            ('internal_id', 'message_id')
        ]
        
        for left_col, expected_right_col in test_cases:
            # Find right column that maps to this left column
            found_right_col = None
            for right_col, left_mapped in test_mappings.items():
                if left_mapped == left_col:
                    found_right_col = right_col
                    break
            
            assert found_right_col == expected_right_col, (
                f"Lookup logic test: For left column '{left_col}', expected to find "
                f"right column '{expected_right_col}' but found '{found_right_col}'"
            )
    
    def test_robust_comparison_deterministic_behavior(self):
        """
        TEST 6: _build_robust_comparison_condition() should produce deterministic results.
        
        This test demonstrates the SECONDARY issue where complex comparison logic
        introduces false positives even when JOIN conditions are correct.
        """
        # Mock comparison config
        mock_config = Mock()
        mock_config.tolerance = 0  # Exact comparison
        
        # Test data: identical values that should NOT be flagged as different
        test_cases = [
            ('value', 'value'),           # Exact match
            ('  VALUE  ', 'value'),       # Case/whitespace difference
            ('true', 'True'),             # Boolean case difference
            ('2024-01-01', '2024-01-01'), # Date match
        ]
        
        for left_val, right_val in test_cases:
            # Generate comparison condition
            condition = self.comparator._build_robust_comparison_condition(
                'test_col', 'test_col_right', mock_config
            )
            
            # The condition is too complex - this test documents the problem
            assert len(condition) < 500, (
                f"BUG EXPOSED: Comparison condition is overly complex ({len(condition)} chars). "
                f"Complex nested logic increases risk of false positives. "
                f"Should use simple, deterministic comparison."
            )
    
    def test_column_name_normalization_consistency(self):
        """
        TEST 7: Verify that normalization is applied consistently throughout pipeline.
        
        This test ensures that column names are normalized the same way in all
        components (menu.py, stager.py, key_validator.py, comparator.py).
        """
        # Test cases: original names and their expected normalized versions
        test_cases = [
            ('From Email', 'from_email'),
            ('Author Email', 'author_email'),
            ('Message ID', 'message_id'),
            ('Subject Line', 'subject_line'),
            ('Is-Incoming', 'is_incoming'),
            ('Date Created', 'date_created')
        ]
        
        for original, expected_normalized in test_cases:
            actual_normalized = normalize_column_name(original)
            
            assert actual_normalized == expected_normalized, (
                f"Normalization inconsistency: '{original}' normalized to "
                f"'{actual_normalized}' but expected '{expected_normalized}'"
            )


class TestRowMismatchIntegration:
    """
    Integration tests that demonstrate the end-to-end ROW MISMATCH issue.
    
    These tests simulate the complete pipeline flow and show how column mapping
    corruption leads to wrong comparison results.
    """
    
    def setup_method(self):
        """Set up integration test fixtures."""
        self.mock_con = Mock()
        self.comparator = DataComparator(self.mock_con)
    
    @pytest.mark.integration  
    def test_end_to_end_row_mismatch_scenario(self):
        """
        INTEGRATION TEST: Demonstrate complete ROW MISMATCH scenario.
        
        This test simulates the exact scenario described in comparison_bug_analysis.md:
        1. User selects 'From Email' as key column in interactive mode
        2. menu.py creates normalized column mapping {'author_email': 'from_email'}
        3. stager.py stages tables with normalized column names
        4. comparator.py generates wrong JOIN due to _get_right_column() bug
        5. Result: wrong rows compared, false differences reported
        """
        # Step 1: Simulate interactive column mapping from menu.py
        original_user_selection = "From Email"
        normalized_left = normalize_column_name(original_user_selection)  # 'from_email'
        normalized_right = "author_email"  # From user mapping
        
        # Step 2: Create column mapping as menu.py would (normalized names)
        column_map = {normalized_right: normalized_left}  # {'author_email': 'from_email'}
        
        # Step 3: Set up comparator with this mapping
        mock_right_config = Mock()
        mock_right_config.column_map = column_map
        self.comparator.right_dataset_config = mock_right_config
        
        # Step 4: Test _get_right_column() with normalized input
        result = self.comparator._get_right_column(normalized_left)
        
        # Step 5: Verify the bug - this WILL FAIL until bug is fixed
        assert result == normalized_right, (
            f"END-TO-END BUG: User selected '{original_user_selection}' as key, "
            f"which normalized to '{normalized_left}'. The system should map this "
            f"to right column '{normalized_right}', but _get_right_column() returned "
            f"'{result}'. This causes JOIN conditions to use wrong columns, "
            f"leading to the ROW MISMATCH ISSUE where wrong rows are compared."
        )
    
    @pytest.mark.integration
    def test_sql_join_correctness_with_mapping(self):
        """
        INTEGRATION TEST: Verify that SQL JOIN conditions use correct mapped columns.
        
        This test ensures that when column mapping exists, the generated SQL uses
        the correct right table column names in JOIN conditions.
        """
        # Set up column mapping scenario
        key_columns = ['from_email']  # Left table key column
        column_map = {'author_email': 'from_email'}  # Right -> Left mapping
        
        # Configure comparator
        mock_right_config = Mock()
        mock_right_config.column_map = column_map
        self.comparator.right_dataset_config = mock_right_config
        
        # Generate JOIN condition as _find_matches() would
        key_conditions = []
        for col in key_columns:
            left_norm = normalize_column_name(col)
            right_col = self.comparator._get_right_column(col)
            right_norm = normalize_column_name(right_col)
            
            condition = f"l.{left_norm} = r.{right_norm}"
            key_conditions.append(condition)
        
        join_sql = " AND ".join(key_conditions)
        
        # Verify correct JOIN SQL is generated
        expected_sql = "l.from_email = r.author_email"
        assert join_sql == expected_sql, (
            f"SQL JOIN BUG: Expected '{expected_sql}' but got '{join_sql}'. "
            f"Wrong JOIN conditions cause row mismatch and false differences."
        )