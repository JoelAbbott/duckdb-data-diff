"""
Test suite for menu-driven interface.
Following TDD approach - tests written before implementation.
"""

import pytest
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import sys

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))

# Add imports for our specific MenuInterface testing
try:
    from src.ui.menu import MenuInterface
    from src.utils.normalizers import normalize_column_name
    MENU_INTERFACE_AVAILABLE = True
except ImportError:
    MENU_INTERFACE_AVAILABLE = False

# Legacy imports for backward compatibility (may not exist)
try:
    from compare_datasets import menu_interface, MenuDrivenComparator
    LEGACY_AVAILABLE = True
except ImportError:
    LEGACY_AVAILABLE = False


@pytest.mark.skipif(not LEGACY_AVAILABLE, reason="Legacy menu interface not available")
class TestLegacyMenuInterface:
    """Test suite for menu-driven interface functionality."""
    
    def test_main_menu_display(self):
        """Test that main menu displays correct options."""
        with patch('builtins.input', return_value='6'):
            with patch('builtins.print') as mock_print:
                menu_interface()
                
                # Check that menu header is displayed
                calls = [str(call) for call in mock_print.call_args_list]
                assert any('DUCKDB DATA COMPARISON SYSTEM' in call for call in calls)
                
                # Check all menu options are displayed
                assert any('1. Quick Comparison' in call for call in calls)
                assert any('2. Interactive Comparison' in call for call in calls)
                assert any('3. Use Existing Configuration' in call for call in calls)
                assert any('4. Clean Up Old Files' in call for call in calls)
                assert any('5. View System Status' in call for call in calls)
                assert any('6. Exit' in call for call in calls)
    
    def test_menu_option_quick_comparison(self):
        """Test quick comparison option."""
        with patch('builtins.input') as mock_input:
            # Simulate menu selection and file selection
            mock_input.side_effect = [
                '1',  # Select quick comparison
                '1',  # Select first file from list
                '2',  # Select second file from list
                'n',  # Don't run pipeline
                '6'   # Exit
            ]
            
            with patch('compare_datasets.DatasetComparator') as mock_comparator:
                with patch('compare_datasets.list_data_files', return_value=['file1.csv', 'file2.xlsx']):
                    menu_interface()
                    
                    # Should create comparator with correct files
                    mock_comparator.assert_called()
                    args = mock_comparator.call_args
                    assert 'file1.csv' in str(args) or 'file2.xlsx' in str(args)
    
    def test_menu_option_interactive_comparison(self):
        """Test interactive comparison option."""
        with patch('builtins.input') as mock_input:
            mock_input.side_effect = [
                '2',  # Select interactive comparison
                '1',  # Select first file
                '2',  # Select second file
                '6'   # Exit
            ]
            
            with patch('compare_datasets.DatasetComparator') as mock_comparator:
                with patch('compare_datasets.list_data_files', return_value=['file1.csv', 'file2.xlsx']):
                    menu_interface()
                    
                    # Should create comparator with interactive=True
                    mock_comparator.assert_called()
                    kwargs = mock_comparator.call_args.kwargs
                    assert kwargs.get('interactive') == True
    
    def test_menu_option_use_existing_config(self):
        """Test using existing configuration option."""
        with patch('builtins.input') as mock_input:
            mock_input.side_effect = [
                '3',  # Select use existing config
                '1',  # Select config file
                '6'   # Exit
            ]
            
            with patch('compare_datasets.list_config_files', return_value=['config1.yaml']):
                with patch('compare_datasets.run_with_existing_config') as mock_run:
                    menu_interface()
                    
                    # Should run with selected config
                    mock_run.assert_called_once()
                    assert 'config1.yaml' in str(mock_run.call_args)
    
    def test_menu_option_cleanup(self):
        """Test cleanup old files option."""
        with patch('builtins.input') as mock_input:
            mock_input.side_effect = [
                '4',  # Select cleanup
                '7',  # Keep last 7 days
                'y',  # Confirm cleanup
                '6'   # Exit
            ]
            
            with patch('compare_datasets.cleanup_old_comparisons') as mock_cleanup:
                menu_interface()
                
                # Should call cleanup function
                mock_cleanup.assert_called_once()
                args = mock_cleanup.call_args
                assert args[1]['days_to_keep'] == 7
    
    def test_menu_option_system_status(self):
        """Test view system status option."""
        with patch('builtins.input') as mock_input:
            mock_input.side_effect = [
                '5',  # Select system status
                '',   # Press enter to continue
                '6'   # Exit
            ]
            
            with patch('builtins.print') as mock_print:
                with patch('compare_datasets.get_system_status', return_value={
                    'comparisons_count': 5,
                    'total_size': '1.2 GB',
                    'last_run': '2024-12-23 14:30:22'
                }):
                    menu_interface()
                    
                    # Should display status information
                    calls = [str(call) for call in mock_print.call_args_list]
                    assert any('System Status' in call for call in calls)
                    assert any('5' in call for call in calls)  # comparisons count
                    assert any('1.2 GB' in call for call in calls)
    
    def test_file_listing_functionality(self):
        """Test that files are listed from data/raw directory."""
        from compare_datasets import list_data_files
        
        with patch('pathlib.Path.iterdir') as mock_iterdir:
            mock_iterdir.return_value = [
                MagicMock(is_file=lambda: True, suffix='.csv', name='file1.csv'),
                MagicMock(is_file=lambda: True, suffix='.xlsx', name='file2.xlsx'),
                MagicMock(is_file=lambda: False, name='subdir'),  # Should be ignored
                MagicMock(is_file=lambda: True, suffix='.txt', name='readme.txt')  # Should be ignored
            ]
            
            files = list_data_files()
            
            # Should only return CSV and Excel files
            assert len(files) == 2
            assert 'file1.csv' in files
            assert 'file2.xlsx' in files
            assert 'readme.txt' not in files
    
    def test_invalid_menu_selection(self):
        """Test handling of invalid menu selections."""
        with patch('builtins.input') as mock_input:
            mock_input.side_effect = [
                '99',  # Invalid option
                'abc', # Non-numeric input
                '6'    # Exit
            ]
            
            with patch('builtins.print') as mock_print:
                menu_interface()
                
                # Should show error messages
                calls = [str(call) for call in mock_print.call_args_list]
                assert any('Invalid' in call or 'invalid' in call for call in calls)
    
    def test_menu_return_after_operation(self):
        """Test that menu returns after completing an operation."""
        with patch('builtins.input') as mock_input:
            mock_input.side_effect = [
                '5',  # View status
                '',   # Continue
                '1',  # Quick comparison
                '0',  # Cancel file selection
                '6'   # Exit
            ]
            
            with patch('builtins.print') as mock_print:
                menu_interface()
                
                # Menu should be displayed multiple times
                calls = [str(call) for call in mock_print.call_args_list]
                menu_displays = [call for call in calls if 'DUCKDB DATA COMPARISON SYSTEM' in call]
                assert len(menu_displays) >= 2  # Initial display and after status view


@pytest.mark.skipif(not MENU_INTERFACE_AVAILABLE, reason="MenuInterface not available")
class TestMenuInterfaceKeyNormalization:
    """Test suite for MenuInterface key selection normalization."""
    
    def test_key_selection_only_offers_staged_columns(self):
        """
        Test that key selection only offers normalized column names that exist in staged tables.
        
        This is the TDD failing test for the bug where MenuInterface presents original
        column names (like 'Serial/Lot Number') but the staged table only contains
        normalized names (like 'serial_lot_number'), causing validation failures.
        """
        # Create a MenuInterface instance
        menu = MenuInterface()
        
        # Mock reviewed matches with original column names
        reviewed_matches = [
            {
                'left_column': 'Internal ID',  # Original column name
                'right_column': 'System ID',   # Original column name
                'confidence': 0.95
            },
            {
                'left_column': 'Serial/Lot Number',  # Original with special chars
                'right_column': 'Serial Number',     # Original column name  
                'confidence': 0.90
            }
        ]
        
        # Mock file paths
        left_file = Path("test_left.csv")
        right_file = Path("test_right.csv")
        
        # Mock the staging process to create tables with normalized column names
        with patch('duckdb.connect') as mock_connect:
            mock_con = Mock()
            mock_connect.return_value = mock_con
            
            # Mock the table staging to have normalized column names only
            # The staged tables should contain 'internal_id' and 'serial_lot_number' 
            # (normalized versions), NOT the original names
            mock_con.execute.return_value.fetchall.side_effect = [
                # Columns in left_table (normalized)
                [('internal_id',), ('serial_lot_number',)],
                # Columns in right_table (normalized) 
                [('system_id',), ('serial_number',)]
            ]
            
            # Mock KeyValidator and KeySelector imports
            with patch('src.core.key_validator.KeyValidator') as mock_validator_class:
                with patch('src.core.key_selector.KeySelector') as mock_selector_class:
                    
                    # Mock validator instance
                    mock_validator = Mock()
                    mock_validator_class.return_value = mock_validator
                    
                    # Mock selector instance (not used directly but imported)
                    mock_selector = Mock()
                    mock_selector_class.return_value = mock_selector
                    
                    # The critical test: validation should FAIL when trying to validate
                    # the original column name against a table with normalized names
                    
                    # First call: try to validate original name 'Serial/Lot Number' -> FAIL
                    validation_result_fail = Mock()
                    validation_result_fail.is_valid = False
                    validation_result_fail.error_message = "Column 'Serial/Lot Number' not found in table. Available columns: ['internal_id', 'serial_lot_number']"
                    
                    # Second call: validate normalized name 'serial_lot_number' -> SUCCESS  
                    validation_result_success = Mock()
                    validation_result_success.is_valid = True
                    validation_result_success.error_message = None
                    
                    # With the fix, validation should only be called once with normalized name and succeed
                    mock_validator.validate_key.return_value = validation_result_success
                    
                    # Mock the staging helper methods
                    with patch.object(menu, '_stage_sample_data_for_validation') as mock_stage:
                        with patch.object(menu, '_create_mock_dataset_config') as mock_config:
                            
                            mock_stage.side_effect = ['left_table', 'right_table']
                            mock_config.return_value = Mock(column_map=None)
                            
                            # Mock user input to select the second column (Serial/Lot Number)
                            with patch('builtins.input', return_value='2') as mock_input:
                                with patch('builtins.print') as mock_print:
                                    
                                    # THIS IS THE FAILING ASSERTION:
                                    # The current implementation will present original column names
                                    # but validation will fail because staged table has normalized names
                                    result = menu._select_and_validate_keys(
                                        left_file, right_file, reviewed_matches
                                    )
                                    
                                    # The test should fail here because the current implementation
                                    # presents 'Serial/Lot Number' to user but validates against
                                    # table containing 'serial_lot_number'
                                    
                                    # Extract all print calls to check what was presented to user
                                    print_calls = [str(call) for call in mock_print.call_args_list]
                                    
                                    # Check that the menu presents normalized column names as selectable options
                                    # (Not just showing original names in parentheses for reference)
                                    menu_options = [call for call in print_calls if '. serial_lot_number' in call]
                                    assert len(menu_options) > 0, (
                                        "Menu should present normalized column names as selectable options. "
                                        "Expected to find '. serial_lot_number' as a menu option."
                                    )
                                    
                                    # Verify that when user selects option 2, they're selecting the normalized name
                                    selection_confirmations = [call for call in print_calls 
                                                             if "Selected key column: 'serial_lot_number'" in call]
                                    assert len(selection_confirmations) > 0, (
                                        "When user selects option 2, they should be selecting the normalized name 'serial_lot_number'."
                                    )
                                    
                                    # Most importantly: Verify that validation should succeed 
                                    # because we're now validating normalized names
                                    # The validation should only be called once and should succeed
                                    mock_validator.validate_key.assert_called_once_with(
                                        table_name='left_table',
                                        key_columns=['serial_lot_number'],  # Normalized name that exists in staged table
                                        dataset_config=mock_config.return_value
                                    )
                                    
                                    # Verify the result contains the ORIGINAL column name for display purposes
                                    # but validation succeeded with normalized name
                                    assert len(result) == 1
                                    assert result[0] == 'Serial/Lot Number'  # Original name returned for display
    
    def test_key_selection_only_uses_approved_matches(self):
        """
        Test that key selection menu ONLY offers columns from reviewed_matches list.
        
        This is the TDD failing test for the critical regression where key selection
        mistakenly offers columns beyond the approved matches, causing pipeline failures.
        The menu should be strictly bounded to only the columns in reviewed_matches.
        """
        # Create a MenuInterface instance
        menu = MenuInterface()
        
        # Mock reviewed matches with LIMITED approved columns
        reviewed_matches = [
            {
                'left_column': 'Internal ID',  # APPROVED column
                'right_column': 'System ID',   
                'confidence': 0.95
            },
            {
                'left_column': 'Serial/Lot Number',  # APPROVED column
                'right_column': 'Serial Number',     
                'confidence': 0.90
            }
        ]
        
        # Mock file paths
        left_file = Path("test_left.csv")
        right_file = Path("test_right.csv")
        
        # Mock staging to simulate a table with MORE columns than just the approved matches
        # This simulates the real scenario where datasets have many columns but user only approved a few
        with patch('duckdb.connect') as mock_connect:
            mock_con = Mock()
            mock_connect.return_value = mock_con
            
            # Staged table has MORE columns than just the approved ones
            # This includes both approved columns (normalized) AND extra columns not in reviewed_matches
            mock_con.execute.return_value.fetchall.side_effect = [
                # Left table has EXTRA columns beyond approved matches
                [('internal_id',), ('serial_lot_number',), ('status',), ('date_created',), ('description',)],
                # Right table also has extras
                [('system_id',), ('serial_number',), ('active',), ('created_at',), ('notes',)]
            ]
            
            # Mock KeyValidator and KeySelector
            with patch('src.core.key_validator.KeyValidator') as mock_validator_class:
                with patch('src.core.key_selector.KeySelector') as mock_selector_class:
                    
                    # Mock validator instance
                    mock_validator = Mock()
                    mock_validator_class.return_value = mock_validator
                    
                    # Mock selector instance
                    mock_selector = Mock()
                    mock_selector_class.return_value = mock_selector
                    
                    # Mock successful validation for approved columns
                    validation_result_success = Mock()
                    validation_result_success.is_valid = True
                    validation_result_success.error_message = None
                    mock_validator.validate_key.return_value = validation_result_success
                    
                    # Mock the staging helper methods
                    with patch.object(menu, '_stage_sample_data_for_validation') as mock_stage:
                        with patch.object(menu, '_create_mock_dataset_config') as mock_config:
                            
                            mock_stage.side_effect = ['left_table', 'right_table']
                            mock_config.return_value = Mock(column_map=None)
                            
                            # Mock user input to select first approved column
                            with patch('builtins.input', return_value='1') as mock_input:
                                with patch('builtins.print') as mock_print:
                                    
                                    # Call the method under test
                                    result = menu._select_and_validate_keys(
                                        left_file, right_file, reviewed_matches
                                    )
                                    
                                    # Extract all print calls to analyze what was presented to user
                                    print_calls = [str(call) for call in mock_print.call_args_list]
                                    
                                    # CRITICAL ASSERTION: Menu should ONLY offer the 2 approved columns
                                    # NOT the extra columns (status, date_created, description, etc.)
                                    
                                    # Look for numbered menu options
                                    menu_option_calls = []
                                    for call in print_calls:
                                        # Look for calls that contain numbered options like "  1. " or "   2. "
                                        if '. ' in call and any(f'{i}. ' in call for i in range(1, 10)):
                                            menu_option_calls.append(call)
                                    
                                    # Should only have 2 menu options (for the 2 approved matches)
                                    assert len(menu_option_calls) == 2, (
                                        f"Key selection menu should ONLY offer the {len(reviewed_matches)} approved columns "
                                        f"from reviewed_matches, but found {len(menu_option_calls)} menu options: {menu_option_calls}."
                                    )
                                    
                                    # Verify the menu options are exactly the normalized versions of approved columns
                                    expected_options = ['internal_id', 'serial_lot_number']  # Normalized approved columns
                                    
                                    for expected_option in expected_options:
                                        option_found = any(expected_option in call for call in menu_option_calls)
                                        assert option_found, (
                                            f"Menu should offer approved column '{expected_option}' but it was not found "
                                            f"in menu options: {menu_option_calls}"
                                        )
                                    
                                    # Verify NO extra columns are offered
                                    extra_columns = ['status', 'date_created', 'description', 'active', 'created_at', 'notes']
                                    for extra_col in extra_columns:
                                        extra_found = any(extra_col in call for call in menu_option_calls)
                                        assert not extra_found, (
                                            f"Menu should NOT offer extra column '{extra_col}' that is not in reviewed_matches, "
                                            f"but it was found in menu options: {menu_option_calls}. "
                                            f"This violates the approved-matches-only restriction."
                                        )
                                    
                                    # Verify successful selection of approved column
                                    assert len(result) == 1
                                    assert result[0] == 'Internal ID'  # Original name of first approved match
    
    def test_key_selection_fails_with_empty_matches(self):
        """
        Test regression scenario: key selection should fail gracefully when no approved matches exist.
        
        This tests the edge case that may be causing the critical regression where
        the pipeline fails when reviewed_matches is empty or contains no valid columns.
        """
        # Create a MenuInterface instance
        menu = MenuInterface()
        
        # Mock EMPTY reviewed matches - this simulates the regression scenario
        reviewed_matches = []  # NO APPROVED MATCHES
        
        # Mock file paths
        left_file = Path("test_left.csv")
        right_file = Path("test_right.csv")
        
        # Mock staging with available columns that user never approved
        with patch('duckdb.connect') as mock_connect:
            mock_con = Mock()
            mock_connect.return_value = mock_con
            
            # Staged tables have columns, but user approved NONE of them
            mock_con.execute.return_value.fetchall.side_effect = [
                [('status',), ('date_created',), ('description',)],  # Available but not approved
                [('active',), ('created_at',), ('notes',)]
            ]
            
            # Mock KeyValidator and KeySelector
            with patch('src.core.key_validator.KeyValidator') as mock_validator_class:
                with patch('src.core.key_selector.KeySelector') as mock_selector_class:
                    
                    mock_validator = Mock()
                    mock_validator_class.return_value = mock_validator
                    mock_selector = Mock()
                    mock_selector_class.return_value = mock_selector
                    
                    # Mock the staging helper methods
                    with patch.object(menu, '_stage_sample_data_for_validation') as mock_stage:
                        with patch.object(menu, '_create_mock_dataset_config') as mock_config:
                            
                            mock_stage.side_effect = ['left_table', 'right_table']
                            mock_config.return_value = Mock(column_map=None)
                            
                            # This should fail or handle gracefully
                            with patch('builtins.input', return_value='1') as mock_input:
                                with patch('builtins.print') as mock_print:
                                    
                                    # CRITICAL TEST: What happens with empty reviewed_matches?
                                    # With the fix, this should return empty list gracefully (no infinite loop)
                                    result = menu._select_and_validate_keys(
                                        left_file, right_file, reviewed_matches
                                    )
                                    
                                    # Should return empty result when no matches approved (graceful handling)
                                    assert len(result) == 0, (
                                        f"Expected empty result when no matches approved, but got: {result}"
                                    )
                                    
                                    # Verify helpful error message was printed
                                    print_calls = [str(call) for call in mock_print.call_args_list]
                                    error_messages = [call for call in print_calls if 'No approved column matches' in call]
                                    assert len(error_messages) > 0, (
                                        f"Expected helpful error message about no approved matches, "
                                        f"but didn't find it in: {print_calls}"
                                    )
    
    def test_key_selection_handles_duplicate_normalized_names(self):
        """
        Test potential regression: key selection with columns that normalize to same name.
        
        This tests the scenario where multiple original columns normalize to the same name,
        which could cause issues in the key mapping logic.
        """
        # Create a MenuInterface instance
        menu = MenuInterface()
        
        # Mock reviewed matches with columns that normalize to SAME NAME (edge case)
        reviewed_matches = [
            {
                'left_column': 'Internal ID',  # Normalizes to 'internal_id'
                'right_column': 'System ID',   
                'confidence': 0.95
            },
            {
                'left_column': 'Internal_ID',  # ALSO normalizes to 'internal_id' - potential conflict!
                'right_column': 'ID Internal',     
                'confidence': 0.90
            }
        ]
        
        # Mock file paths
        left_file = Path("test_left.csv")
        right_file = Path("test_right.csv")
        
        with patch('duckdb.connect') as mock_connect:
            mock_con = Mock()
            mock_connect.return_value = mock_con
            
            mock_con.execute.return_value.fetchall.side_effect = [
                [('internal_id',)],  # Only one normalized column in staged table
                [('system_id',)]
            ]
            
            with patch('src.core.key_validator.KeyValidator') as mock_validator_class:
                with patch('src.core.key_selector.KeySelector') as mock_selector_class:
                    
                    mock_validator = Mock()
                    mock_validator_class.return_value = mock_validator
                    mock_selector = Mock()
                    mock_selector_class.return_value = mock_selector
                    
                    validation_result_success = Mock()
                    validation_result_success.is_valid = True
                    mock_validator.validate_key.return_value = validation_result_success
                    
                    with patch.object(menu, '_stage_sample_data_for_validation') as mock_stage:
                        with patch.object(menu, '_create_mock_dataset_config') as mock_config:
                            
                            mock_stage.side_effect = ['left_table', 'right_table']
                            mock_config.return_value = Mock(column_map=None)
                            
                            with patch('builtins.input', return_value='1') as mock_input:
                                with patch('builtins.print') as mock_print:
                                    
                                    # This might fail due to duplicate normalized names
                                    # or only show one option instead of two
                                    result = menu._select_and_validate_keys(
                                        left_file, right_file, reviewed_matches
                                    )
                                    
                                    print_calls = [str(call) for call in mock_print.call_args_list]
                                    
                                    # Look for menu options
                                    menu_option_calls = []
                                    for call in print_calls:
                                        if '. ' in call and any(f'{i}. ' in call for i in range(1, 10)):
                                            menu_option_calls.append(call)
                                    
                                    # CRITICAL: Should handle duplicate normalization gracefully
                                    # Either by showing only unique normalized names (1 option)
                                    # Or by showing both with disambiguation (2 options)
                                    assert len(menu_option_calls) > 0, (
                                        f"Menu should handle duplicate normalized names gracefully, "
                                        f"but no options were presented. Print calls: {print_calls}"
                                    )
                                    
                                    # Result should be one of the original column names
                                    assert len(result) == 1
                                    assert result[0] in ['Internal ID', 'Internal_ID']