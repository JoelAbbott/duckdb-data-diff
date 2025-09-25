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

from compare_datasets import menu_interface, MenuDrivenComparator


class TestMenuInterface:
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