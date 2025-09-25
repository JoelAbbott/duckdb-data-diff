"""
Critical bug fix tests following TDD methodology.
Tests for the 4 critical bugs identified in CLAUDE.md
"""

import pytest
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import tempfile
import shutil
import sys

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from ui.menu import MenuInterface


class TestBug1FileSelectionIndex:
    """Test for BUG 1: File selection index error (off-by-one)"""
    
    def setup_method(self):
        """Setup test environment with multiple files."""
        self.test_dir = Path(tempfile.mkdtemp())
        
        # Create 7 test files to specifically test positions 5, 6, 7
        test_files = [
            "file1.csv", "file2.csv", "file3.csv", "file4.csv", 
            "file5.csv", "file6.csv", "file7.csv"
        ]
        
        for filename in test_files:
            test_file = self.test_dir / filename
            test_file.write_text("col1,col2\nval1,val2\n")
        
        self.menu = MenuInterface(self.test_dir)
        
    def teardown_method(self):
        """Clean up test files."""
        shutil.rmtree(self.test_dir, ignore_errors=True)
    
    def test_file_selection_index_accuracy(self):
        """Test that selecting file index 6 returns actual file at position 6."""
        # Get available files (should be sorted)
        available_files = self.menu.available_files
        assert len(available_files) == 7, f"Expected 7 files, got {len(available_files)}"
        
        # Expected file at position 6 (1-indexed) = index 5 (0-indexed) 
        expected_file_at_position_6 = available_files[5]  # file6.csv
        
        # Simulate user selecting position 6
        # First exclude file1.csv to test the exclude functionality
        exclude_file = available_files[0]  # file1.csv
        
        # Mock user input to select position 6
        with patch('builtins.input', return_value='6'):
            selected_file = self.menu._select_single_file("test", exclude=exclude_file)
        
        # This should fail initially due to the off-by-one bug
        # The bug causes position 6 to return position 7 file
        # Available files after exclude: [file2, file3, file4, file5, file6, file7]
        # User selects 6, should get file7.csv (index 5 in filtered list)
        # But bug makes it return wrong file
        expected_available_after_exclude = [f for f in available_files if f != exclude_file]
        expected_file_for_selection_6 = expected_available_after_exclude[5]  # Should be file7.csv
        
        assert selected_file == expected_file_for_selection_6, \
            f"BUG 1: User selected position 6, expected {expected_file_for_selection_6.name}, got {selected_file.name if selected_file else None}"


class TestBug2ColumnDetection:
    """Test for BUG 2: Missing column detection (16 vs 15 columns)"""
    
    def test_all_columns_detected_in_sparse_files(self):
        """Test profiling finds all 16 columns in netsuite_messages (1).csv"""
        # Create a test file with sparse columns (empty in first 1000 rows)
        test_dir = Path(tempfile.mkdtemp())
        sparse_file = test_dir / "sparse_test.csv"
        
        try:
            # Create CSV with 16 columns, where one column is empty for first 1000 rows
            header = "col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12,col13,col14,col15,sparse_col"
            
            # Create 1200 rows where sparse_col is empty for first 1000 rows
            rows = [header]
            
            # First 1000 rows with empty sparse_col
            for i in range(1000):
                rows.append(f"val{i},val{i},val{i},val{i},val{i},val{i},val{i},val{i},val{i},val{i},val{i},val{i},val{i},val{i},val{i},")
            
            # Next 200 rows with sparse_col having values
            for i in range(1000, 1200):
                rows.append(f"val{i},val{i},val{i},val{i},val{i},val{i},val{i},val{i},val{i},val{i},val{i},val{i},val{i},val{i},val{i},sparse_value{i}")
            
            sparse_file.write_text("\n".join(rows))
            
            menu = MenuInterface(test_dir)
            profile = menu._profile_dataset(sparse_file)
            
            # This should fail initially - sparse_col won't be detected due to nrows=1000 limit
            detected_columns = len(profile.get('columns', {}))
            expected_columns = 16
            
            assert detected_columns == expected_columns, \
                f"BUG 2: Expected {expected_columns} columns in sparse file, but detected {detected_columns}"
            
            # Specific check for the sparse column
            columns = profile.get('columns', {})
            assert 'sparse_col' in columns, \
                f"BUG 2: Sparse column 'sparse_col' not detected in profile. Detected columns: {list(columns.keys())}"
                
        finally:
            shutil.rmtree(test_dir, ignore_errors=True)


class TestBug3ColumnMappingPersistence:
    """Test for BUG 3: Column name mismatch in comparison SQL"""
    
    def test_interactive_column_mapping_persistence(self):
        """Test that approved mappings are used in comparison SQL"""
        # Create test files
        test_dir = Path(tempfile.mkdtemp())
        
        left_file = test_dir / "left.csv"
        left_file.write_text("From,Subject,Date\nuser1,test1,2023-01-01\n")
        
        right_file = test_dir / "right.csv" 
        right_file.write_text("author,title,timestamp\nuser1,test1,2023-01-01\n")
        
        try:
            menu = MenuInterface(test_dir)
            
            # Create approved mappings (simulating interactive session)
            approved_matches = [
                {
                    'left_column': 'From',
                    'right_column': 'author', 
                    'confidence': 1.0,
                    'match_reason': 'manual_selection'
                },
                {
                    'left_column': 'Subject',
                    'right_column': 'title',
                    'confidence': 1.0, 
                    'match_reason': 'manual_selection'
                }
            ]
            
            # Generate config with mappings
            config = menu._create_interactive_config(left_file, right_file, approved_matches)
            
            # This should fail initially due to BUG 3:
            # The config should contain proper column mappings but doesn't
            datasets = config.get('datasets', {})
            right_dataset = list(datasets.values())[1]  # Second dataset
            
            # Check column mapping exists
            assert 'column_map' in right_dataset, \
                "BUG 3: column_map missing from right dataset config"
            
            column_map = right_dataset['column_map']
            assert 'author' in column_map, \
                "BUG 3: 'author' column mapping missing"
            assert column_map['author'] == 'From', \
                f"BUG 3: Expected 'author' -> 'From' mapping, got {column_map.get('author')}"
            
            # Check key columns are properly set with mapped names
            comparisons = config.get('comparisons', [])
            assert len(comparisons) > 0, "BUG 3: No comparisons found in config"
            
            comparison = comparisons[0]
            keys = comparison.get('keys', [])
            assert len(keys) > 0, "BUG 3: No key columns set in comparison"
            
            # The key should be the LEFT column name, not the right
            assert keys[0] == 'From', \
                f"BUG 3: Expected key column 'From', got '{keys[0]}'"
                
        finally:
            shutil.rmtree(test_dir, ignore_errors=True)


class TestBug4KeyValidation:
    """Test for BUG 4: Key column validation error with mapped names"""
    
    def test_key_validation_with_mapped_columns(self):
        """Test validation uses mapped column names, not original names"""
        # This test would require access to the comparator/validator
        # For now, create a mock test that demonstrates the expected behavior
        
        test_dir = Path(tempfile.mkdtemp())
        
        left_file = test_dir / "messages.csv"
        left_file.write_text("From,Subject\nuser1,test1\n")
        
        right_file = test_dir / "msgs.csv"
        right_file.write_text("author,title\nuser1,test1\n")
        
        try:
            menu = MenuInterface(test_dir)
            
            # Create config with column mappings
            approved_matches = [
                {
                    'left_column': 'From',
                    'right_column': 'author',
                    'confidence': 1.0,
                    'match_reason': 'manual_selection'
                }
            ]
            
            config = menu._create_interactive_config(left_file, right_file, approved_matches)
            
            # Mock validation process
            datasets = config.get('datasets', {})
            left_name = list(datasets.keys())[0]
            right_name = list(datasets.keys())[1]
            
            left_dataset = datasets[left_name]
            right_dataset = datasets[right_name]
            
            # Validate that key columns exist in their respective datasets
            left_key_columns = left_dataset.get('key_columns', [])
            right_key_columns = right_dataset.get('key_columns', [])
            
            # This should fail initially due to BUG 4:
            # Validation looks for wrong column names after mapping
            assert len(left_key_columns) > 0, "BUG 4: No key columns set for left dataset"
            assert len(right_key_columns) > 0, "BUG 4: No key columns set for right dataset"
            
            # Left should have 'From', right should have 'author'
            assert 'From' in left_key_columns, \
                f"BUG 4: Expected 'From' in left key columns, got {left_key_columns}"
            assert 'author' in right_key_columns, \
                f"BUG 4: Expected 'author' in right key columns, got {right_key_columns}"
            
            # The validator should not look for 'From' in the right dataset
            # This is where the actual bug occurs - needs to be tested in the comparator
            
        finally:
            shutil.rmtree(test_dir, ignore_errors=True)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])