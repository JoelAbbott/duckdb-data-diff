"""
Test suite for the SmartProfiler class.
Following TDD approach - tests written before implementation.
"""

import pytest
import pandas as pd
import numpy as np
import json
import os
from pathlib import Path
from unittest.mock import Mock, patch
import sys

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))

from profile_dataset import SmartProfiler


class TestSmartProfiler:
    """Test suite for SmartProfiler functionality."""
    
    @pytest.fixture
    def sample_dataframe(self):
        """Create a sample DataFrame for testing."""
        return pd.DataFrame({
            'id': range(1, 101),
            'name': ['John', 'Jane', 'Bob', 'Alice', 'Charlie'] * 20,
            'email': [f'user{i}@example.com' for i in range(1, 101)],
            'phone': ['555-0100', '555-0101', None, '555-0102', 'invalid'] * 20,
            'amount': np.random.uniform(10.50, 1000.75, 100),
            'date': pd.date_range('2024-01-01', periods=100, freq='D'),
            'status': ['active', 'inactive', 'pending', None, 'active'] * 20,
            'currency': ['$100.50', '$200.75', '€150.25', None, '¥5000'] * 20,
            'null_column': [None] * 100,
            'mixed_types': [1, '2', 3.0, None, 'five'] * 20
        })
    
    @pytest.fixture
    def test_excel_path(self):
        """Path to test Excel file."""
        return Path(__file__).parent.parent / 'data' / 'raw' / 'qa2_netsuite_messages.xlsx'
    
    @pytest.fixture
    def test_csv_path(self):
        """Path to test CSV file."""
        return Path(__file__).parent.parent / 'data' / 'raw' / 'netsuite_messages (1).csv'
    
    def test_initialization_with_file_path(self, test_excel_path):
        """Test that SmartProfiler can be initialized with a file path."""
        profiler = SmartProfiler(str(test_excel_path))
        assert profiler.file_path == str(test_excel_path)
        assert profiler.df is not None
        assert profiler.profile is None  # Not analyzed yet
    
    def test_initialization_with_dataframe(self, sample_dataframe):
        """Test that SmartProfiler can be initialized with a DataFrame."""
        profiler = SmartProfiler(sample_dataframe)
        assert profiler.file_path is None
        assert profiler.df is not None
        assert len(profiler.df) == 100
        assert profiler.profile is None
    
    def test_load_excel_file(self, test_excel_path):
        """Test loading an Excel file."""
        if test_excel_path.exists():
            profiler = SmartProfiler(str(test_excel_path))
            assert isinstance(profiler.df, pd.DataFrame)
            assert len(profiler.df) > 0
            assert len(profiler.df.columns) > 0
    
    def test_load_csv_file(self, test_csv_path):
        """Test loading a CSV file."""
        if test_csv_path.exists():
            profiler = SmartProfiler(str(test_csv_path))
            assert isinstance(profiler.df, pd.DataFrame)
            assert len(profiler.df) > 0
            assert len(profiler.df.columns) > 0
    
    def test_analyze_creates_profile(self, sample_dataframe):
        """Test that analyze() creates a comprehensive profile."""
        profiler = SmartProfiler(sample_dataframe)
        profiler.analyze()
        
        assert profiler.profile is not None
        assert 'columns' in profiler.profile
        assert 'row_count' in profiler.profile
        assert 'column_count' in profiler.profile
        assert 'potential_keys' in profiler.profile
        assert 'data_quality_issues' in profiler.profile
    
    def test_column_profiling(self, sample_dataframe):
        """Test detailed column profiling."""
        profiler = SmartProfiler(sample_dataframe)
        profiler.analyze()
        
        columns = profiler.profile['columns']
        
        # Test ID column profile
        id_profile = columns['id']
        assert id_profile['data_type'] == 'integer'
        assert id_profile['unique_count'] == 100
        assert id_profile['null_percentage'] == 0.0
        assert id_profile['cardinality'] == 1.0  # 100% unique
        assert 'min' in id_profile['statistics']
        assert 'max' in id_profile['statistics']
        
        # Test email column profile
        email_profile = columns['email']
        assert email_profile['data_type'] == 'string'
        assert email_profile['pattern'] == 'email'
        assert email_profile['unique_count'] == 100
        
        # Test null column profile
        null_profile = columns['null_column']
        assert null_profile['null_percentage'] == 100.0
        assert null_profile['data_type'] == 'null'
    
    def test_pattern_detection(self, sample_dataframe):
        """Test pattern detection for various data types."""
        profiler = SmartProfiler(sample_dataframe)
        profiler.analyze()
        
        columns = profiler.profile['columns']
        
        assert columns['email']['pattern'] == 'email'
        assert columns['phone']['pattern'] == 'phone'
        assert columns['currency']['pattern'] == 'currency'
        assert columns['date']['pattern'] == 'date'
    
    def test_potential_key_detection(self, sample_dataframe):
        """Test detection of potential key columns."""
        profiler = SmartProfiler(sample_dataframe)
        profiler.analyze()
        
        potential_keys = profiler.profile['potential_keys']
        
        # ID should be identified as a potential key (100% unique)
        assert 'id' in potential_keys['single_column_keys']
        
        # Email should also be a potential key
        assert 'email' in potential_keys['single_column_keys']
    
    def test_data_quality_issues_detection(self, sample_dataframe):
        """Test detection of data quality issues."""
        profiler = SmartProfiler(sample_dataframe)
        profiler.analyze()
        
        issues = profiler.profile['data_quality_issues']
        
        # Should detect null column
        assert any('null_column' in str(issue) for issue in issues)
        
        # Should detect mixed types column
        assert any('mixed_types' in str(issue) for issue in issues)
        
        # Should detect missing values in phone and status
        assert any('missing' in str(issue).lower() or 'null' in str(issue).lower() 
                  for issue in issues)
    
    def test_statistical_analysis_numeric_columns(self, sample_dataframe):
        """Test statistical analysis for numeric columns."""
        profiler = SmartProfiler(sample_dataframe)
        profiler.analyze()
        
        amount_stats = profiler.profile['columns']['amount']['statistics']
        
        assert 'mean' in amount_stats
        assert 'std' in amount_stats
        assert 'min' in amount_stats
        assert 'max' in amount_stats
        assert 'percentile_25' in amount_stats
        assert 'percentile_50' in amount_stats
        assert 'percentile_75' in amount_stats
    
    def test_value_frequency_analysis(self, sample_dataframe):
        """Test most frequent values analysis."""
        profiler = SmartProfiler(sample_dataframe)
        profiler.analyze()
        
        name_profile = profiler.profile['columns']['name']
        
        assert 'most_frequent_values' in name_profile
        assert len(name_profile['most_frequent_values']) <= 10
        
        # Each entry should have value and count
        for entry in name_profile['most_frequent_values']:
            assert 'value' in entry
            assert 'count' in entry
            assert 'percentage' in entry
    
    def test_save_report_json(self, sample_dataframe, tmp_path):
        """Test saving profile report as JSON."""
        profiler = SmartProfiler(sample_dataframe)
        profiler.analyze()
        
        report_path = tmp_path / "test_profile.json"
        profiler.save_report(str(report_path))
        
        assert report_path.exists()
        
        # Load and verify the saved report
        with open(report_path, 'r') as f:
            loaded_profile = json.load(f)
        
        assert loaded_profile == profiler.profile
    
    def test_encoding_detection(self):
        """Test detection of encoding issues."""
        # Create DataFrame with potential encoding issues
        df = pd.DataFrame({
            'text': ['normal', 'cafÃ©', 'ä¸­æ–‡', None, 'normal text']
        })
        
        profiler = SmartProfiler(df)
        profiler.analyze()
        
        issues = profiler.profile['data_quality_issues']
        # Should detect potential encoding issues
        assert any('encoding' in str(issue).lower() for issue in issues)
    
    def test_outlier_detection(self):
        """Test detection of outliers in numeric columns."""
        # Create DataFrame with outliers
        values = [10, 12, 11, 13, 12, 11, 10, 12, 1000, 11]  # 1000 is an outlier
        df = pd.DataFrame({'values': values})
        
        profiler = SmartProfiler(df)
        profiler.analyze()
        
        column_profile = profiler.profile['columns']['values']
        assert 'outliers' in column_profile
        assert len(column_profile['outliers']) > 0
    
    def test_composite_key_suggestion(self):
        """Test suggestion of composite keys."""
        # Create DataFrame where combination of columns forms a unique key
        df = pd.DataFrame({
            'category': ['A', 'A', 'B', 'B', 'C'] * 20,
            'subcategory': list(range(20)) * 5,
            'value': np.random.randn(100)
        })
        
        profiler = SmartProfiler(df)
        profiler.analyze()
        
        potential_keys = profiler.profile['potential_keys']
        assert 'composite_key_suggestions' in potential_keys
    
    def test_real_excel_file_profiling(self, test_excel_path):
        """Test profiling the actual qa2_netsuite_messages.xlsx file."""
        if not test_excel_path.exists():
            pytest.skip(f"Test file not found: {test_excel_path}")
        
        profiler = SmartProfiler(str(test_excel_path))
        profiler.analyze()
        
        # Basic assertions about the profile
        assert profiler.profile is not None
        assert profiler.profile['row_count'] > 0
        assert profiler.profile['column_count'] > 0
        assert len(profiler.profile['columns']) == profiler.profile['column_count']
        
        # Save the report
        report_path = test_excel_path.parent / 'qa2_profile_test.json'
        profiler.save_report(str(report_path))
        assert report_path.exists()
        
        # Clean up
        if report_path.exists():
            os.remove(report_path)
    
    def test_real_csv_file_profiling(self, test_csv_path):
        """Test profiling the actual netsuite_messages.csv file."""
        if not test_csv_path.exists():
            pytest.skip(f"Test file not found: {test_csv_path}")
        
        profiler = SmartProfiler(str(test_csv_path))
        profiler.analyze()
        
        # Basic assertions about the profile
        assert profiler.profile is not None
        assert profiler.profile['row_count'] > 0
        assert profiler.profile['column_count'] > 0
        assert len(profiler.profile['columns']) == profiler.profile['column_count']
        
        # Save the report
        report_path = test_csv_path.parent / 'netsuite_profile_test.json'
        profiler.save_report(str(report_path))
        assert report_path.exists()
        
        # Clean up
        if report_path.exists():
            os.remove(report_path)
    
    def test_deterministic_profiling(self, sample_dataframe):
        """Test that profiling is deterministic - same input produces same output."""
        profiler1 = SmartProfiler(sample_dataframe.copy())
        profiler1.analyze()
        
        profiler2 = SmartProfiler(sample_dataframe.copy())
        profiler2.analyze()
        
        # Remove any timestamp fields that might differ
        for profile in [profiler1.profile, profiler2.profile]:
            if 'timestamp' in profile:
                del profile['timestamp']
        
        assert profiler1.profile == profiler2.profile
    
    def test_memory_efficiency(self, sample_dataframe):
        """Test that profiler doesn't consume excessive memory."""
        import tracemalloc
        
        tracemalloc.start()
        
        profiler = SmartProfiler(sample_dataframe)
        profiler.analyze()
        
        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        
        # Profile should use less than 10MB for this small dataset
        assert peak / 1024 / 1024 < 10
    
    def test_performance(self, sample_dataframe):
        """Test that profiling completes in reasonable time."""
        import time
        
        # Create a larger DataFrame
        large_df = pd.concat([sample_dataframe] * 100, ignore_index=True)
        
        profiler = SmartProfiler(large_df)
        
        start_time = time.time()
        profiler.analyze()
        elapsed = time.time() - start_time
        
        # Should complete in less than 5 seconds for 10k rows
        assert elapsed < 5
    
    def test_error_handling_invalid_file(self):
        """Test error handling for invalid file paths."""
        with pytest.raises(FileNotFoundError):
            SmartProfiler("nonexistent_file.csv")
    
    def test_error_handling_invalid_input_type(self):
        """Test error handling for invalid input types."""
        with pytest.raises(TypeError):
            SmartProfiler(123)  # Invalid type
    
    def test_empty_dataframe_handling(self):
        """Test handling of empty DataFrames."""
        empty_df = pd.DataFrame()
        profiler = SmartProfiler(empty_df)
        profiler.analyze()
        
        assert profiler.profile['row_count'] == 0
        assert profiler.profile['column_count'] == 0
        assert len(profiler.profile['columns']) == 0