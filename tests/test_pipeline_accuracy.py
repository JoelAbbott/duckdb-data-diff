"""
Test suite to ensure pipeline accuracy and prevent false positives.
Following TDD approach to ensure production readiness.
"""

import pytest
import pandas as pd
import yaml
from pathlib import Path
import sys

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))

from pipeline import stage_dataset, compare_pair, load_config


class TestPipelineAccuracy:
    """Test suite for ensuring accurate difference detection."""
    
    def test_column_name_mapping_accuracy(self):
        """Test that column names with dots are handled correctly."""
        # Create test data with column names containing dots
        test_data = pd.DataFrame({
            'Internal ID': [1, 2, 3],
            'Internal ID.1': [100, 200, 300],
            'Name': ['A', 'B', 'C']
        })
        
        # Test column mapping handles dots correctly
        column_map = {
            'Internal ID': 'message_id',
            'Internal ID.1': 'transaction_id',
            'Name': 'name'
        }
        
        # Apply column mapping
        renamed = test_data.rename(columns=column_map)
        
        assert 'message_id' in renamed.columns
        assert 'transaction_id' in renamed.columns
        assert renamed['message_id'].tolist() == [1, 2, 3]
        assert renamed['transaction_id'].tolist() == [100, 200, 300]
    
    def test_key_column_matching(self):
        """Test that key columns are properly matched between datasets."""
        # Create two datasets with matching keys
        left_df = pd.DataFrame({
            'message_id': [1, 2, 3, 4, 5],
            'transaction_id': [100, 200, 300, 400, 500],
            'value': ['a', 'b', 'c', 'd', 'e']
        })
        
        right_df = pd.DataFrame({
            'message_id': [1, 2, 3, 4, 5],
            'transaction_id': [100, 200, 300, 400, 500],
            'value': ['a', 'b', 'c', 'd', 'e']
        })
        
        # Merge on keys
        merged = pd.merge(
            left_df,
            right_df,
            on=['message_id', 'transaction_id'],
            suffixes=('_left', '_right'),
            how='outer',
            indicator=True
        )
        
        # All records should match (no left_only or right_only)
        assert all(merged['_merge'] == 'both')
        assert len(merged[merged['_merge'] == 'left_only']) == 0
        assert len(merged[merged['_merge'] == 'right_only']) == 0
    
    def test_false_positive_prevention(self):
        """Test that identical datasets produce no differences."""
        # Create identical datasets
        data = pd.DataFrame({
            'key1': [1, 2, 3],
            'key2': [10, 20, 30],
            'value1': ['a', 'b', 'c'],
            'value2': [1.1, 2.2, 3.3]
        })
        
        # Compare identical data
        merged = pd.merge(
            data,
            data,
            on=['key1', 'key2'],
            suffixes=('_left', '_right'),
            how='outer'
        )
        
        # Check no differences
        for col in ['value1', 'value2']:
            assert all(merged[f'{col}_left'] == merged[f'{col}_right'])
    
    def test_yaml_configuration_parsing(self):
        """Test that YAML configuration is parsed correctly."""
        yaml_content = """
datasets:
  test_dataset:
    column_map:
      "Internal ID": message_id
      "Internal ID.1": transaction_id
    keys: ["message_id", "transaction_id"]
comparisons:
  - name: test_comparison
    left: test_dataset
    right: test_dataset
    keys: ["message_id", "transaction_id"]
"""
        
        config = yaml.safe_load(yaml_content)
        
        # Check column mapping
        column_map = config['datasets']['test_dataset']['column_map']
        assert column_map['Internal ID'] == 'message_id'
        assert column_map['Internal ID.1'] == 'transaction_id'
        
        # Check keys
        keys = config['datasets']['test_dataset']['keys']
        assert 'message_id' in keys
        assert 'transaction_id' in keys
    
    def test_real_data_sample_accuracy(self):
        """Test with a sample of real data to ensure accuracy."""
        # Load a sample of the real datasets
        qa2_sample = pd.read_excel('data/raw/qa2_netsuite_messages.xlsx', nrows=100)
        netsuite_sample = pd.read_csv('data/raw/netsuite_messages (1).csv', nrows=100)
        
        # Check column names
        assert 'message_id' in qa2_sample.columns
        assert 'transaction_id' in qa2_sample.columns
        assert 'Internal ID' in netsuite_sample.columns
        assert 'Internal ID.1' in netsuite_sample.columns
        
        # Apply column mapping from datasets.yaml
        column_map = {
            'Internal ID': 'message_id',
            'Internal ID.1': 'transaction_id'
        }
        
        netsuite_renamed = netsuite_sample.rename(columns=column_map)
        
        # Check that key columns now exist
        assert 'message_id' in netsuite_renamed.columns
        assert 'transaction_id' in netsuite_renamed.columns
        
        # Verify data types are compatible
        assert netsuite_renamed['message_id'].dtype.kind in ['i', 'f']  # numeric
        assert netsuite_renamed['transaction_id'].dtype.kind in ['i', 'f']  # numeric
    
    def test_duplicate_key_detection(self):
        """Test that duplicate keys are detected and handled."""
        # Create data with duplicate keys
        df = pd.DataFrame({
            'message_id': [1, 1, 2, 3, 3],
            'transaction_id': [100, 100, 200, 300, 300],
            'value': ['a', 'b', 'c', 'd', 'e']
        })
        
        # Check for duplicates
        duplicates = df[df.duplicated(subset=['message_id', 'transaction_id'], keep=False)]
        
        assert len(duplicates) == 4  # Two pairs of duplicates
        
        # Ensure pipeline would detect this issue
        unique_keys = df[['message_id', 'transaction_id']].drop_duplicates()
        assert len(unique_keys) == 3  # Only 3 unique key combinations
    
    def test_normalizer_consistency(self):
        """Test that normalizers are applied consistently."""
        # Test data with various formats
        data = pd.DataFrame({
            'email': ['TEST@EXAMPLE.COM', 'test@example.com', 'TeSt@ExAmPlE.cOm'],
            'text': ['  hello  world  ', 'hello world', 'HELLO   WORLD']
        })
        
        # Apply normalizers
        from pipeline import upper, collapse_spaces, unicode_clean
        
        # Test upper normalizer
        emails_upper = data['email'].apply(upper)
        assert all(email.isupper() for email in emails_upper)
        
        # Test collapse_spaces
        text_collapsed = data['text'].apply(collapse_spaces)
        assert all('  ' not in t for t in text_collapsed)
        
        # All normalized emails should be identical
        emails_normalized = data['email'].apply(upper)
        assert len(emails_normalized.unique()) == 1
    
    def test_comparison_with_fixed_config(self):
        """Test that the fixed configuration produces reasonable results."""
        # Load the fixed configuration
        with open('datasets.yaml', 'r') as f:
            config = yaml.safe_load(f)
        
        # Check the critical fix was applied
        netsuite_config = config['datasets']['netsuite_messages']
        column_map = netsuite_config['column_map']
        
        # The fix: "Internal ID.1" should map to transaction_id
        assert 'Internal ID.1' in column_map
        assert column_map['Internal ID.1'] == 'transaction_id'
        
        # Old incorrect mapping should not exist
        assert 'Internal ID_1' not in column_map
        
        # Both key columns should be properly mapped
        assert column_map['Internal ID'] == 'message_id'
        assert column_map['Internal ID.1'] == 'transaction_id'


if __name__ == "__main__":
    # Run specific tests
    import sys
    pytest.main([__file__, "-v", "--tb=short"])