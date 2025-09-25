"""
Test suite for the ConfigGenerator class.
Following TDD approach - tests written before implementation.
"""

import pytest
import yaml
import json
from pathlib import Path
from unittest.mock import Mock, patch
import sys

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))

from generate_config import ConfigGenerator


class TestConfigGenerator:
    """Test suite for ConfigGenerator functionality."""
    
    @pytest.fixture
    def sample_profiles(self):
        """Create sample profiles for testing."""
        left_profile = {
            'row_count': 1000,
            'column_count': 5,
            'columns': {
                'customer_id': {
                    'data_type': 'integer',
                    'unique_count': 1000,
                    'cardinality': 1.0,
                    'pattern': 'identifier'
                },
                'email_address': {
                    'data_type': 'string',
                    'unique_count': 950,
                    'cardinality': 0.95,
                    'pattern': 'email'
                },
                'amount': {
                    'data_type': 'float',
                    'unique_count': 500,
                    'cardinality': 0.5,
                    'pattern': 'currency'
                },
                'created_date': {
                    'data_type': 'date',
                    'unique_count': 365,
                    'cardinality': 0.365,
                    'pattern': 'date'
                },
                'is_active': {
                    'data_type': 'boolean',
                    'unique_count': 2,
                    'cardinality': 0.002
                }
            },
            'potential_keys': {
                'single_column_keys': ['customer_id']
            }
        }
        
        right_profile = {
            'row_count': 950,
            'column_count': 5,
            'columns': {
                'cust_id': {
                    'data_type': 'integer',
                    'unique_count': 950,
                    'cardinality': 1.0,
                    'pattern': 'identifier'
                },
                'email': {
                    'data_type': 'string',
                    'unique_count': 900,
                    'cardinality': 0.947,
                    'pattern': 'email'
                },
                'total_amount': {
                    'data_type': 'float',
                    'unique_count': 480,
                    'cardinality': 0.505,
                    'pattern': 'currency'
                },
                'date_created': {
                    'data_type': 'date',
                    'unique_count': 350,
                    'cardinality': 0.368,
                    'pattern': 'date'
                },
                'active': {
                    'data_type': 'boolean',
                    'unique_count': 2,
                    'cardinality': 0.002
                }
            },
            'potential_keys': {
                'single_column_keys': ['cust_id']
            }
        }
        
        return left_profile, right_profile
    
    @pytest.fixture
    def sample_match_results(self):
        """Create sample match results from SmartMatcher."""
        return {
            'matches': [
                {
                    'left_column': 'customer_id',
                    'right_column': 'cust_id',
                    'confidence': 0.95,
                    'match_reason': 'pattern_match + cardinality'
                },
                {
                    'left_column': 'email_address',
                    'right_column': 'email',
                    'confidence': 0.90,
                    'match_reason': 'pattern_match (email)'
                },
                {
                    'left_column': 'amount',
                    'right_column': 'total_amount',
                    'confidence': 0.85,
                    'match_reason': 'pattern_match (currency)'
                },
                {
                    'left_column': 'created_date',
                    'right_column': 'date_created',
                    'confidence': 0.88,
                    'match_reason': 'pattern_match (date)'
                },
                {
                    'left_column': 'is_active',
                    'right_column': 'active',
                    'confidence': 0.75,
                    'match_reason': 'fuzzy_name_match'
                }
            ],
            'unmatched_left': [],
            'unmatched_right': []
        }
    
    def test_initialization(self, sample_profiles, sample_match_results):
        """Test ConfigGenerator initialization."""
        left_profile, right_profile = sample_profiles
        generator = ConfigGenerator(
            left_profile=left_profile,
            right_profile=right_profile,
            match_results=sample_match_results,
            left_file="test_left.xlsx",
            right_file="test_right.csv"
        )
        
        assert generator.left_profile == left_profile
        assert generator.right_profile == right_profile
        assert generator.match_results == sample_match_results
        assert generator.left_file == "test_left.xlsx"
        assert generator.right_file == "test_right.csv"
    
    def test_generate_basic_config(self, sample_profiles, sample_match_results):
        """Test basic configuration generation."""
        left_profile, right_profile = sample_profiles
        generator = ConfigGenerator(
            left_profile=left_profile,
            right_profile=right_profile,
            match_results=sample_match_results,
            left_file="data/raw/test_left.xlsx",
            right_file="data/raw/test_right.csv"
        )
        
        config = generator.generate()
        
        assert 'datasets' in config
        assert len(config['datasets']) == 2
        
        # Check left dataset config
        left_config = config['datasets']['test_left']
        assert left_config['path'] == "data/raw/test_left.xlsx"
        assert 'column_map' in left_config
        assert 'dtypes' in left_config
        assert 'normalizers' in left_config
        assert 'keys' in left_config
    
    def test_column_mapping_generation(self, sample_profiles, sample_match_results):
        """Test that column mappings are correctly generated."""
        left_profile, right_profile = sample_profiles
        generator = ConfigGenerator(
            left_profile=left_profile,
            right_profile=right_profile,
            match_results=sample_match_results,
            left_file="test_left.xlsx",
            right_file="test_right.csv"
        )
        
        config = generator.generate()
        
        # Left dataset should map to normalized names
        left_map = config['datasets']['test_left']['column_map']
        assert left_map['customer_id'] == 'customer_id'
        assert left_map['email_address'] == 'email_address'
        assert left_map['amount'] == 'amount'
        
        # Right dataset should map to same normalized names as matched left columns
        right_map = config['datasets']['test_right']['column_map']
        assert right_map['cust_id'] == 'customer_id'  # Maps to left column name
        assert right_map['email'] == 'email_address'
        assert right_map['total_amount'] == 'amount'
    
    def test_data_type_assignment(self, sample_profiles, sample_match_results):
        """Test that data types are correctly assigned."""
        left_profile, right_profile = sample_profiles
        generator = ConfigGenerator(
            left_profile=left_profile,
            right_profile=right_profile,
            match_results=sample_match_results,
            left_file="test_left.xlsx",
            right_file="test_right.csv"
        )
        
        config = generator.generate()
        
        # Check data types for left dataset
        left_dtypes = config['datasets']['test_left']['dtypes']
        assert left_dtypes['customer_id'] == 'int64'
        assert left_dtypes['email_address'] == 'string'
        assert left_dtypes['amount'] == 'float64'
        assert left_dtypes['created_date'] == 'date'
        assert left_dtypes['is_active'] == 'boolean'
        
        # Check data types for right dataset
        right_dtypes = config['datasets']['test_right']['dtypes']
        assert right_dtypes['customer_id'] == 'int64'  # Uses normalized name
        assert right_dtypes['email_address'] == 'string'
        assert right_dtypes['amount'] == 'float64'
    
    def test_normalizer_assignment(self, sample_profiles, sample_match_results):
        """Test that normalizers are correctly assigned based on patterns."""
        left_profile, right_profile = sample_profiles
        generator = ConfigGenerator(
            left_profile=left_profile,
            right_profile=right_profile,
            match_results=sample_match_results,
            left_file="test_left.xlsx",
            right_file="test_right.csv"
        )
        
        config = generator.generate()
        
        # Check normalizers for left dataset
        left_norm = config['datasets']['test_left']['normalizers']
        
        # Email columns should have email normalizers
        assert 'unicode_clean' in left_norm['email_address']
        assert 'upper' in left_norm['email_address']
        
        # Currency columns should have appropriate normalizers
        assert any(n in ['unicode_clean', 'collapse_spaces'] for n in left_norm.get('amount', []))
        
        # Boolean columns should have boolean normalizer
        assert 'boolean_t_f' in left_norm['is_active']
        
        # ID columns typically don't need normalizers
        assert left_norm['customer_id'] == []
    
    def test_key_detection(self, sample_profiles, sample_match_results):
        """Test that keys are correctly identified."""
        left_profile, right_profile = sample_profiles
        generator = ConfigGenerator(
            left_profile=left_profile,
            right_profile=right_profile,
            match_results=sample_match_results,
            left_file="test_left.xlsx",
            right_file="test_right.csv"
        )
        
        config = generator.generate()
        
        # Keys should be identified from potential_keys
        left_keys = config['datasets']['test_left']['keys']
        assert 'customer_id' in left_keys
        
        right_keys = config['datasets']['test_right']['keys']
        assert 'cust_id' in right_keys  # Original column name for key
    
    def test_excel_sheet_handling(self, sample_profiles, sample_match_results):
        """Test that Excel files get sheet specification."""
        left_profile, right_profile = sample_profiles
        generator = ConfigGenerator(
            left_profile=left_profile,
            right_profile=right_profile,
            match_results=sample_match_results,
            left_file="test.xlsx",
            right_file="test.csv"
        )
        
        config = generator.generate()
        
        # Excel file should have sheet specified
        assert config['datasets']['test']['sheet'] == 'Sheet1'
        
        # CSV file should not have sheet
        assert 'sheet' not in config['datasets']['test_1']
    
    def test_unmatched_columns_handling(self):
        """Test handling of unmatched columns."""
        left_profile = {
            'columns': {
                'matched_col': {'data_type': 'string'},
                'unmatched_col': {'data_type': 'string'}
            },
            'potential_keys': {'single_column_keys': []}
        }
        
        right_profile = {
            'columns': {
                'matched': {'data_type': 'string'},
                'extra_col': {'data_type': 'integer'}
            },
            'potential_keys': {'single_column_keys': []}
        }
        
        match_results = {
            'matches': [
                {
                    'left_column': 'matched_col',
                    'right_column': 'matched',
                    'confidence': 0.9
                }
            ],
            'unmatched_left': ['unmatched_col'],
            'unmatched_right': ['extra_col']
        }
        
        generator = ConfigGenerator(
            left_profile=left_profile,
            right_profile=right_profile,
            match_results=match_results,
            left_file="left.csv",
            right_file="right.csv"
        )
        
        config = generator.generate()
        
        # Unmatched columns should still be in column_map
        left_map = config['datasets']['left']['column_map']
        assert 'unmatched_col' in left_map
        assert left_map['unmatched_col'] == 'unmatched_col'
        
        right_map = config['datasets']['right']['column_map']
        assert 'extra_col' in right_map
        assert right_map['extra_col'] == 'extra_col'
    
    def test_save_yaml_file(self, sample_profiles, sample_match_results, tmp_path):
        """Test saving configuration to YAML file."""
        left_profile, right_profile = sample_profiles
        generator = ConfigGenerator(
            left_profile=left_profile,
            right_profile=right_profile,
            match_results=sample_match_results,
            left_file="test_left.xlsx",
            right_file="test_right.csv"
        )
        
        config = generator.generate()
        
        # Save to YAML
        yaml_path = tmp_path / "test_config.yaml"
        generator.save_yaml(str(yaml_path))
        
        assert yaml_path.exists()
        
        # Load and verify
        with open(yaml_path, 'r') as f:
            loaded = yaml.safe_load(f)
        
        assert loaded == config
    
    def test_pattern_based_normalizers(self):
        """Test that normalizers are assigned based on detected patterns."""
        profiles_with_patterns = {
            'columns': {
                'email_col': {'data_type': 'string', 'pattern': 'email'},
                'phone_col': {'data_type': 'string', 'pattern': 'phone'},
                'currency_col': {'data_type': 'string', 'pattern': 'currency'},
                'date_col': {'data_type': 'date', 'pattern': 'date'},
                'id_col': {'data_type': 'string', 'pattern': 'identifier'},
                'url_col': {'data_type': 'string', 'pattern': 'url'}
            },
            'potential_keys': {'single_column_keys': ['id_col']}
        }
        
        match_results = {
            'matches': [],
            'unmatched_left': list(profiles_with_patterns['columns'].keys()),
            'unmatched_right': []
        }
        
        generator = ConfigGenerator(
            left_profile=profiles_with_patterns,
            right_profile={'columns': {}, 'potential_keys': {'single_column_keys': []}},
            match_results=match_results,
            left_file="test.csv",
            right_file="empty.csv"
        )
        
        config = generator.generate()
        normalizers = config['datasets']['test']['normalizers']
        
        # Check pattern-specific normalizers
        assert 'unicode_clean' in normalizers['email_col']
        assert 'upper' in normalizers['email_col']
        
        assert 'unicode_clean' in normalizers['phone_col'] or 'collapse_spaces' in normalizers['phone_col']
        
        assert 'unicode_clean' in normalizers['currency_col'] or 'collapse_spaces' in normalizers['currency_col']
        
        # Date columns typically don't need text normalizers
        assert normalizers['date_col'] == []
        
        # ID columns typically don't need normalizers
        assert normalizers['id_col'] == []
    
    def test_real_profiles_config_generation(self):
        """Test with actual qa2 and netsuite profiles."""
        profile_dir = Path(__file__).parent.parent / 'data' / 'raw'
        qa2_profile_path = profile_dir / 'qa2_profile.json'
        netsuite_profile_path = profile_dir / 'netsuite_profile.json'
        match_results_path = profile_dir / 'match_results.json'
        
        if not all(p.exists() for p in [qa2_profile_path, netsuite_profile_path, match_results_path]):
            pytest.skip("Profile or match result files not found")
        
        with open(qa2_profile_path, 'r') as f:
            qa2_profile = json.load(f)
        
        with open(netsuite_profile_path, 'r') as f:
            netsuite_profile = json.load(f)
        
        with open(match_results_path, 'r') as f:
            match_results = json.load(f)
        
        generator = ConfigGenerator(
            left_profile=qa2_profile,
            right_profile=netsuite_profile,
            match_results=match_results,
            left_file="data/raw/qa2_netsuite_messages.xlsx",
            right_file="data/raw/netsuite_messages (1).csv"
        )
        
        config = generator.generate()
        
        # Should generate valid configuration
        assert 'datasets' in config
        assert 'qa2_netsuite_messages' in config['datasets']
        assert 'netsuite_messages_1' in config['datasets']  # Updated to match actual output
        
        # Check that matched columns have consistent normalized names
        qa2_map = config['datasets']['qa2_netsuite_messages']['column_map']
        netsuite_map = config['datasets']['netsuite_messages_1']['column_map']
        
        # For each match, the normalized names should be the same
        for match in match_results['matches']:
            left_col = match['left_column']
            right_col = match['right_column']
            if left_col in qa2_map and right_col in netsuite_map:
                # Both should map to the same normalized name
                assert qa2_map[left_col] == netsuite_map[right_col]
    
    def test_validation_of_generated_config(self, sample_profiles, sample_match_results):
        """Test that generated config is valid for pipeline.py."""
        left_profile, right_profile = sample_profiles
        generator = ConfigGenerator(
            left_profile=left_profile,
            right_profile=right_profile,
            match_results=sample_match_results,
            left_file="test_left.xlsx",
            right_file="test_right.csv"
        )
        
        config = generator.generate()
        
        # Validate structure
        assert generator.validate_config(config)
        
        # Each dataset should have required fields
        for dataset_name, dataset_config in config['datasets'].items():
            assert 'path' in dataset_config
            assert 'column_map' in dataset_config
            assert 'dtypes' in dataset_config
            assert 'normalizers' in dataset_config
            assert 'keys' in dataset_config
            
            # All columns in column_map should have dtypes and normalizers
            for orig_col, norm_col in dataset_config['column_map'].items():
                assert norm_col in dataset_config['dtypes']
                assert norm_col in dataset_config['normalizers']
    
    def test_handle_duplicate_normalized_names(self):
        """Test handling of columns that normalize to the same name."""
        left_profile = {
            'columns': {
                'User Name': {'data_type': 'string'},
                'user_name': {'data_type': 'string'},
                'UserName': {'data_type': 'string'}
            },
            'potential_keys': {'single_column_keys': []}
        }
        
        right_profile = {
            'columns': {},
            'potential_keys': {'single_column_keys': []}
        }
        
        match_results = {
            'matches': [],
            'unmatched_left': ['User Name', 'user_name', 'UserName'],
            'unmatched_right': []
        }
        
        generator = ConfigGenerator(
            left_profile=left_profile,
            right_profile=right_profile,
            match_results=match_results,
            left_file="test.csv",
            right_file="empty.csv"
        )
        
        config = generator.generate()
        
        # Should handle duplicate normalized names
        column_map = config['datasets']['test']['column_map']
        normalized_names = list(column_map.values())
        
        # All normalized names should be unique
        assert len(normalized_names) == len(set(normalized_names))
        
        # Should have one of the original names
        assert 'User Name' in normalized_names or 'user_name' in normalized_names or 'UserName' in normalized_names
        # And variations with numbers for duplicates
        assert len([n for n in normalized_names if 'user' in n.lower() or 'User' in n]) == 3
    
    def test_composite_key_handling(self):
        """Test handling of composite keys."""
        left_profile = {
            'columns': {
                'order_id': {'data_type': 'integer'},
                'product_id': {'data_type': 'integer'}
            },
            'potential_keys': {
                'single_column_keys': [],
                'composite_key_suggestions': [['order_id', 'product_id']]
            }
        }
        
        right_profile = {
            'columns': {},
            'potential_keys': {'single_column_keys': []}
        }
        
        match_results = {
            'matches': [],
            'unmatched_left': ['order_id', 'product_id'],
            'unmatched_right': []
        }
        
        generator = ConfigGenerator(
            left_profile=left_profile,
            right_profile=right_profile,
            match_results=match_results,
            left_file="test.csv",
            right_file="empty.csv"
        )
        
        config = generator.generate()
        
        # Should use composite key
        keys = config['datasets']['test']['keys']
        assert 'order_id' in keys
        assert 'product_id' in keys
        assert len(keys) == 2
    
    def test_right_dataset_column_mapping(self):
        """Test that right dataset columns normalize to left dataset column names."""
        left_profile = {
            'columns': {
                'message_type': {'data_type': 'string'},
                'author': {'data_type': 'string'},
                'message_id': {'data_type': 'integer', 'cardinality': 1.0}
            },
            'potential_keys': {'single_column_keys': ['message_id']}
        }
        
        right_profile = {
            'columns': {
                'Type': {'data_type': 'string'},
                'From': {'data_type': 'string'},
                'Internal ID': {'data_type': 'integer', 'cardinality': 1.0}
            },
            'potential_keys': {'single_column_keys': ['Internal ID']}
        }
        
        match_results = {
            'matches': [
                {'left_column': 'message_type', 'right_column': 'Type', 'confidence': 0.95},
                {'left_column': 'author', 'right_column': 'From', 'confidence': 0.90},
                {'left_column': 'message_id', 'right_column': 'Internal ID', 'confidence': 0.98}
            ],
            'unmatched_left': [],
            'unmatched_right': []
        }
        
        generator = ConfigGenerator(
            left_profile=left_profile,
            right_profile=right_profile,
            match_results=match_results,
            left_file="left.csv",
            right_file="right.csv"
        )
        
        config = generator.generate()
        
        # Right dataset columns should map to left dataset normalized names
        left_map = config['datasets']['left']['column_map']
        right_map = config['datasets']['right']['column_map']
        
        # Check that Type maps to message_type (not stays as Type)
        assert right_map['Type'] == 'message_type'
        assert right_map['From'] == 'author'
        assert right_map['Internal ID'] == 'message_id'
        
        # Left columns should map to themselves
        assert left_map['message_type'] == 'message_type'
        assert left_map['author'] == 'author'
        assert left_map['message_id'] == 'message_id'
    
    def test_yaml_has_comparisons_section(self):
        """Test that generated YAML includes comparisons section for pipeline."""
        left_profile = {
            'columns': {
                'id': {'data_type': 'integer', 'cardinality': 1.0},
                'name': {'data_type': 'string'},
                'value': {'data_type': 'float'}
            },
            'potential_keys': {'single_column_keys': ['id']}
        }
        
        right_profile = {
            'columns': {
                'record_id': {'data_type': 'integer', 'cardinality': 1.0},
                'record_name': {'data_type': 'string'},
                'amount': {'data_type': 'float'}
            },
            'potential_keys': {'single_column_keys': ['record_id']}
        }
        
        match_results = {
            'matches': [
                {'left_column': 'id', 'right_column': 'record_id', 'confidence': 0.95},
                {'left_column': 'name', 'right_column': 'record_name', 'confidence': 0.90},
                {'left_column': 'value', 'right_column': 'amount', 'confidence': 0.85}
            ],
            'unmatched_left': [],
            'unmatched_right': []
        }
        
        generator = ConfigGenerator(
            left_profile=left_profile,
            right_profile=right_profile,
            match_results=match_results,
            left_file="dataset1.csv",
            right_file="dataset2.csv"
        )
        
        config = generator.generate()
        
        # Config must have comparisons section for pipeline to work
        assert 'comparisons' in config, "Missing comparisons section - pipeline will fail!"
        
        comparisons = config['comparisons']
        assert isinstance(comparisons, list), "Comparisons should be a list"
        assert len(comparisons) > 0, "Comparisons list should not be empty"
        
        # Check structure of comparison
        comparison = comparisons[0]
        assert 'name' in comparison
        assert 'left' in comparison
        assert 'right' in comparison
        assert 'keys' in comparison
        assert 'compare_columns' in comparison
        
        # Verify dataset names match
        assert comparison['left'] == 'dataset1'
        assert comparison['right'] == 'dataset2'
        
        # Keys should be the matched key columns
        assert comparison['keys'] == ['id']  # Using normalized name
        
        # Compare columns should include non-key matched columns
        assert 'name' in comparison['compare_columns']
        assert 'value' in comparison['compare_columns']