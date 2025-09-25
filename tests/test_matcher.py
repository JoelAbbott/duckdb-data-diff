"""
Test suite for the SmartMatcher class.
Following TDD approach - tests written before implementation.
"""

import pytest
import pandas as pd
import numpy as np
import json
from pathlib import Path
from unittest.mock import Mock, patch
import sys

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))

from smart_matcher import SmartMatcher
from profile_dataset import SmartProfiler


class TestSmartMatcher:
    """Test suite for SmartMatcher functionality."""
    
    @pytest.fixture
    def sample_left_profile(self):
        """Create a sample left dataset profile."""
        return {
            'row_count': 1000,
            'column_count': 5,
            'columns': {
                'customer_id': {
                    'data_type': 'integer',
                    'unique_count': 1000,
                    'null_percentage': 0.0,
                    'cardinality': 1.0,
                    'pattern': 'identifier'
                },
                'email_address': {
                    'data_type': 'string',
                    'unique_count': 950,
                    'null_percentage': 2.0,
                    'cardinality': 0.95,
                    'pattern': 'email'
                },
                'transaction_amount': {
                    'data_type': 'float',
                    'unique_count': 500,
                    'null_percentage': 0.0,
                    'cardinality': 0.5,
                    'statistics': {
                        'mean': 150.5,
                        'std': 50.2,
                        'min': 10.0,
                        'max': 500.0
                    }
                },
                'phone_number': {
                    'data_type': 'string',
                    'unique_count': 900,
                    'null_percentage': 5.0,
                    'cardinality': 0.9,
                    'pattern': 'phone'
                },
                'registration_date': {
                    'data_type': 'date',
                    'unique_count': 365,
                    'null_percentage': 0.0,
                    'cardinality': 0.365,
                    'pattern': 'date'
                }
            }
        }
    
    @pytest.fixture
    def sample_right_profile(self):
        """Create a sample right dataset profile with different column names."""
        return {
            'row_count': 950,
            'column_count': 6,
            'columns': {
                'cust_id': {  # Similar to customer_id
                    'data_type': 'integer',
                    'unique_count': 950,
                    'null_percentage': 0.0,
                    'cardinality': 1.0,
                    'pattern': 'identifier'
                },
                'email': {  # Similar to email_address
                    'data_type': 'string',
                    'unique_count': 900,
                    'null_percentage': 3.0,
                    'cardinality': 0.947,
                    'pattern': 'email'
                },
                'amount': {  # Similar to transaction_amount
                    'data_type': 'float',
                    'unique_count': 480,
                    'null_percentage': 0.0,
                    'cardinality': 0.505,
                    'statistics': {
                        'mean': 148.3,
                        'std': 51.1,
                        'min': 10.0,
                        'max': 495.0
                    }
                },
                'contact_phone': {  # Similar to phone_number
                    'data_type': 'string',
                    'unique_count': 850,
                    'null_percentage': 6.0,
                    'cardinality': 0.895,
                    'pattern': 'phone'
                },
                'signup_date': {  # Similar to registration_date
                    'data_type': 'date',
                    'unique_count': 350,
                    'null_percentage': 0.0,
                    'cardinality': 0.368,
                    'pattern': 'date'
                },
                'extra_field': {  # No match in left
                    'data_type': 'string',
                    'unique_count': 100,
                    'null_percentage': 10.0,
                    'cardinality': 0.105
                }
            }
        }
    
    @pytest.fixture
    def sample_left_df(self):
        """Create sample DataFrame for left dataset."""
        np.random.seed(42)
        return pd.DataFrame({
            'customer_id': range(1, 101),
            'email_address': [f'user{i}@example.com' for i in range(1, 101)],
            'transaction_amount': np.random.uniform(10, 500, 100),
            'phone_number': [f'555-{i:04d}' for i in range(1, 101)],
            'registration_date': pd.date_range('2024-01-01', periods=100, freq='D')
        })
    
    @pytest.fixture
    def sample_right_df(self):
        """Create sample DataFrame for right dataset with overlapping values."""
        np.random.seed(42)
        # Create with 80% overlap in IDs
        ids = list(range(1, 81)) + list(range(101, 121))
        return pd.DataFrame({
            'cust_id': ids,
            'email': [f'user{i}@example.com' for i in ids],
            'amount': np.random.uniform(10, 500, 100),
            'contact_phone': [f'555-{i:04d}' for i in ids],
            'signup_date': pd.date_range('2024-01-01', periods=100, freq='D'),
            'extra_field': ['extra'] * 100
        })
    
    def test_initialization_with_profiles(self, sample_left_profile, sample_right_profile):
        """Test SmartMatcher initialization with profiles."""
        matcher = SmartMatcher(sample_left_profile, sample_right_profile)
        assert matcher.left_profile == sample_left_profile
        assert matcher.right_profile == sample_right_profile
        assert matcher.matches is None
    
    def test_initialization_with_dataframes(self, sample_left_df, sample_right_df):
        """Test SmartMatcher initialization with DataFrames."""
        matcher = SmartMatcher(
            left_profile={'columns': {}},
            right_profile={'columns': {}},
            left_df=sample_left_df,
            right_df=sample_right_df
        )
        assert matcher.left_df is not None
        assert matcher.right_df is not None
        assert len(matcher.left_df) == 100
        assert len(matcher.right_df) == 100
    
    def test_exact_name_matching(self):
        """Test exact column name matching."""
        left_profile = {
            'columns': {
                'customer_id': {'data_type': 'integer'},
                'email': {'data_type': 'string'}
            }
        }
        right_profile = {
            'columns': {
                'customer_id': {'data_type': 'integer'},
                'email': {'data_type': 'string'}
            }
        }
        
        matcher = SmartMatcher(left_profile, right_profile)
        matcher.match()
        
        # Should find exact matches
        assert len(matcher.matches) == 2
        assert any(m['left_column'] == 'customer_id' and 
                  m['right_column'] == 'customer_id' and
                  m['confidence'] == 1.0 for m in matcher.matches)
    
    def test_pattern_matching(self, sample_left_profile, sample_right_profile):
        """Test matching based on patterns (email, phone, etc.)."""
        matcher = SmartMatcher(sample_left_profile, sample_right_profile)
        matcher.match()
        
        # Should match email_address to email based on pattern
        email_match = next((m for m in matcher.matches 
                           if m['left_column'] == 'email_address'), None)
        assert email_match is not None
        assert email_match['right_column'] == 'email'
        assert email_match['confidence'] > 0.6  # Pattern matching gets weighted
        assert 'pattern' in email_match['match_reason']
    
    def test_statistical_similarity_matching(self, sample_left_profile, sample_right_profile):
        """Test matching based on statistical similarity."""
        matcher = SmartMatcher(sample_left_profile, sample_right_profile)
        matcher.match()
        
        # Should match transaction_amount to amount based on statistics
        amount_match = next((m for m in matcher.matches 
                           if m['left_column'] == 'transaction_amount'), None)
        assert amount_match is not None
        assert amount_match['right_column'] == 'amount'
        assert amount_match['confidence'] > 0.7
        assert 'statistical' in amount_match['match_reason']
    
    def test_value_overlap_matching(self, sample_left_df, sample_right_df):
        """Test matching based on value overlap."""
        # Create profiles from DataFrames
        left_profiler = SmartProfiler(sample_left_df)
        left_profiler.analyze()
        
        right_profiler = SmartProfiler(sample_right_df)
        right_profiler.analyze()
        
        matcher = SmartMatcher(
            left_profiler.profile,
            right_profiler.profile,
            left_df=sample_left_df,
            right_df=sample_right_df
        )
        matcher.match()
        
        # Should find high overlap in customer_id/cust_id
        id_match = next((m for m in matcher.matches 
                        if m['left_column'] == 'customer_id'), None)
        assert id_match is not None
        assert 'value_overlap' in id_match['match_reason']
        assert id_match['sample_overlap'] is not None
    
    def test_fuzzy_name_matching(self):
        """Test fuzzy column name matching as fallback."""
        left_profile = {
            'columns': {
                'customer_email': {'data_type': 'string'},
                'transaction_amt': {'data_type': 'float'}
            }
        }
        right_profile = {
            'columns': {
                'cust_email': {'data_type': 'string'},
                'trans_amount': {'data_type': 'float'}
            }
        }
        
        matcher = SmartMatcher(left_profile, right_profile)
        matcher.match()
        
        # Should find fuzzy matches
        assert len(matcher.matches) >= 1
        email_match = next((m for m in matcher.matches 
                           if m['left_column'] == 'customer_email'), None)
        assert email_match is not None
        assert 'fuzzy' in email_match['match_reason'].lower()
    
    def test_cardinality_similarity(self, sample_left_profile, sample_right_profile):
        """Test that cardinality is considered in matching."""
        matcher = SmartMatcher(sample_left_profile, sample_right_profile)
        matcher.match()
        
        # Columns with similar cardinality should score higher
        id_match = next((m for m in matcher.matches 
                        if m['left_column'] == 'customer_id'), None)
        assert id_match is not None
        # Both have cardinality 1.0 (unique)
        assert id_match['confidence'] > 0.6  # Multiple factors affect confidence
    
    def test_unmatched_columns_detection(self, sample_left_profile, sample_right_profile):
        """Test detection of unmatched columns."""
        matcher = SmartMatcher(sample_left_profile, sample_right_profile)
        matcher.match()
        
        result = matcher.get_match_result()
        
        # extra_field in right should be unmatched
        assert 'extra_field' in result['unmatched_right']
        
        # All left columns should be matched in this case
        assert len(result['unmatched_left']) == 0
    
    def test_match_result_format(self, sample_left_profile, sample_right_profile):
        """Test the format of match results."""
        matcher = SmartMatcher(sample_left_profile, sample_right_profile)
        matcher.match()
        result = matcher.get_match_result()
        
        assert 'matches' in result
        assert 'unmatched_left' in result
        assert 'unmatched_right' in result
        assert 'summary' in result
        
        # Check match format
        for match in result['matches']:
            assert 'left_column' in match
            assert 'right_column' in match
            assert 'confidence' in match
            assert 'match_reason' in match
            assert 0 <= match['confidence'] <= 1
    
    def test_save_and_load_results(self, sample_left_profile, sample_right_profile, tmp_path):
        """Test saving and loading match results."""
        matcher = SmartMatcher(sample_left_profile, sample_right_profile)
        matcher.match()
        
        # Save results
        result_path = tmp_path / "match_results.json"
        matcher.save_results(str(result_path))
        assert result_path.exists()
        
        # Load and verify
        with open(result_path, 'r') as f:
            loaded = json.load(f)
        
        assert loaded == matcher.get_match_result()
    
    def test_confidence_thresholds(self):
        """Test that confidence scores make sense."""
        left_profile = {
            'columns': {
                'exact_match': {'data_type': 'string'},
                'very_different': {'data_type': 'string', 'cardinality': 0.1}
            }
        }
        right_profile = {
            'columns': {
                'exact_match': {'data_type': 'string'},
                'totally_unrelated': {'data_type': 'integer', 'cardinality': 0.9}
            }
        }
        
        matcher = SmartMatcher(left_profile, right_profile)
        matcher.match()
        
        # Exact match should have confidence 1.0
        exact = next((m for m in matcher.matches 
                     if m['left_column'] == 'exact_match'), None)
        assert exact['confidence'] == 1.0
        
        # Very different columns should have low confidence if matched
        poor_match = next((m for m in matcher.matches 
                          if m['left_column'] == 'very_different'), None)
        if poor_match:
            assert poor_match['confidence'] < 0.5
    
    def test_real_profiles_matching(self):
        """Test with actual qa2 and netsuite profile files."""
        profile_dir = Path(__file__).parent.parent / 'data' / 'raw'
        qa2_profile_path = profile_dir / 'qa2_profile.json'
        netsuite_profile_path = profile_dir / 'netsuite_profile.json'
        
        if not qa2_profile_path.exists() or not netsuite_profile_path.exists():
            pytest.skip("Profile files not found")
        
        with open(qa2_profile_path, 'r') as f:
            qa2_profile = json.load(f)
        
        with open(netsuite_profile_path, 'r') as f:
            netsuite_profile = json.load(f)
        
        matcher = SmartMatcher(qa2_profile, netsuite_profile)
        matcher.match()
        
        result = matcher.get_match_result()
        
        # Should find reasonable matches
        assert len(result['matches']) > 0
        
        # Check some expected matches
        # author -> From
        author_match = next((m for m in result['matches']
                           if m['left_column'] == 'author'), None)
        assert author_match is not None
        
        # author_email -> From Email Address
        email_match = next((m for m in result['matches']
                          if m['left_column'] == 'author_email'), None)
        assert email_match is not None
    
    def test_performance_large_datasets(self):
        """Test performance with many columns."""
        import time
        
        # Create profiles with many columns
        left_cols = {f'col_{i}': {
            'data_type': 'string',
            'cardinality': i/100,
            'unique_count': i*10
        } for i in range(100)}
        
        right_cols = {f'column_{i}': {
            'data_type': 'string', 
            'cardinality': i/100,
            'unique_count': i*10
        } for i in range(100)}
        
        left_profile = {'columns': left_cols}
        right_profile = {'columns': right_cols}
        
        matcher = SmartMatcher(left_profile, right_profile)
        
        start = time.time()
        matcher.match()
        elapsed = time.time() - start
        
        # Should complete in reasonable time
        assert elapsed < 5.0
        assert len(matcher.matches) > 0
    
    def test_no_false_positive_matches(self):
        """Test that very different columns aren't matched with high confidence."""
        left_profile = {
            'columns': {
                'user_age': {
                    'data_type': 'integer',
                    'statistics': {'mean': 35, 'min': 18, 'max': 65}
                },
                'product_price': {
                    'data_type': 'float',
                    'statistics': {'mean': 150.0, 'min': 10.0, 'max': 500.0}
                }
            }
        }
        right_profile = {
            'columns': {
                'order_quantity': {
                    'data_type': 'integer',
                    'statistics': {'mean': 3, 'min': 1, 'max': 10}
                },
                'shipping_cost': {
                    'data_type': 'float',
                    'statistics': {'mean': 15.0, 'min': 5.0, 'max': 50.0}
                }
            }
        }
        
        matcher = SmartMatcher(left_profile, right_profile)
        matcher.match()
        
        # Very different statistics shouldn't match with high confidence
        for match in matcher.matches:
            assert match['confidence'] < 0.6  # Low confidence for poor matches
    
    def test_match_stability(self, sample_left_profile, sample_right_profile):
        """Test that matching is deterministic."""
        matcher1 = SmartMatcher(sample_left_profile, sample_right_profile)
        matcher1.match()
        result1 = matcher1.get_match_result()
        
        matcher2 = SmartMatcher(sample_left_profile, sample_right_profile)
        matcher2.match()
        result2 = matcher2.get_match_result()
        
        # Results should be identical
        assert result1['matches'] == result2['matches']
        assert result1['unmatched_left'] == result2['unmatched_left']
        assert result1['unmatched_right'] == result2['unmatched_right']
    
    def test_empty_profiles_handling(self):
        """Test handling of empty profiles."""
        empty_left = {'columns': {}}
        empty_right = {'columns': {}}
        
        matcher = SmartMatcher(empty_left, empty_right)
        matcher.match()
        
        result = matcher.get_match_result()
        assert len(result['matches']) == 0
        assert len(result['unmatched_left']) == 0
        assert len(result['unmatched_right']) == 0
    
    def test_one_sided_matching(self):
        """Test when one dataset has many more columns."""
        left_profile = {
            'columns': {
                'id': {'data_type': 'integer'},
                'name': {'data_type': 'string'}
            }
        }
        right_profile = {
            'columns': {
                'id': {'data_type': 'integer'},
                'name': {'data_type': 'string'},
                'address': {'data_type': 'string'},
                'phone': {'data_type': 'string'},
                'email': {'data_type': 'string'}
            }
        }
        
        matcher = SmartMatcher(left_profile, right_profile)
        matcher.match()
        
        result = matcher.get_match_result()
        
        # All left columns should be matched
        assert len(result['matches']) == 2
        assert len(result['unmatched_left']) == 0
        
        # Some right columns should be unmatched
        assert len(result['unmatched_right']) == 3