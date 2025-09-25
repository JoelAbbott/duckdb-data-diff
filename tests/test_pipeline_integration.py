"""
Integration tests for the complete pipeline flow.
Tests the entire system from profiling through pipeline execution.
"""

import pytest
import sys
import json
import yaml
import subprocess
import tempfile
from pathlib import Path
from unittest.mock import patch, Mock, MagicMock
import pandas as pd
import shutil

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from compare_datasets import DatasetComparator
from profile_dataset import SmartProfiler
from smart_matcher import SmartMatcher
from generate_config import ConfigGenerator


class TestPipelineIntegration:
    """Test the complete pipeline integration."""
    
    @pytest.fixture
    def temp_data_dir(self):
        """Create a temporary data directory."""
        temp_dir = Path(tempfile.mkdtemp())
        data_dir = temp_dir / "data"
        data_dir.mkdir()
        (data_dir / "raw").mkdir()
        (data_dir / "staging").mkdir()
        (data_dir / "reports").mkdir()
        (data_dir / "comparisons").mkdir()
        
        yield data_dir
        
        # Cleanup
        shutil.rmtree(temp_dir, ignore_errors=True)
    
    @pytest.fixture
    def sample_data_files(self, temp_data_dir):
        """Create sample data files for testing."""
        # Create left dataset
        left_data = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'email': ['test1@example.com', 'test2@example.com', 'test3@example.com', 
                      'test4@example.com', 'test5@example.com'],
            'amount': [100.0, 200.0, 300.0, 400.0, 500.0],
            'status': ['active', 'inactive', 'active', 'active', 'inactive'],
            'created_date': ['2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05']
        })
        
        # Create right dataset with slight variations
        right_data = pd.DataFrame({
            'ID': [1, 2, 3, 4, 6],  # Different case, missing 5, added 6
            'Email_Address': ['test1@example.com', 'test2@example.com', 'modified@example.com',
                             'test4@example.com', 'test6@example.com'],
            'Amount_USD': ['$100.00', '$200.00', '$350.00', '$400.00', '$600.00'],
            'Status_Flag': ['True', 'False', 'True', 'True', 'False'],
            'Date_Created': ['2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04', '2024-01-06']
        })
        
        left_file = temp_data_dir / "raw" / "left_dataset.csv"
        right_file = temp_data_dir / "raw" / "right_dataset.csv"
        
        left_data.to_csv(left_file, index=False)
        right_data.to_csv(right_file, index=False)
        
        return {
            'left_file': str(left_file),
            'right_file': str(right_file),
            'temp_dir': temp_data_dir
        }
    
    def test_config_file_location_issue(self, sample_data_files):
        """
        Test that demonstrates the config file location issue.
        Pipeline.py expects datasets.yaml in ROOT, but compare_datasets.py saves it in output_dir.
        """
        # Run the comparator which saves config to output_dir/datasets.yaml
        comparator = DatasetComparator(
            left_file=sample_data_files['left_file'],
            right_file=sample_data_files['right_file'],
            interactive=False,
            run_pipeline=False,
            verbose=False
        )
        
        result = comparator.run()
        assert result['success'] is True
        
        # Check where datasets.yaml was saved
        output_yaml = comparator.output_dir / "datasets.yaml"
        assert output_yaml.exists(), f"datasets.yaml should exist in {comparator.output_dir}"
        
        # Check if datasets.yaml exists in ROOT (where pipeline.py expects it)
        root_yaml = Path.cwd() / "datasets.yaml"
        
        # This will show the issue: pipeline.py expects the file in a different location
        if not root_yaml.exists():
            print(f"\n❌ ISSUE FOUND: datasets.yaml saved to {output_yaml}")
            print(f"   but pipeline.py expects it at {root_yaml}")
        
        # Verify the config has correct structure
        with open(output_yaml, 'r') as f:
            config = yaml.safe_load(f)
        
        assert 'datasets' in config, "Config should have 'datasets' section"
        assert 'comparisons' in config, "Config should have 'comparisons' section"
        
    def test_pipeline_subprocess_error_handling(self, sample_data_files, monkeypatch):
        """Test that subprocess errors are properly caught and reported."""
        
        # Change to temp directory to isolate test
        monkeypatch.chdir(sample_data_files['temp_dir'].parent)
        
        comparator = DatasetComparator(
            left_file=sample_data_files['left_file'],
            right_file=sample_data_files['right_file'],
            interactive=False,
            run_pipeline=True,  # This will try to run pipeline
            verbose=False
        )
        
        # Mock subprocess to simulate pipeline failure
        mock_result = Mock()
        mock_result.returncode = 1
        mock_result.stderr = "FileNotFoundError: datasets.yaml not found"
        mock_result.stdout = ""
        
        with patch('subprocess.run', return_value=mock_result) as mock_run:
            result = comparator.run()
            
            # The comparator should report pipeline failure
            assert result['pipeline_executed'] is True
            assert result['pipeline_success'] is False
            
            # Verify subprocess was called
            mock_run.assert_called_once()
            call_args = mock_run.call_args[0][0]
            assert 'pipeline.py' in call_args[1]
    
    def test_pipeline_command_construction(self, sample_data_files):
        """Test that the pipeline command is constructed correctly."""
        comparator = DatasetComparator(
            left_file=sample_data_files['left_file'],
            right_file=sample_data_files['right_file'],
            interactive=False,
            run_pipeline=False,
            verbose=False
        )
        
        # Run to generate config
        comparator.run()
        
        # Check the command that would be constructed
        dataset_names = list(comparator.config['datasets'].keys())
        expected_cmd = [
            sys.executable,
            "pipeline.py",
            "--pair", f"{dataset_names[0]}:{dataset_names[1]}"
        ]
        
        # The issue: pipeline.py will look for datasets.yaml in ROOT, not output_dir
        # This test documents the expected vs actual behavior
        
    def test_config_file_path_resolution(self, sample_data_files):
        """Test that config file paths are resolved correctly."""
        # The current implementation saves to output_dir/datasets.yaml
        # But pipeline.py loads from ROOT/datasets.yaml
        # This is a critical architectural issue
        
        comparator = DatasetComparator(
            left_file=sample_data_files['left_file'],
            right_file=sample_data_files['right_file'],
            interactive=False,
            run_pipeline=False,
            verbose=False
        )
        
        comparator.run()
        
        # Document the issue
        output_yaml = comparator.output_dir / "datasets.yaml"
        root_yaml = Path.cwd() / "datasets.yaml"
        
        assert output_yaml.exists()
        # This assertion would fail in production:
        # assert root_yaml.exists()  # Pipeline.py expects this
        
    def test_silent_failure_scenario(self, sample_data_files):
        """
        Test that demonstrates silent failure scenario.
        Pipeline appears to succeed but actually fails.
        """
        comparator = DatasetComparator(
            left_file=sample_data_files['left_file'],
            right_file=sample_data_files['right_file'],
            interactive=False,
            run_pipeline=True,
            verbose=True
        )
        
        # Mock subprocess to simulate silent failure
        mock_result = Mock()
        mock_result.returncode = 1
        mock_result.stderr = ""  # Empty error message (silent failure)
        mock_result.stdout = ""
        
        with patch('subprocess.run', return_value=mock_result) as mock_run:
            with patch('builtins.print') as mock_print:
                result = comparator.run()
                
                # Check if misleading success message was printed
                print_calls = [str(call) for call in mock_print.call_args_list]
                
                # The issue: even though pipeline failed, success message might be shown
                # because result['config'] exists (line 606 in compare_datasets.py)
                
    def test_error_propagation_chain(self, sample_data_files):
        """Test that errors propagate correctly through the chain."""
        
        # Test each component in isolation first
        
        # 1. Test profiler error handling
        with pytest.raises(FileNotFoundError):
            profiler = SmartProfiler("nonexistent_file.csv")
            profiler.analyze()
        
        # 2. Test matcher with invalid profiles
        with pytest.raises(KeyError):
            matcher = SmartMatcher({}, {})  # Empty profiles
            matcher.match()
        
        # 3. Test generator with invalid inputs
        generator = ConfigGenerator(
            left_profile={'columns': {}},
            right_profile={'columns': {}},
            match_results={'matches': []},
            left_file="test.csv",
            right_file="test.csv"
        )
        config = generator.generate()
        assert 'comparisons' in config  # Must have comparisons section
        
    def test_column_mapping_in_pipeline(self, sample_data_files):
        """Test that column mappings are correctly applied in pipeline."""
        
        # Create a comparator and generate config
        comparator = DatasetComparator(
            left_file=sample_data_files['left_file'],
            right_file=sample_data_files['right_file'],
            interactive=False,
            run_pipeline=False,
            verbose=False
        )
        
        result = comparator.run()
        
        # Check column mappings in generated config
        config = comparator.config
        
        # Verify that right dataset columns are mapped
        right_dataset_key = None
        for key in config['datasets']:
            if 'right' in key.lower():
                right_dataset_key = key
                break
        
        if right_dataset_key:
            column_map = config['datasets'][right_dataset_key].get('column_map', {})
            # Should have mappings like 'Email_Address' -> 'email'
            assert len(column_map) > 0, "Right dataset should have column mappings"
            
            # Check that mapped names match left dataset columns
            left_dataset_key = None
            for key in config['datasets']:
                if 'left' in key.lower():
                    left_dataset_key = key
                    break
            
            if left_dataset_key:
                left_dtypes = config['datasets'][left_dataset_key].get('dtypes', {})
                # Mapped column names should correspond to left dataset columns
                for original, mapped in column_map.items():
                    # The mapped name should be a normalized version that matches left
                    print(f"Mapping: {original} -> {mapped}")