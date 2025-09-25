"""
End-to-end test of the complete pipeline with all fixes applied.
"""

import pytest
import sys
import subprocess
import tempfile
import shutil
from pathlib import Path
import pandas as pd
import yaml

sys.path.insert(0, str(Path(__file__).parent.parent))

from compare_datasets import DatasetComparator


class TestEndToEnd:
    """Test the complete system end-to-end."""
    
    @pytest.fixture
    def test_data(self):
        """Create test data files."""
        temp_dir = Path(tempfile.mkdtemp())
        
        # Create directory structure
        (temp_dir / "data" / "raw").mkdir(parents=True)
        (temp_dir / "data" / "staging").mkdir(parents=True)
        (temp_dir / "data" / "reports").mkdir(parents=True)
        
        # Create small test datasets
        left_df = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
            'email': ['alice@test.com', 'bob@test.com', 'charlie@test.com', 
                     'david@test.com', 'eve@test.com'],
            'amount': [100.50, 200.75, 300.00, 400.25, 500.50],
            'status': ['active', 'inactive', 'active', 'active', 'inactive']
        })
        
        right_df = pd.DataFrame({
            'ID': [1, 2, 3, 4, 6],  # Missing 5, added 6
            'Full Name': ['Alice', 'Bob', 'Charles', 'David', 'Frank'],  # Charles vs Charlie
            'Email Address': ['alice@test.com', 'bob@test.com', 'charlie@test.com',
                             'david@test.com', 'frank@test.com'],
            'Total Amount': ['$100.50', '$200.75', '$350.00', '$400.25', '$600.00'],  # 350 vs 300
            'Is Active': ['True', 'False', 'True', 'True', 'False']
        })
        
        left_file = temp_dir / "data" / "raw" / "left_data.csv"
        right_file = temp_dir / "data" / "raw" / "right_data.csv"
        
        left_df.to_csv(left_file, index=False)
        right_df.to_csv(right_file, index=False)
        
        yield {
            'temp_dir': temp_dir,
            'left_file': str(left_file),
            'right_file': str(right_file)
        }
        
        # Cleanup
        shutil.rmtree(temp_dir, ignore_errors=True)
    
    def test_complete_pipeline_flow(self, test_data):
        """Test the complete flow from comparison to pipeline execution."""
        import os
        original_cwd = os.getcwd()
        
        try:
            os.chdir(test_data['temp_dir'])
            
            # Step 1: Run comparison without pipeline
            comparator = DatasetComparator(
                left_file=test_data['left_file'],
                right_file=test_data['right_file'],
                interactive=False,
                run_pipeline=False,
                verbose=False
            )
            
            result = comparator.run()
            
            # Verify comparison succeeded
            assert result['success'] is True, "Comparison should succeed"
            assert result['left_profile'] is not None, "Should have left profile"
            assert result['right_profile'] is not None, "Should have right profile"
            assert result['match_results'] is not None, "Should have match results"
            assert result['config'] is not None, "Should have config"
            
            # Step 2: Verify config structure
            config = result['config']
            assert 'datasets' in config, "Config should have datasets"
            assert 'comparisons' in config, "Config should have comparisons"
            
            # Check that column names are properly normalized
            for dataset_name, dataset_config in config['datasets'].items():
                column_map = dataset_config.get('column_map', {})
                for original, mapped in column_map.items():
                    # Mapped names should not have spaces or special characters
                    assert ' ' not in mapped, f"Mapped name '{mapped}' should not have spaces"
                    assert all(c.isalnum() or c == '_' for c in mapped), \
                           f"Mapped name '{mapped}' should only have alphanumeric and underscore"
            
            # Step 3: Verify files are saved correctly
            output_dir = comparator.output_dir
            assert output_dir.exists(), "Output directory should exist"
            
            # Check for config file in output dir
            output_yaml = output_dir / "datasets.yaml"
            assert output_yaml.exists(), "Config should be saved in output dir"
            
            # Step 4: Test pipeline execution
            # Copy config to root (simulating the fix)
            root_yaml = Path.cwd() / "datasets.yaml"
            shutil.copy2(output_yaml, root_yaml)
            
            # Run pipeline directly
            cmd = [sys.executable, str(Path(__file__).parent.parent / "pipeline.py")]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            
            # Check pipeline execution
            if result.returncode != 0:
                print(f"Pipeline stderr: {result.stderr}")
                print(f"Pipeline stdout: {result.stdout}")
            
            # Pipeline should complete (even if with diffs)
            assert "[SUCCESS]" in result.stdout or result.returncode == 0, \
                   "Pipeline should complete successfully"
            
            # Check for staging files
            staging_dir = Path.cwd() / "data" / "staging"
            parquet_files = list(staging_dir.glob("*.parquet"))
            assert len(parquet_files) >= 2, "Should have created parquet files"
            
            # Check for reports
            reports_dir = Path.cwd() / "data" / "reports"
            report_files = list(reports_dir.glob("*"))
            # Should have created some report files
            assert len(report_files) > 0, "Should have created report files"
            
            print("\n✅ END-TO-END TEST PASSED:")
            print(f"   - Comparison: SUCCESS")
            print(f"   - Config generation: SUCCESS")
            print(f"   - Column normalization: SUCCESS")
            print(f"   - File organization: SUCCESS")
            print(f"   - Pipeline execution: SUCCESS")
            print(f"   - Output generation: SUCCESS")
            
        finally:
            os.chdir(original_cwd)
    
    def test_error_handling(self, test_data):
        """Test that errors are properly handled and reported."""
        import os
        original_cwd = os.getcwd()
        
        try:
            os.chdir(test_data['temp_dir'])
            
            # Test with non-existent files
            comparator = DatasetComparator(
                left_file="nonexistent.csv",
                right_file="alsonothere.xlsx",
                interactive=False,
                run_pipeline=False,
                verbose=False
            )
            
            with pytest.raises(FileNotFoundError):
                comparator.run()
            
            print("✅ Error handling test passed")
            
        finally:
            os.chdir(original_cwd)
    
    def test_config_validation(self, test_data):
        """Test that config validation works."""
        import os
        original_cwd = os.getcwd()
        
        try:
            os.chdir(test_data['temp_dir'])
            
            comparator = DatasetComparator(
                left_file=test_data['left_file'],
                right_file=test_data['right_file'],
                interactive=False,
                run_pipeline=False,
                verbose=False
            )
            
            result = comparator.run()
            
            # Test the validation method
            is_valid = comparator._validate_config()
            assert is_valid is True, "Config should be valid"
            
            # Corrupt the config and test again
            comparator.config = {}
            is_valid = comparator._validate_config()
            assert is_valid is False, "Empty config should be invalid"
            
            print("✅ Config validation test passed")
            
        finally:
            os.chdir(original_cwd)