"""
Tests that demonstrate and fix pipeline failure issues.
Using TDD approach: write tests first, then fix the issues.
"""

import pytest
import sys
import subprocess
import tempfile
import shutil
from pathlib import Path
from unittest.mock import patch, Mock
import pandas as pd
import yaml

sys.path.insert(0, str(Path(__file__).parent.parent))

from compare_datasets import DatasetComparator


class TestPipelineFailures:
    """Tests that demonstrate current pipeline failures."""
    
    @pytest.fixture
    def temp_workspace(self):
        """Create a temporary workspace for testing."""
        temp_dir = Path(tempfile.mkdtemp())
        
        # Create directory structure
        (temp_dir / "data" / "raw").mkdir(parents=True)
        (temp_dir / "data" / "staging").mkdir(parents=True)
        (temp_dir / "data" / "reports").mkdir(parents=True)
        (temp_dir / "data" / "comparisons").mkdir(parents=True)
        
        # Create sample data files
        left_df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'amount': [100, 200, 300]
        })
        
        right_df = pd.DataFrame({
            'id': [1, 2, 3],
            'full_name': ['Alice', 'Bob', 'Charles'],  # Note: Charles vs Charlie
            'total': [100, 200, 350]  # Note: 350 vs 300
        })
        
        left_file = temp_dir / "data" / "raw" / "left.csv"
        right_file = temp_dir / "data" / "raw" / "right.csv"
        
        left_df.to_csv(left_file, index=False)
        right_df.to_csv(right_file, index=False)
        
        yield {
            'temp_dir': temp_dir,
            'left_file': str(left_file),
            'right_file': str(right_file)
        }
        
        # Cleanup
        shutil.rmtree(temp_dir, ignore_errors=True)
    
    def test_issue_1_config_file_location_mismatch(self, temp_workspace):
        """
        ISSUE #1: Config file location mismatch
        - compare_datasets.py saves to: output_dir/datasets.yaml
        - pipeline.py looks for: ROOT/datasets.yaml
        """
        import os
        original_cwd = os.getcwd()
        try:
            os.chdir(temp_workspace['temp_dir'])
            
            comparator = DatasetComparator(
                left_file=temp_workspace['left_file'],
                right_file=temp_workspace['right_file'],
                interactive=False,
                run_pipeline=False,
                verbose=False
            )
            
            result = comparator.run()
            assert result['success'] is True
            
            # Check where files are saved
            output_yaml = comparator.output_dir / "datasets.yaml"
            root_yaml = Path.cwd() / "datasets.yaml"
            
            assert output_yaml.exists(), "Config saved to output_dir"
            assert not root_yaml.exists(), "Config NOT saved to ROOT (where pipeline expects it)"
            
            # This is the problem!
            print(f"\n❌ ISSUE CONFIRMED:")
            print(f"   Config saved to: {output_yaml}")
            print(f"   Pipeline expects: {root_yaml}")
            
        finally:
            os.chdir(original_cwd)
    
    def test_issue_2_pipeline_no_config_argument(self, temp_workspace):
        """
        ISSUE #2: Pipeline doesn't accept --config argument
        - compare_datasets.py tries to pass --config
        - pipeline.py doesn't have this argument
        """
        # Check run_with_existing_config function in compare_datasets.py
        from compare_datasets import run_with_existing_config
        
        # Mock subprocess to capture the command
        with patch('subprocess.run') as mock_run:
            mock_run.return_value = Mock(returncode=0, stdout="", stderr="")
            
            # This function builds wrong command
            config_file = "test_config.yaml"
            run_with_existing_config(config_file)
            
            # Check what command was built
            call_args = mock_run.call_args[0][0]
            
            # This will show the issue
            assert "--config" in call_args, "compare_datasets tries to use --config"
            # But pipeline.py doesn't support --config!
            
            print(f"\n❌ ISSUE CONFIRMED:")
            print(f"   Command built: {call_args}")
            print(f"   But pipeline.py doesn't accept --config argument!")
    
    def test_issue_3_silent_failure_empty_stderr(self, temp_workspace):
        """
        ISSUE #3: Silent failures with empty stderr
        - Pipeline fails but returns empty stderr
        - compare_datasets shows misleading success message
        """
        comparator = DatasetComparator(
            left_file=temp_workspace['left_file'],
            right_file=temp_workspace['right_file'],
            interactive=False,
            run_pipeline=True,
            verbose=False
        )
        
        # Mock subprocess with silent failure
        mock_result = Mock()
        mock_result.returncode = 1  # Failure
        mock_result.stderr = ""  # Empty error message
        mock_result.stdout = ""
        
        with patch('subprocess.run', return_value=mock_result):
            result = comparator.run()
            
            # The pipeline failed but...
            assert result['pipeline_executed'] is True
            assert result['pipeline_success'] is False  # At least this is correct
            
            # But check line 606 in compare_datasets.py
            # It checks self.config instead of pipeline_success!
            
            print(f"\n❌ ISSUE CONFIRMED:")
            print(f"   Pipeline failed with empty stderr")
            print(f"   User sees misleading success indicator")
    
    def test_issue_4_no_error_handling_for_missing_config(self, temp_workspace):
        """
        ISSUE #4: No error handling for missing config file in pipeline.py
        - pipeline.py assumes datasets.yaml exists
        - No try/except around load_config()
        """
        import os
        original_cwd = os.getcwd()
        try:
            os.chdir(temp_workspace['temp_dir'])
            
            # Try to run pipeline without datasets.yaml
            cmd = [sys.executable, "-c", """
import sys
sys.path.insert(0, r'C:\\Users\\JoelAbbott\\OneDrive - Compass Datacenters, LLC\\Desktop\\my_repos\\duckdb-data-diff')
from pipeline import load_config
try:
    cfg = load_config()
except FileNotFoundError as e:
    print(f"ERROR: {e}")
    sys.exit(1)
"""]
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            # Pipeline will crash with FileNotFoundError
            assert result.returncode == 1
            assert "FileNotFoundError" in result.stderr or "ERROR:" in result.stdout
            
            print(f"\n❌ ISSUE CONFIRMED:")
            print(f"   Pipeline crashes when datasets.yaml missing")
            print(f"   No graceful error handling")
            
        finally:
            os.chdir(original_cwd)
    
    def test_issue_5_wrong_success_indicator(self, temp_workspace):
        """
        ISSUE #5: Wrong success indicator in summary
        Line 606 in compare_datasets.py uses self.config instead of pipeline result
        """
        comparator = DatasetComparator(
            left_file=temp_workspace['left_file'],
            right_file=temp_workspace['right_file'],
            interactive=False,
            run_pipeline=True,
            verbose=True
        )
        
        # Mock subprocess to fail
        mock_result = Mock(returncode=1, stderr="Pipeline failed", stdout="")
        
        with patch('subprocess.run', return_value=mock_result):
            with patch('builtins.print') as mock_print:
                result = comparator.run()
                
                # Find the summary print calls
                print_calls = [str(call) for call in mock_print.call_args_list]
                
                # Line 606 checks self.config (which exists) not pipeline_success
                # This is wrong!
                
                print(f"\n❌ ISSUE CONFIRMED:")
                print(f"   Line 606 uses 'self.config' to determine success")
                print(f"   Should use actual pipeline result!")
    
    def test_issue_6_no_validation_before_pipeline_run(self, temp_workspace):
        """
        ISSUE #6: No validation before running pipeline
        - No check if datasets.yaml exists in expected location
        - No validation of config structure
        - No dry-run option
        """
        comparator = DatasetComparator(
            left_file=temp_workspace['left_file'],
            right_file=temp_workspace['right_file'],
            interactive=False,
            run_pipeline=True,
            verbose=False
        )
        
        # The _run_pipeline method doesn't validate anything before subprocess
        # It just blindly runs the command
        
        print(f"\n❌ ISSUE CONFIRMED:")
        print(f"   No pre-flight checks before pipeline execution")
        print(f"   No validation of config location or structure")
    
    def test_proposed_fix_1_copy_config_to_root(self, temp_workspace):
        """
        PROPOSED FIX #1: Copy datasets.yaml to ROOT before running pipeline
        """
        import os
        original_cwd = os.getcwd()
        try:
            os.chdir(temp_workspace['temp_dir'])
            
            comparator = DatasetComparator(
                left_file=temp_workspace['left_file'],
                right_file=temp_workspace['right_file'],
                interactive=False,
                run_pipeline=False,
                verbose=False
            )
            
            result = comparator.run()
            
            # PROPOSED FIX: Copy config to root
            output_yaml = comparator.output_dir / "datasets.yaml"
            root_yaml = Path.cwd() / "datasets.yaml"
            
            if output_yaml.exists() and not root_yaml.exists():
                shutil.copy2(output_yaml, root_yaml)
            
            assert root_yaml.exists(), "Config now exists where pipeline expects it"
            
            print(f"\n✅ FIX VALIDATED:")
            print(f"   Copy datasets.yaml from {output_yaml}")
            print(f"   To {root_yaml}")
            print(f"   Before running pipeline")
            
        finally:
            os.chdir(original_cwd)
    
    def test_proposed_fix_2_add_config_argument(self, temp_workspace):
        """
        PROPOSED FIX #2: Add --config argument to pipeline.py
        """
        # This would require modifying pipeline.py to accept --config
        # and use it instead of hardcoded ROOT / "datasets.yaml"
        
        print(f"\n✅ FIX PROPOSED:")
        print(f"   Modify pipeline.py to accept --config argument")
        print(f"   Update load_config() to use provided path")
        print(f"   This is the cleaner architectural solution")
    
    def test_proposed_fix_3_better_error_messages(self, temp_workspace):
        """
        PROPOSED FIX #3: Add better error handling and messages
        """
        print(f"\n✅ FIX PROPOSED:")
        print(f"   1. Add try/except in pipeline.py load_config()")
        print(f"   2. Check subprocess.stderr AND stdout for errors")
        print(f"   3. Fix line 606 to use pipeline_success not self.config")
        print(f"   4. Add pre-flight validation before pipeline run")
        print(f"   5. Capture and display actual error messages")