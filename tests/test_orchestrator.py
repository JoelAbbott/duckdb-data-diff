"""
Test suite for the compare_datasets orchestrator.
Following TDD approach - tests written before implementation.
"""

import pytest
import json
import yaml
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock, call
import sys
import tempfile

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))

from compare_datasets import DatasetComparator, main


class TestDatasetComparator:
    """Test suite for DatasetComparator orchestrator."""
    
    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for test outputs."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)
    
    @pytest.fixture
    def sample_files(self, temp_dir):
        """Create sample test files."""
        # Create dummy files
        left_file = temp_dir / "test_left.xlsx"
        right_file = temp_dir / "test_right.csv"
        
        left_file.touch()
        right_file.touch()
        
        return str(left_file), str(right_file)
    
    def test_initialization(self, sample_files):
        """Test DatasetComparator initialization."""
        left_file, right_file = sample_files
        
        comparator = DatasetComparator(
            left_file=left_file,
            right_file=right_file,
            output_dir="output",
            interactive=False,
            run_pipeline=False
        )
        
        assert str(comparator.left_file) == left_file or comparator.left_file == Path(left_file)
        assert str(comparator.right_file) == right_file or comparator.right_file == Path(right_file)
        assert comparator.output_dir == Path("output")
        assert comparator.interactive is False
        assert comparator.run_pipeline is False
    
    def test_file_validation(self, temp_dir):
        """Test that file existence is validated."""
        valid_file = temp_dir / "valid.csv"
        valid_file.touch()
        
        # Should raise error for non-existent file
        with pytest.raises(FileNotFoundError):
            DatasetComparator(
                left_file=str(valid_file),
                right_file="nonexistent.csv"
            )
        
        # Should raise error for both non-existent
        with pytest.raises(FileNotFoundError):
            DatasetComparator(
                left_file="nonexistent1.csv",
                right_file="nonexistent2.csv"
            )
    
    @patch('compare_datasets.SmartProfiler')
    @patch('compare_datasets.SmartMatcher')
    @patch('compare_datasets.ConfigGenerator')
    def test_run_basic_flow(self, mock_gen, mock_matcher, mock_profiler, sample_files):
        """Test basic end-to-end flow without pipeline execution."""
        left_file, right_file = sample_files
        
        # Setup mocks
        mock_profile_instance = Mock()
        mock_profile_instance.profile = {'columns': {}, 'row_count': 100}
        mock_profiler.return_value = mock_profile_instance
        
        mock_matcher_instance = Mock()
        mock_matcher_instance.get_match_result.return_value = {
            'matches': [],
            'unmatched_left': [],
            'unmatched_right': [],
            'summary': {'total_matches': 0}
        }
        mock_matcher.return_value = mock_matcher_instance
        
        mock_gen_instance = Mock()
        mock_gen_instance.config = {'datasets': {}}
        mock_gen.return_value = mock_gen_instance
        
        # Run comparator
        comparator = DatasetComparator(
            left_file=left_file,
            right_file=right_file,
            run_pipeline=False
        )
        
        result = comparator.run()
        
        # Verify all components were called
        assert mock_profiler.call_count == 2  # Once for each file
        mock_profile_instance.analyze.assert_called()
        mock_matcher_instance.match.assert_called_once()
        mock_gen_instance.generate.assert_called_once()
        
        # Check result structure
        assert 'left_profile' in result
        assert 'right_profile' in result
        assert 'match_results' in result
        assert 'config' in result
        assert result['success'] is True
    
    @patch('compare_datasets.SmartProfiler')
    def test_progress_indicators(self, mock_profiler, sample_files, capsys):
        """Test that progress indicators are displayed."""
        left_file, right_file = sample_files
        
        # Setup mock
        mock_profile_instance = Mock()
        mock_profile_instance.profile = {'columns': {}, 'row_count': 100}
        mock_profiler.return_value = mock_profile_instance
        
        comparator = DatasetComparator(
            left_file=left_file,
            right_file=right_file,
            verbose=True
        )
        
        # Mock other components to avoid errors
        with patch('compare_datasets.SmartMatcher'), \
             patch('compare_datasets.ConfigGenerator'):
            comparator.run()
        
        # Check output contains progress messages
        captured = capsys.readouterr()
        assert "Analyzing" in captured.out or "Profiling" in captured.out
        assert "✓" in captured.out or "complete" in captured.out.lower()
    
    @patch('compare_datasets.SmartProfiler')
    @patch('compare_datasets.SmartMatcher')
    @patch('compare_datasets.ConfigGenerator')
    def test_save_intermediate_results(self, mock_gen, mock_matcher, mock_profiler, 
                                       sample_files, temp_dir):
        """Test that intermediate results are saved."""
        left_file, right_file = sample_files
        
        # Setup mocks
        mock_profile = {'columns': {'col1': {}}, 'row_count': 100}
        mock_profile_instance = Mock()
        mock_profile_instance.profile = mock_profile
        mock_profiler.return_value = mock_profile_instance
        
        mock_match_result = {
            'matches': [{'left_column': 'col1', 'right_column': 'col1', 'confidence': 1.0}],
            'unmatched_left': [],
            'unmatched_right': [],
            'summary': {'total_matches': 1}
        }
        mock_matcher_instance = Mock()
        mock_matcher_instance.get_match_result.return_value = mock_match_result
        mock_matcher.return_value = mock_matcher_instance
        
        mock_config = {'datasets': {'test': {}}}
        mock_gen_instance = Mock()
        mock_gen_instance.config = mock_config
        mock_gen.return_value = mock_gen_instance
        
        # Run with output directory
        comparator = DatasetComparator(
            left_file=left_file,
            right_file=right_file,
            output_dir=str(temp_dir)
        )
        
        comparator.run()
        
        # Check that files were saved
        assert (temp_dir / "left_profile.json").exists()
        assert (temp_dir / "right_profile.json").exists()
        assert (temp_dir / "match_results.json").exists()
        assert (temp_dir / "datasets.yaml").exists()
    
    @patch('builtins.input')
    @patch('compare_datasets.SmartMatcher')
    def test_interactive_mode_accept(self, mock_matcher, mock_input, sample_files):
        """Test interactive mode where user accepts suggestions."""
        left_file, right_file = sample_files
        
        # Setup matcher mock with medium confidence matches
        mock_match_result = {
            'matches': [
                {'left_column': 'email', 'right_column': 'email_addr', 
                 'confidence': 0.65, 'match_reason': 'pattern_match'},
                {'left_column': 'name', 'right_column': 'full_name',
                 'confidence': 0.55, 'match_reason': 'fuzzy_match'}
            ],
            'unmatched_left': [],
            'unmatched_right': ['extra_col'],
            'summary': {'total_matches': 2}
        }
        mock_matcher_instance = Mock()
        mock_matcher_instance.get_match_result.return_value = mock_match_result
        mock_matcher_instance.matches = mock_match_result['matches']
        mock_matcher.return_value = mock_matcher_instance
        
        # User accepts all suggestions (press Enter)
        mock_input.side_effect = ['', '', 'n']  # Accept, Accept, Don't add unmatched
        
        comparator = DatasetComparator(
            left_file=left_file,
            right_file=right_file,
            interactive=True
        )
        
        # Mock other components
        with patch('compare_datasets.SmartProfiler'):
            with patch('compare_datasets.ConfigGenerator'):
                result = comparator.run()
        
        # Verify matches were kept
        assert len(result['match_results']['matches']) == 2
    
    @patch('builtins.input')
    @patch('compare_datasets.SmartMatcher')
    def test_interactive_mode_modify(self, mock_matcher, mock_input, sample_files):
        """Test interactive mode where user modifies matches."""
        left_file, right_file = sample_files
        
        # Setup matcher mock
        mock_match_result = {
            'matches': [
                {'left_column': 'email', 'right_column': 'email_addr',
                 'confidence': 0.65, 'match_reason': 'pattern_match'}
            ],
            'unmatched_left': ['phone'],
            'unmatched_right': ['contact_phone', 'extra'],
            'summary': {'total_matches': 1}
        }
        mock_matcher_instance = Mock()
        mock_matcher_instance.get_match_result.return_value = mock_match_result
        mock_matcher_instance.matches = mock_match_result['matches']
        mock_matcher.return_value = mock_matcher_instance
        
        # User chooses different column for first match
        mock_input.side_effect = [
            '2',  # Choose different column
            '1',  # Select contact_phone
            'y',  # Add unmatched columns
            '1',  # Match phone
            '1',  # To contact_phone
            'n'   # No more matches
        ]
        
        comparator = DatasetComparator(
            left_file=left_file,
            right_file=right_file,
            interactive=True
        )
        
        # Mock profiles with column info
        with patch('compare_datasets.SmartProfiler') as mock_profiler:
            mock_profile = Mock()
            mock_profile.profile = {
                'columns': {'email': {}, 'phone': {}},
                'row_count': 100
            }
            mock_profiler.return_value = mock_profile
            
            with patch('compare_datasets.ConfigGenerator'):
                result = comparator.run()
        
        # Input was mocked, so we just verify the flow completed
        assert result['success'] is True
    
    @patch('compare_datasets.subprocess.run')
    @patch('compare_datasets.SmartProfiler')
    @patch('compare_datasets.SmartMatcher')
    @patch('compare_datasets.ConfigGenerator')
    def test_pipeline_execution(self, mock_gen, mock_matcher, mock_profiler, 
                                mock_subprocess, sample_files):
        """Test that pipeline is executed when requested."""
        left_file, right_file = sample_files
        
        # Setup mocks
        mock_profiler.return_value = Mock(profile={'columns': {}})
        mock_matcher.return_value = Mock(
            get_match_result=Mock(return_value={'matches': [], 'summary': {}})
        )
        mock_gen.return_value = Mock(config={'datasets': {}})
        
        # Mock successful pipeline run
        mock_subprocess.return_value = Mock(returncode=0, stdout="Success", stderr="")
        
        comparator = DatasetComparator(
            left_file=left_file,
            right_file=right_file,
            run_pipeline=True
        )
        
        result = comparator.run()
        
        # Verify pipeline was called
        mock_subprocess.assert_called()
        assert result['pipeline_executed'] is True
        assert result['pipeline_success'] is True
    
    def test_command_line_interface(self):
        """Test the CLI argument parsing."""
        test_args = [
            'compare_datasets.py',
            'left.csv',
            'right.xlsx',
            '--output', 'results',
            '--interactive',
            '--run-pipeline'
        ]
        
        with patch('sys.argv', test_args):
            with patch('compare_datasets.DatasetComparator') as mock_comparator:
                with patch('pathlib.Path.exists', return_value=True):
                    main()
                
                # Verify comparator was initialized with correct arguments
                mock_comparator.assert_called_once()
                call_args = mock_comparator.call_args[1]
                assert call_args['left_file'] == 'left.csv'
                assert call_args['right_file'] == 'right.xlsx'
                assert call_args['output_dir'] == 'results'
                assert call_args['interactive'] is True
                assert call_args['run_pipeline'] is True
    
    def test_config_reuse(self, sample_files):
        """Test using existing configuration."""
        left_file, right_file = sample_files
        
        comparator = DatasetComparator(
            left_file=left_file,
            right_file=right_file,
            config_file="existing_config.yaml"
        )
        
        # Mock loading existing config
        with patch('builtins.open'), \
             patch('yaml.safe_load', return_value={'datasets': {}}):
            with patch('compare_datasets.subprocess.run'):
                # Should skip profiling and matching when config provided
                with patch('compare_datasets.SmartProfiler') as mock_profiler:
                    with patch('compare_datasets.SmartMatcher') as mock_matcher:
                        comparator.run()
                        
                        # These shouldn't be called when using existing config
                        mock_profiler.assert_not_called()
                        mock_matcher.assert_not_called()
    
    @patch('compare_datasets.SmartProfiler')
    def test_error_handling(self, mock_profiler, sample_files):
        """Test error handling and recovery."""
        left_file, right_file = sample_files
        
        # Mock profiler to raise an error
        mock_profiler.side_effect = Exception("Profiling failed")
        
        comparator = DatasetComparator(
            left_file=left_file,
            right_file=right_file
        )
        
        result = comparator.run()
        
        # Should handle error gracefully
        assert result['success'] is False
        assert 'error' in result
        assert "Profiling failed" in result['error']
    
    def test_summary_report(self, sample_files, capsys):
        """Test that summary report is displayed."""
        left_file, right_file = sample_files
        
        comparator = DatasetComparator(
            left_file=left_file,
            right_file=right_file,
            verbose=True
        )
        
        # Mock components for successful run
        with patch('compare_datasets.SmartProfiler') as mock_profiler:
            with patch('compare_datasets.SmartMatcher') as mock_matcher:
                with patch('compare_datasets.ConfigGenerator'):
                    # Setup mocks
                    mock_profiler.return_value = Mock(
                        profile={'columns': {'col1': {}}, 'row_count': 100}
                    )
                    mock_matcher.return_value = Mock(
                        get_match_result=Mock(return_value={
                            'matches': [{'left_column': 'col1', 'right_column': 'col1'}],
                            'unmatched_left': [],
                            'unmatched_right': [],
                            'summary': {
                                'total_matches': 1,
                                'high_confidence_matches': 1,
                                'average_confidence': 0.95
                            }
                        })
                    )
                    
                    comparator.run()
        
        captured = capsys.readouterr()
        
        # Check for summary elements
        assert "Summary" in captured.out or "Results" in captured.out
        assert "match" in captured.out.lower()
        assert "1" in captured.out  # match count
    
    def test_real_files(self):
        """Test with actual qa2 and netsuite files if they exist."""
        data_dir = Path(__file__).parent.parent / 'data' / 'raw'
        excel_file = data_dir / 'qa2_netsuite_messages.xlsx'
        csv_file = data_dir / 'netsuite_messages (1).csv'
        
        if not excel_file.exists() or not csv_file.exists():
            pytest.skip("Test files not found")
        
        comparator = DatasetComparator(
            left_file=str(excel_file),
            right_file=str(csv_file),
            output_dir=str(data_dir / 'test_output'),
            verbose=False
        )
        
        result = comparator.run()
        
        # Basic assertions
        assert result['success'] is True
        assert 'left_profile' in result
        assert 'right_profile' in result
        assert 'match_results' in result
        assert 'config' in result
        
        # Check that some matches were found
        matches = result['match_results']['matches']
        assert len(matches) > 0
        
        # Cleanup
        import shutil
        output_dir = data_dir / 'test_output'
        if output_dir.exists():
            shutil.rmtree(output_dir)