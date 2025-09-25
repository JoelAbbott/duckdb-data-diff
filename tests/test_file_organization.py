"""
Test suite for file organization and naming conventions.
Following TDD approach - tests written before implementation.
"""

import pytest
from pathlib import Path
from datetime import datetime
from unittest.mock import Mock, patch
import sys
import re

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))

from compare_datasets import DatasetComparator


class TestFileOrganization:
    """Test suite for file organization and naming."""
    
    def test_descriptive_file_names_with_timestamps(self):
        """Test that generated files have descriptive names with timestamps."""
        with patch('compare_datasets.datetime') as mock_datetime:
            mock_datetime.now.return_value.strftime.return_value = "20241223_143022"
            
            comparator = DatasetComparator("qa2_netsuite_messages.xlsx", "netsuite_messages.csv")
            
            # Generate output file names
            profile_name = comparator._get_output_filename("profile", "left")
            match_name = comparator._get_output_filename("match_results")
            config_name = comparator._get_output_filename("config")
            
            # Should include dataset names and timestamp
            assert "qa2_netsuite_messages" in profile_name
            assert "20241223_143022" in profile_name
            assert profile_name.endswith("_profile_left.json")
            
            assert "qa2_vs_netsuite" in match_name or "netsuite" in match_name
            assert "20241223_143022" in match_name
            assert "match_results" in match_name
            
            assert "qa2_vs_netsuite" in config_name or "netsuite" in config_name
            assert "20241223_143022" in config_name
            assert config_name.endswith(".yaml") or config_name.endswith(".yml")
    
    def test_single_directory_per_comparison(self):
        """Test that all files for a comparison go in a single directory."""
        with patch('compare_datasets.datetime') as mock_datetime:
            mock_datetime.now.return_value.strftime.return_value = "20241223_143022"
            
            comparator = DatasetComparator("dataset1.xlsx", "dataset2.csv")
            
            # Get output directory
            output_dir = comparator._get_output_directory()
            
            # Should be under data/comparisons/
            assert "comparisons" in str(output_dir)
            
            # Should include dataset names and timestamp
            assert "dataset1_vs_dataset2" in str(output_dir) or "dataset1" in str(output_dir)
            assert "20241223_143022" in str(output_dir)
            
            # Should have subdirectories for organization
            profiles_dir = output_dir / "profiles"
            matches_dir = output_dir / "matches" 
            config_dir = output_dir / "config"
            reports_dir = output_dir / "reports"
            
            # Test that subdirectories would be created
            expected_dirs = [profiles_dir, matches_dir, config_dir, reports_dir]
            for dir_path in expected_dirs:
                assert dir_path.parent == output_dir
    
    def test_no_generic_file_names(self):
        """Test that we don't use generic names like 'left_profile.json'."""
        comparator = DatasetComparator("test1.csv", "test2.csv")
        
        # These generic names should not be used
        bad_names = [
            "left_profile.json",
            "right_profile.json", 
            "config.yaml",
            "match_results.json"
        ]
        
        # Get generated names
        with patch('compare_datasets.datetime') as mock_datetime:
            mock_datetime.now.return_value.strftime.return_value = "20241223_143022"
            
            profile_left = comparator._get_output_filename("profile", "left")
            profile_right = comparator._get_output_filename("profile", "right")
            config = comparator._get_output_filename("config")
            matches = comparator._get_output_filename("match_results")
            
            generated_names = [profile_left, profile_right, config, matches]
            
            # None should be generic names
            for name in generated_names:
                assert name not in bad_names
                # Should include dataset identifier
                assert "test1" in name or "test2" in name or "test1_vs_test2" in name
    
    def test_cleanup_old_files_functionality(self):
        """Test that cleanup function can remove old comparison directories."""
        import tempfile
        import shutil
        from datetime import timedelta
        
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            comparisons_dir = tmpdir_path / "comparisons"
            comparisons_dir.mkdir()
            
            # Create mock old directories
            old_dir1 = comparisons_dir / "dataset1_vs_dataset2_20241201_120000"
            old_dir2 = comparisons_dir / "dataset3_vs_dataset4_20241210_150000"
            recent_dir = comparisons_dir / "dataset5_vs_dataset6_20241222_180000"
            
            for dir_path in [old_dir1, old_dir2, recent_dir]:
                dir_path.mkdir()
                (dir_path / "test.txt").touch()
            
            # Run cleanup (keep last 7 days)
            from compare_datasets import cleanup_old_comparisons
            
            with patch('compare_datasets.datetime') as mock_datetime:
                mock_datetime.now.return_value = datetime(2024, 12, 23, 14, 30, 22)
                
                # Cleanup should remove directories older than 7 days
                cleanup_old_comparisons(comparisons_dir, days_to_keep=7)
            
            # Old directories should be removed
            assert not old_dir1.exists()
            assert not old_dir2.exists()
            # Recent directory should remain
            assert recent_dir.exists()
    
    def test_archive_functionality(self):
        """Test that old files can be archived instead of deleted."""
        import tempfile
        
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            comparisons_dir = tmpdir_path / "comparisons"
            archive_dir = tmpdir_path / "archive"
            comparisons_dir.mkdir()
            
            # Create a comparison directory
            comp_dir = comparisons_dir / "dataset1_vs_dataset2_20241201_120000"
            comp_dir.mkdir()
            (comp_dir / "config.yaml").touch()
            
            from compare_datasets import archive_old_comparison
            
            # Archive the comparison
            archive_old_comparison(comp_dir, archive_dir)
            
            # Should be moved to archive
            assert not comp_dir.exists()
            archived_dir = archive_dir / "dataset1_vs_dataset2_20241201_120000"
            assert archived_dir.exists()
            assert (archived_dir / "config.yaml").exists()
    
    def test_filename_sanitization(self):
        """Test that special characters in dataset names are handled."""
        comparator = DatasetComparator(
            "data (1)/test file!.xlsx",
            "dataset@2#$.csv"
        )
        
        with patch('compare_datasets.datetime') as mock_datetime:
            mock_datetime.now.return_value.strftime.return_value = "20241223_143022"
            
            output_dir = comparator._get_output_directory()
            
            # Should sanitize special characters
            dir_name = output_dir.name
            
            # Should not contain problematic characters
            assert "!" not in dir_name
            assert "@" not in dir_name
            assert "#" not in dir_name
            assert "$" not in dir_name
            assert "(" not in dir_name
            assert ")" not in dir_name
            
            # Should still be identifiable
            assert "test_file" in dir_name or "dataset_2" in dir_name
    
    def test_output_structure_matches_spec(self):
        """Test that output structure matches the specification."""
        expected_structure = """
        data/
        ├── raw/                    # Input files
        ├── comparisons/           # All results
        │   └── qa2_vs_netsuite_20241223_143022/
        │       ├── profiles/
        │       ├── matches/
        │       ├── config/
        │       └── reports/
        └── archive/              # Old files
        """
        
        comparator = DatasetComparator("qa2.xlsx", "netsuite.csv")
        
        with patch('compare_datasets.datetime') as mock_datetime:
            mock_datetime.now.return_value.strftime.return_value = "20241223_143022"
            
            output_dir = comparator._get_output_directory()
            
            # Check base structure
            assert "comparisons" in str(output_dir)
            assert output_dir.parent.name == "comparisons"
            
            # Check comparison directory name format
            assert re.match(r"qa2_vs_netsuite_\d{8}_\d{6}", output_dir.name)
            
            # Check subdirectories
            expected_subdirs = ["profiles", "matches", "config", "reports"]
            subdirs = comparator._get_subdirectories()
            
            for subdir in expected_subdirs:
                assert subdir in subdirs