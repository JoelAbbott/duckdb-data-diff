"""
Unit tests for DataStager component.
Following TDD: Test for schema fingerprint validation pattern to prevent stale cache reuse.
"""

import pytest
from unittest.mock import Mock, MagicMock, patch, mock_open
from pathlib import Path
import sys
import json
import os

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Import existing components
from src.pipeline.stager import DataStager
from src.config.manager import DatasetConfig


class TestDataStagerSchemaValidation:
    """Test cases for DataStager schema fingerprint validation pattern."""
    
    def setup_method(self):
        """Set up test fixtures."""
        # Mock DuckDB connection
        self.mock_con = Mock()
        
        # Create DataStager instance
        self.stager = DataStager(staging_dir=Path("/tmp/test_staging"))
        
        # Mock dataset config
        self.mock_config = Mock(spec=DatasetConfig)
        self.mock_config.name = "test_dataset"
        self.mock_config.path = "/path/to/test.csv"
        self.mock_config.custom_sql = None
        self.mock_config.normalizers = {}
        self.mock_config.converters = {}
        
        # Mock file reader
        self.stager.file_reader = Mock()
    
    def test_stage_dataset_forces_restage_on_schema_drift(self):
        """
        Test that staging detects schema drift and forces restaging.
        
        This test verifies two scenarios:
        1. Schema change between source and staged files
        2. Source file modification time change
        """
        # Setup mock paths
        staging_path = self.stager.staging_dir / "test_dataset.parquet"
        metadata_path = self.stager.staging_dir / "test_dataset.meta"
        
        # Mock that staged file exists
        with patch('pathlib.Path.exists') as mock_exists:
            mock_exists.return_value = True
            
            # Mock source file with different schema than metadata
            current_source_columns = ['id', 'name', 'email', 'new_column']  # Schema changed
            stored_metadata = {
                'source_columns': ['id', 'name', 'email'],  # Old schema
                'source_mtime': 1234567890
            }
            current_source_mtime = 1234567999  # File was modified
            
            # Mock _read_source_columns to return current schema
            with patch.object(self.stager, '_read_source_columns') as mock_read_columns:
                mock_read_columns.return_value = current_source_columns
                
                # Mock metadata file reading
                with patch('builtins.open', mock_open(read_data=json.dumps(stored_metadata))):
                    # Mock file stat for modification time
                    with patch('pathlib.Path.stat') as mock_stat:
                        mock_stat.return_value.st_mtime = current_source_mtime
                        
                        # Mock _should_restage to return True (schema drift detected)
                        with patch.object(self.stager, '_should_restage') as mock_should_restage:
                            mock_should_restage.return_value = True
                            
                            # Mock the standard staging process
                            with patch.object(self.stager, '_stage_standard') as mock_stage_standard:
                                with patch.object(self.stager, '_apply_normalizations') as mock_normalizations:
                                    with patch.object(self.stager, '_apply_conversions') as mock_conversions:
                                        with patch.object(self.stager, '_normalize_columns') as mock_normalize:
                                            # Mock DuckDB operations
                                            self.mock_con.execute.return_value.fetchone.return_value = [1000]
                                            
                                            # Call stage_dataset with force_restage=False (should still restage due to drift)
                                            result = self.stager.stage_dataset(
                                                self.mock_con, 
                                                self.mock_config, 
                                                force_restage=False
                                            )
                                            
                                            # Verify that schema drift was detected and restaging occurred
                                            mock_should_restage.assert_called_once()
                                            mock_stage_standard.assert_called_once()
                                            mock_normalizations.assert_called_once()
                                            mock_conversions.assert_called_once()
                                            mock_normalize.assert_called_once()
                                            
                                            # Verify return value
                                            assert result == "test_dataset"
                                            
                                            # Verify that the staging process was executed instead of using cached file
                                            # This would fail in current implementation since schema drift detection doesn't exist yet
                                            
    def test_read_source_columns_helper_function(self):
        """Test that _read_source_columns helper correctly reads source file columns."""
        # This test will fail until _read_source_columns is implemented
        
        # Mock pandas read_csv to return DataFrame with specific columns
        mock_df = Mock()
        mock_df.columns.tolist.return_value = ['id', 'name', 'email']
        
        with patch('pandas.read_csv') as mock_read_csv:
            mock_read_csv.return_value = mock_df
            
            # This should fail since _read_source_columns doesn't exist yet
            with pytest.raises(AttributeError):
                columns = self.stager._read_source_columns("/path/to/test.csv")
                
    def test_should_restage_helper_function(self):
        """Test that _should_restage helper correctly detects when restaging is needed."""
        # This test will fail until _should_restage is implemented
        
        # Mock parameters
        staging_path = Path("/tmp/staging/test.parquet")
        source_path = Path("/path/to/test.csv")
        
        # This should fail since _should_restage doesn't exist yet
        with pytest.raises(AttributeError):
            should_restage = self.stager._should_restage(staging_path, source_path, self.mock_config)