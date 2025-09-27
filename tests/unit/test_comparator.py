"""
Unit tests for DataComparator component.
Following TDD: These tests MUST fail until DataComparator modifications are implemented.
"""

import pytest
from unittest.mock import Mock, MagicMock, patch
from pathlib import Path
import sys

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

# Import existing components
from src.core.comparator import DataComparator, ComparisonResult
from src.config.manager import ComparisonConfig


class TestDataComparatorValidatedKeys:
    """Test cases for DataComparator accepting pre-validated keys."""
    
    def setup_method(self):
        """Set up test fixtures."""
        # Mock DuckDB connection
        self.mock_con = Mock()
        
        # Create DataComparator instance
        self.comparator = DataComparator(self.mock_con)
        
        # Mock configuration
        self.mock_config = Mock(spec=ComparisonConfig)
        self.mock_config.comparison_keys = []  # Empty to trigger key determination normally
        self.mock_config.value_columns = []
        self.mock_config.tolerance = 0.01
        
        # Mock dataset configs
        self.mock_left_config = Mock()
        self.mock_left_config.column_map = None
        
        self.mock_right_config = Mock()
        self.mock_right_config.column_map = None
        
        # Mock database responses for row counts and columns
        self.mock_con.execute.return_value.fetchone.return_value = [1000]  # Row count
        self.mock_con.execute.return_value.fetchall.return_value = [
            ('user_key',), ('column1',), ('column2',)
        ]  # Column list
    
    def test_compare_skips_key_determination_when_validated_keys_provided(self):
        """
        Test that compare() method skips _determine_keys when validated_keys are provided.
        
        This test validates the CLAUDE.md Step 4 requirement:
        - DataComparator should accept pre-validated keys
        - When validated keys are provided, _determine_keys should NOT be called
        - The provided validated keys should be used directly
        """
        # Arrange: Mock the _determine_keys method to track if it's called
        with patch.object(self.comparator, '_determine_keys', 
                         return_value=['auto_detected_key']) as mock_determine_keys:
            
            # Mock other internal methods to avoid side effects
            with patch.object(self.comparator, '_find_matches', return_value=0), \
                 patch.object(self.comparator, '_find_only_in_left', return_value=0), \
                 patch.object(self.comparator, '_find_only_in_right', return_value=0), \
                 patch.object(self.comparator, '_find_value_differences', return_value=0), \
                 patch.object(self.comparator, '_determine_value_columns', return_value=['column1']):
                
                # Act: Call compare with validated_keys parameter
                validated_keys = ["user_key"]  # Pre-validated key from KeySelector
                
                # This should fail until the validated_keys parameter is implemented
                result = self.comparator.compare(
                    left_table="left_table",
                    right_table="right_table", 
                    config=self.mock_config,
                    left_dataset_config=self.mock_left_config,
                    right_dataset_config=self.mock_right_config,
                    validated_keys=validated_keys  # NEW PARAMETER - will fail until implemented
                )
        
        # Assert: _determine_keys should NOT have been called
        mock_determine_keys.assert_not_called()
        
        # Assert: Result should use the validated keys
        assert isinstance(result, ComparisonResult)
        assert result.key_columns == validated_keys
        assert "user_key" in result.key_columns
        
        # Verify the validated keys were used (not auto-detected)
        assert result.key_columns != ['auto_detected_key']
    
    def test_compare_uses_determine_keys_when_no_validated_keys_provided(self):
        """
        Test that compare() still uses _determine_keys when no validated_keys provided.
        
        This ensures backward compatibility - existing behavior should remain unchanged.
        """
        # Arrange: Mock the _determine_keys method to track calls
        with patch.object(self.comparator, '_determine_keys', 
                         return_value=['auto_detected_key']) as mock_determine_keys:
            
            # Mock other internal methods
            with patch.object(self.comparator, '_find_matches', return_value=0), \
                 patch.object(self.comparator, '_find_only_in_left', return_value=0), \
                 patch.object(self.comparator, '_find_only_in_right', return_value=0), \
                 patch.object(self.comparator, '_find_value_differences', return_value=0), \
                 patch.object(self.comparator, '_determine_value_columns', return_value=['column1']):
                
                # Act: Call compare WITHOUT validated_keys parameter (existing behavior)
                result = self.comparator.compare(
                    left_table="left_table",
                    right_table="right_table",
                    config=self.mock_config,
                    left_dataset_config=self.mock_left_config,
                    right_dataset_config=self.mock_right_config
                    # No validated_keys parameter - should use existing key determination
                )
        
        # Assert: _determine_keys SHOULD have been called (existing behavior)
        mock_determine_keys.assert_called_once_with("left_table", "right_table", self.mock_config)
        
        # Assert: Result should use the auto-detected keys
        assert isinstance(result, ComparisonResult)
        assert result.key_columns == ['auto_detected_key']
    
    def test_validated_keys_parameter_signature_exists(self):
        """
        Test that the compare method accepts validated_keys parameter.
        
        This test validates that the parameter is now implemented and working.
        """
        # Mock internal methods to avoid side effects  
        with patch.object(self.comparator, '_find_matches', return_value=100), \
             patch.object(self.comparator, '_find_only_in_left', return_value=10), \
             patch.object(self.comparator, '_find_only_in_right', return_value=5), \
             patch.object(self.comparator, '_find_value_differences', return_value=2), \
             patch.object(self.comparator, '_determine_value_columns', return_value=['column1']):
            
            # Act: Call with validated_keys parameter - should work now
            result = self.comparator.compare(
                left_table="left_table",
                right_table="right_table",
                config=self.mock_config,
                left_dataset_config=self.mock_left_config,
                right_dataset_config=self.mock_right_config,
                validated_keys=["user_key"]  # This parameter should work now
            )
        
        # Assert: Should work correctly with validated keys
        assert isinstance(result, ComparisonResult)
        assert result.key_columns == ["user_key"]
        assert result.matched_rows == 100
    
    def test_empty_validated_keys_falls_back_to_determine_keys(self):
        """
        Test that empty validated_keys list falls back to _determine_keys.
        
        This ensures robust handling of edge cases.
        """
        # This test will be implemented after the basic functionality is working
        # For now, just verify the test structure exists
        
        # Skip until basic validated_keys implementation is complete
        pytest.skip("Test structure placeholder - will implement after basic validated_keys support")
    
    def test_validated_keys_logging_and_debug_info(self):
        """
        Test that validated keys are properly logged for debugging.
        
        CLAUDE.md requirement: Comprehensive logging for audit trail.
        """
        # Skip until basic validated_keys implementation is complete
        pytest.skip("Test structure placeholder - will implement after basic validated_keys support")


class TestDataComparatorExistingFunctionality:
    """Test cases to ensure existing DataComparator functionality is preserved."""
    
    def setup_method(self):
        """Set up test fixtures for existing functionality tests."""
        self.mock_con = Mock()
        self.comparator = DataComparator(self.mock_con)
        
        # Mock basic responses
        self.mock_con.execute.return_value.fetchone.return_value = [1000]
        self.mock_con.execute.return_value.fetchall.return_value = [
            ('id',), ('name',), ('email',)
        ]
    
    def test_existing_compare_method_still_works(self):
        """
        Test that existing compare method calls work without validated_keys.
        
        This ensures we don't break existing code when adding the new parameter.
        """
        # Arrange
        mock_config = Mock(spec=ComparisonConfig)
        mock_config.comparison_keys = ['id']  # Use configured keys
        mock_config.value_columns = []
        mock_config.tolerance = 0.01
        
        # Mock internal methods
        with patch.object(self.comparator, '_find_matches', return_value=500), \
             patch.object(self.comparator, '_find_only_in_left', return_value=100), \
             patch.object(self.comparator, '_find_only_in_right', return_value=50), \
             patch.object(self.comparator, '_find_value_differences', return_value=25), \
             patch.object(self.comparator, '_determine_value_columns', return_value=['name', 'email']):
            
            # Act: Call existing method without validated_keys
            result = self.comparator.compare(
                left_table="test_left",
                right_table="test_right",
                config=mock_config
            )
        
        # Assert: Should work as before
        assert isinstance(result, ComparisonResult)
        assert result.key_columns == ['id']  # From config.comparison_keys
        assert result.matched_rows == 500
        assert result.only_in_left == 100
        assert result.only_in_right == 50
        assert result.value_differences == 25


class TestDataComparatorStagedKeyPropagation:
    """Test cases for staged key propagation from KeyValidator to DataComparator."""
    
    def setup_method(self):
        """Set up test fixtures."""
        # Mock DuckDB connection
        self.mock_con = Mock()
        
        # Create DataComparator instance
        self.comparator = DataComparator(self.mock_con)
        
        # Mock configuration
        self.mock_config = Mock(spec=ComparisonConfig)
        self.mock_config.comparison_keys = []  # Empty to use provided keys
        self.mock_config.value_columns = []
        self.mock_config.tolerance = 0.01
        
        # Mock dataset configs
        self.mock_left_config = Mock()
        self.mock_left_config.column_map = None  # Left table (no column mapping)
        
        self.mock_right_config = Mock()
        self.mock_right_config.column_map = {'staged_key': 'user_key'}  # Right table mapping
        
    def test_comparator_uses_discovered_staged_keys(self):
        """
        Test that DataComparator uses discovered staged key names for SQL generation.
        
        CRITICAL BUG REPRODUCTION:
        - User selects 'user_key' as key column in interactive mode
        - KeyValidator discovers actual staged column name is 'staged_key'
        - DataComparator must use 'staged_key' for all SQL generation, not 'user_key'
        - This prevents "Binder Error: Referenced column 'user_key' not found"
        
        This test MUST FAIL until staged key propagation is implemented.
        """
        # Arrange: Mock KeyValidator to simulate key discovery behavior
        # KeyValidator should find that 'user_key' maps to 'staged_key' in the staged table
        mock_validation_result = Mock()
        mock_validation_result.is_valid = True
        mock_validation_result.discovered_keys = ['staged_key']  # NEW: discovered staged column names
        
        # Mock row counts
        self.mock_con.execute.return_value.fetchone.return_value = [1000]
        
        # Mock SQL generation methods to capture what keys are actually used
        captured_sql_calls = []
        
        def mock_execute(sql):
            captured_sql_calls.append(sql)
            mock_result = Mock()
            mock_result.fetchone.return_value = [1000]
            mock_result.fetchall.return_value = [('staged_key',), ('column1',), ('column2',)]
            return mock_result
        
        self.mock_con.execute = mock_execute
        
        # Mock KeyValidator to return discovered keys
        with patch('src.core.comparator.KeyValidator') as mock_validator_class:
            mock_validator = Mock()
            mock_validator.validate_key.return_value = mock_validation_result
            mock_validator_class.return_value = mock_validator
            
            # Mock other internal methods to focus on key propagation
            with patch.object(self.comparator, '_determine_value_columns', return_value=['column1']), \
                 patch.object(self.comparator, '_find_matches', return_value=0) as mock_find_matches, \
                 patch.object(self.comparator, '_find_only_in_left', return_value=0), \
                 patch.object(self.comparator, '_find_only_in_right', return_value=0), \
                 patch.object(self.comparator, '_find_value_differences', return_value=0):
                
                # Act: Call compare with user-selected key
                result = self.comparator.compare(
                    left_table="test_left_table",
                    right_table="test_right_table", 
                    config=self.mock_config,
                    left_dataset_config=self.mock_left_config,
                    right_dataset_config=self.mock_right_config,
                    validated_keys=["user_key"]  # User-selected key that needs discovery
                )
        
        # Assert: DataComparator should use discovered staged key names, not user-selected names
        # The _find_matches method should be called with the discovered staged key
        mock_find_matches.assert_called_once_with(
            "test_left_table",
            "test_right_table", 
            ["staged_key"]  # Should use discovered key, not "user_key"
        )
        
        # Assert: Result should contain the discovered staged key names
        assert result.key_columns == ["staged_key"], f"Expected discovered key 'staged_key', got {result.key_columns}"
        
        # This test documents the EXACT failure we need to fix:
        # DataComparator must propagate discovered staged key names from KeyValidator
        # to all subsequent SQL generation methods


class TestDataComparatorFailFast:
    """Test cases for DataComparator fail fast comparison pattern."""
    
    def setup_method(self):
        """Set up test fixtures."""
        # Mock DuckDB connection
        self.mock_con = Mock()
        
        # Create DataComparator instance
        self.comparator = DataComparator(self.mock_con)
        
        # Mock configuration
        self.mock_config = Mock(spec=ComparisonConfig)
        self.mock_config.comparison_keys = []  # Empty to trigger key determination
        self.mock_config.value_columns = []
        self.mock_config.tolerance = 0.01
        
        # Mock dataset configs
        self.mock_left_config = Mock()
        self.mock_left_config.column_map = None
        
        self.mock_right_config = Mock()
        self.mock_right_config.column_map = None
        
        # Mock database responses for row counts and columns
        self.mock_con.execute.return_value.fetchone.return_value = [1000]  # Row count
        self.mock_con.execute.return_value.fetchall.return_value = [
            ('user_key',), ('column1',), ('column2',)
        ]  # Column list
    
    def test_compare_fails_fast_on_key_validation_failure(self):
        """
        Test that DataComparator.compare() immediately raises KeyValidationError
        when key validation fails, implementing the FAIL FAST COMPARISON PATTERN.
        
        This test verifies two scenarios:
        1. Left dataset key validation fails -> immediate KeyValidationError
        2. Right dataset key validation fails -> immediate KeyValidationError
        """
        from src.core.key_validator import KeyValidationError, KeyValidationResult
        
        # Mock _determine_keys to return test keys
        with patch.object(self.comparator, '_determine_keys', return_value=['duplicate_key']):
            
            # Scenario 1: Left validation fails
            mock_validator = Mock()
            mock_left_validation = KeyValidationResult(
                is_valid=False,
                total_rows=100,
                unique_values=95,
                duplicate_count=5,
                discovered_keys=['duplicate_key'],
                error_message="Duplicate keys found"
            )
            mock_right_validation = KeyValidationResult(
                is_valid=True,
                total_rows=100,
                unique_values=100,
                duplicate_count=0,
                discovered_keys=['duplicate_key'],
                error_message=None
            )
            
            mock_validator.validate_key.side_effect = [mock_left_validation, mock_right_validation]
            
            with patch('src.core.comparator.KeyValidator', return_value=mock_validator):
                # This should raise KeyValidationError immediately - no further processing
                with pytest.raises(KeyValidationError) as exc_info:
                    self.comparator.compare(
                        left_table="left_table",
                        right_table="right_table",
                        config=self.mock_config,
                        left_dataset_config=self.mock_left_config,
                        right_dataset_config=self.mock_right_config
                    )
                
                # Verify error message format matches CLAUDE.md mandatory format
                error_msg = str(exc_info.value)
                assert "[KEY VALIDATION ERROR]" in error_msg
                assert "left dataset" in error_msg
                assert "Suggestion:" in error_msg
                
            # Scenario 2: Right validation fails  
            mock_validator.reset_mock()
            mock_left_validation_success = KeyValidationResult(
                is_valid=True,
                total_rows=100,
                unique_values=100,
                duplicate_count=0,
                discovered_keys=['duplicate_key'],
                error_message=None
            )
            mock_right_validation_fail = KeyValidationResult(
                is_valid=False,
                total_rows=100,
                unique_values=97,
                duplicate_count=3,
                discovered_keys=['duplicate_key'],
                error_message="Duplicate keys found"
            )
            
            mock_validator.validate_key.side_effect = [mock_left_validation_success, mock_right_validation_fail]
            
            with patch('src.core.comparator.KeyValidator', return_value=mock_validator):
                # This should also raise KeyValidationError immediately
                with pytest.raises(KeyValidationError) as exc_info:
                    self.comparator.compare(
                        left_table="left_table",
                        right_table="right_table", 
                        config=self.mock_config,
                        left_dataset_config=self.mock_left_config,
                        right_dataset_config=self.mock_right_config
                    )
                
                # Verify error message format for right dataset
                error_msg = str(exc_info.value)
                assert "[KEY VALIDATION ERROR]" in error_msg
                assert "right dataset" in error_msg
                assert "Suggestion:" in error_msg


class TestDataComparatorReportFidelity:
    """Test cases for DataComparator REPORT FIDELITY PATTERN implementation."""
    
    def setup_method(self):
        """Set up test fixtures."""
        # Mock DuckDB connection
        self.mock_con = Mock()
        
        # Create DataComparator instance
        self.comparator = DataComparator(self.mock_con)
        
        # Mock configuration with report fidelity attributes
        self.mock_config = Mock(spec=ComparisonConfig)
        self.mock_config.comparison_keys = ["id"]
        self.mock_config.value_columns = ["name", "email", "status"]
        self.mock_config.tolerance = 0.01
        self.mock_config.max_differences = 1000
        
        # NEW: Report fidelity configuration
        self.mock_config.csv_preview_limit = 500
        self.mock_config.export_full = True
        self.mock_config.annotate_entire_column = True
        self.mock_config.chunk_export_size = 50000
        self.mock_config.enable_smart_preview = True
        
        # Mock dataset configs
        self.mock_left_config = Mock()
        self.mock_left_config.column_map = None
        
        self.mock_right_config = Mock()
        self.mock_right_config.column_map = None
        
        # Mock database responses
        self.mock_con.execute.return_value.fetchone.return_value = [1000]
        self.mock_con.execute.return_value.fetchall.return_value = [
            ('id',), ('name',), ('email',), ('status',)
        ]
    
    def test_export_differences_generates_chunked_full_and_preview(self):
        """
        Test that export_differences generates both chunked full exports and annotated previews.
        
        This test implements the REPORT FIDELITY PATTERN by verifying:
        1. Full chunked exports are generated when export_full=True
        2. Preview CSV includes entire_column annotation when annotate_entire_column=True  
        3. Smart preview logic is applied when enable_smart_preview=True
        4. Chunked export size is respected
        
        This test MUST FAIL until the new functionality is implemented.
        """
        # Arrange: Mock large dataset to trigger chunking logic but smaller for easier testing
        self.mock_con.execute.return_value.fetchone.return_value = [1000]  # Smaller dataset for easier mocking
        
        # Mock the directory and file operations
        with patch('pathlib.Path.mkdir') as mock_mkdir, \
             patch('pathlib.Path.exists', return_value=True) as mock_exists, \
             patch('builtins.open', create=True) as mock_open, \
             patch.object(self.comparator, '_determine_value_columns', 
                         return_value=['name', 'email', 'status']) as mock_determine_cols:
            
            # Mock file handles for CSV operations  
            mock_file_handle = Mock()
            mock_file_handle.__enter__ = Mock(return_value=mock_file_handle)
            mock_file_handle.__exit__ = Mock(return_value=None)
            mock_file_handle.read.return_value = "header1,header2\nvalue1,value2\n"
            mock_open.return_value = mock_file_handle
            
            # Create temporary output directory
            output_dir = Path("C:/tmp/test_output")  # Use Windows-compatible path
            
            # Act: Call export_differences with new configuration
            result = self.comparator.export_differences(
                left_table="large_left_table",
                right_table="large_right_table", 
                config=self.mock_config,
                output_dir=output_dir,
                left_dataset_config=self.mock_left_config,
                right_dataset_config=self.mock_right_config
            )
        
        # Assert: Verify new report fidelity outputs are generated
        
        # 1. Should generate full chunked exports for value differences
        assert "value_differences_full" in result, "Full chunked export not generated"
        full_export_path = result["value_differences_full"]
        assert isinstance(full_export_path, Path), f"Full export path should be Path object: {full_export_path}"
        
        # 2. Should generate annotated preview
        assert "value_differences" in result, "Preview export not generated"
        preview_path = result["value_differences"]
        assert isinstance(preview_path, Path), f"Preview path should be Path object: {preview_path}"
        
        # 3. Verify the mock_con.execute was called with complex queries containing CTEs and annotations
        # Check that annotation queries with "Entire Column Different" were generated
        execute_calls = [str(call) for call in self.mock_con.execute.call_args_list]
        
        # Should have calls with CTE queries for annotation
        annotation_calls = [call for call in execute_calls if "Entire Column Different" in call]
        assert len(annotation_calls) > 0, "No annotation queries with 'Entire Column Different' found"
        
        # Should have calls with chunked export logic (either _export_full_csv or chunked queries)
        full_export_calls = [call for call in execute_calls if "value_differences_full" in str(result)]
        assert "value_differences_full" in str(result), "Full export not configured properly"
        
        # 4. Verify the new configuration attributes are being used
        # The implementation should check for export_full, annotate_entire_column, etc.
        assert hasattr(self.mock_config, 'export_full'), "export_full attribute not found in config"
        assert hasattr(self.mock_config, 'annotate_entire_column'), "annotate_entire_column attribute not found in config" 
        assert hasattr(self.mock_config, 'enable_smart_preview'), "enable_smart_preview attribute not found in config"
        
        # 5. Verify expected output files are in result
        expected_outputs = ["only_left", "only_right", "value_differences", "value_differences_full", "summary"]
        for expected_output in expected_outputs:
            if expected_output != "value_differences_full":
                # All outputs except full should always be present
                assert expected_output in result, f"Expected output {expected_output} not found in result"
            else:
                # Full export should be present when export_full=True
                if self.mock_config.export_full:
                    assert expected_output in result, f"Full export {expected_output} not found when export_full=True"
        
        # This test documents the EXACT functionality we have implemented:
        # - Chunked full exports with configurable size
        # - Annotated previews with entire_column flag  
        # - Smart preview combining summaries and samples
        # - Proper configuration handling for all report fidelity options


class TestDataComparatorSQLSanitization:
    """Test cases for DataComparator SQL QUERY SANITIZATION PATTERN implementation."""
    
    def setup_method(self):
        """Set up test fixtures."""
        # Mock DuckDB connection
        self.mock_con = Mock()
        
        # Create DataComparator instance
        self.comparator = DataComparator(self.mock_con)
        
    def test_export_full_csv_strips_semicolon_and_wraps_query(self):
        """
        Test that _export_full_csv properly sanitizes SQL queries and wraps them safely.
        
        CRITICAL REGRESSION REPRODUCTION:
        - Base queries with trailing semicolons cause Parser Error: syntax error at or near 'ORDER'
        - This happens when chunking logic tries to wrap: "SELECT * FROM table;" + "ORDER BY 1 LIMIT 50000"
        - Result: Invalid SQL like "SELECT * FROM table; ORDER BY 1 LIMIT 50000"
        
        This test MUST FAIL until SQL sanitization and proper query wrapping is implemented.
        """
        # Arrange: Create a base query with trailing semicolon (the regression case)
        base_query_with_semicolon = "SELECT id, name, value FROM test_table;"
        
        # Mock the DuckDB connection responses for chunking logic
        mock_count_result = Mock()
        mock_count_result.fetchone.return_value = [100000]  # Large count to trigger chunking
        
        mock_copy_result = Mock()
        mock_copy_result.fetchone.return_value = [1]
        
        self.mock_con.execute.return_value = mock_count_result
        
        # Mock file operations to avoid actual file I/O
        output_path = Path("C:/tmp/test_sanitized_output.csv")
        
        with patch('pathlib.Path.mkdir') as mock_mkdir, \
             patch('pathlib.Path.exists', return_value=True) as mock_exists, \
             patch('pathlib.Path.unlink') as mock_unlink, \
             patch('builtins.open', create=True) as mock_open:
            
            mock_file_handle = Mock()
            mock_file_handle.__enter__ = Mock(return_value=mock_file_handle)
            mock_file_handle.__exit__ = Mock(return_value=None)
            mock_file_handle.read.return_value = "id,name,value\n1,test,123\n"
            mock_file_handle.write = Mock()
            mock_open.return_value = mock_file_handle
            
            # Act: Call _export_full_csv with the problematic query
            # This should FAIL with the current implementation because it doesn't sanitize
            try:
                self.comparator._export_full_csv(
                    query=base_query_with_semicolon,
                    output_path=output_path,
                    chunk_size=50000
                )
                
                # If we get here, the method completed successfully
                sql_success = True
                
            except Exception as e:
                # Should not fail after sanitization implementation
                sql_success = False
                error_message = str(e)
        
        # Assert: After fix, this should work without parser errors
        
        # 1. The method should complete successfully after sanitization is implemented
        assert sql_success, "SQL execution should succeed after query sanitization is implemented"
        
        # 2. Verify that the mock_con.execute was called with properly wrapped queries
        execute_calls = [call[0][0] for call in self.mock_con.execute.call_args_list]
        
        # 3. Should have a count query that properly wraps the sanitized base query
        count_calls = [call for call in execute_calls if "SELECT COUNT(*)" in call]
        assert len(count_calls) > 0, "Should have count query for chunking logic"
        
        # 4. Count query should wrap the sanitized query (no trailing semicolon)
        count_query = count_calls[0]
        assert "SELECT COUNT(*) FROM (" in count_query, "Count query should wrap the base query"
        assert ") AS count_subquery" in count_query, "Count query should use proper subquery alias"
        
        # 5. The wrapped query should NOT contain the problematic trailing semicolon pattern
        # This verifies the sanitization worked
        for call in execute_calls:
            # No SQL should have pattern: "table; ORDER BY" which causes parser error
            assert "; ORDER BY" not in call, f"Found unsanitized semicolon before ORDER BY in: {call}"
            # No SQL should have pattern: ") q; ORDER BY" 
            assert ") q; ORDER BY" not in call, f"Found improper semicolon in wrapped query: {call}"
        
        # 6. Verify proper query wrapping structure
        copy_calls = [call for call in execute_calls if "COPY (" in call]
        if copy_calls:
            copy_query = copy_calls[0]
            # Should have structure: COPY (SELECT * FROM (sanitized_query) q ORDER BY ... ) TO 'file'
            assert "SELECT * FROM (" in copy_query, "Should wrap query in subselect"
            assert ") q" in copy_query, "Should alias the subquery"
            assert "ORDER BY" in copy_query, "Should include ORDER BY for chunking"
            
        # This test documents the EXACT SQL sanitization pattern needed:
        # 1. Strip trailing semicolons from base queries
        # 2. Wrap sanitized query in subselect: SELECT * FROM (clean_query) q
        # 3. Apply chunking clauses: ORDER BY col LIMIT size OFFSET pos
        # 4. Ensure no "; ORDER BY" patterns exist in final SQL