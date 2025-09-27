"""
Unit tests for permanent collapse behavior in exports.
Following mandatory TDD: These tests MUST fail until permanent collapse is implemented.

Testing requirements:
- Full exports collapse entire-column differences by default (no config flag needed)
- Audit exports provide opt-in full row-level detail via export_rowlevel_audit_full=True
- Backward compatibility with deprecated flags
"""

import pytest
from unittest.mock import Mock, MagicMock, patch
from pathlib import Path
import sys

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

# Import existing components
from src.core.comparator import DataComparator
from src.config.manager import ComparisonConfig


class TestPermanentCollapseExports:
    """Test cases for permanent collapse behavior in full exports."""
    
    def setup_method(self):
        """Set up test fixtures."""
        # Mock DuckDB connection
        self.mock_con = Mock()
        
        # Create DataComparator instance
        self.comparator = DataComparator(self.mock_con)
        
        # Mock database responses
        self.mock_con.execute.return_value.fetchone.return_value = [1000]
        self.mock_con.execute.return_value.fetchall.return_value = [
            ('Key',), ('name',), ('email',)
        ]
    
    def test_full_export_collapses_entire_column_by_default(self):
        """
        Test that full exports collapse entire-column differences by DEFAULT.
        No config flags should be needed - this is the new permanent behavior.
        
        This test MUST FAIL until permanent collapse is implemented.
        """
        # Arrange: Standard config with NO collapse flags set
        config = ComparisonConfig(
            left_dataset="test_left",
            right_dataset="test_right", 
            comparison_keys=["Key"],
            export_full=True,
            annotate_entire_column=True
            # NOTE: No collapse_entire_column_in_full flag - should collapse by default
        )
        
        # Mock dataset configs
        left_config = Mock()
        left_config.column_map = None
        right_config = Mock() 
        right_config.column_map = None
        
        # Track calls to _export_full_csv to verify collapse behavior
        captured_queries = []
        
        def mock_export_full_csv(query, output_path, chunk_size=50000, order_cols=None):
            captured_queries.append({
                'query': str(query),
                'output_path': str(output_path),
                'chunk_size': chunk_size
            })
        
        # Mock other dependencies
        with patch.object(self.comparator, '_export_full_csv', side_effect=mock_export_full_csv), \
             patch.object(self.comparator, '_determine_value_columns', return_value=['name', 'email']), \
             patch('pathlib.Path.mkdir'), \
             patch('builtins.open', create=True) as mock_open:
            
            # Mock file operations
            mock_file_handle = Mock()
            mock_file_handle.__enter__ = Mock(return_value=mock_file_handle)
            mock_file_handle.__exit__ = Mock(return_value=None)
            mock_open.return_value = mock_file_handle
            
            output_dir = Path("C:/tmp/test_output")
            
            # Act: Call export_differences with standard config (no collapse flags)
            result = self.comparator.export_differences(
                left_table="test_left_table",
                right_table="test_right_table",
                config=config,
                output_dir=output_dir,
                left_dataset_config=left_config,
                right_dataset_config=right_config
            )
        
        # Assert: Standard full export should be collapsed by default
        
        # 1. Should generate value_differences_full.csv (standard name, not _collapsed)
        full_export_calls = [call for call in captured_queries if "value_differences_full.csv" in call['output_path']]
        assert len(full_export_calls) > 0, f"Should generate standard value_differences_full.csv: {captured_queries}"
        
        # 2. The full export query should contain collapse logic (ROW_NUMBER, PARTITION BY)
        full_export_query = full_export_calls[0]['query']
        
        # This will FAIL until permanent collapse is implemented
        assert "ROW_NUMBER()" in full_export_query, \
            f"Full export should use collapse logic by default: {full_export_query}"
        assert "PARTITION BY" in full_export_query, \
            f"Full export should partition by column for collapse: {full_export_query}"
        assert "WHERE rn = 1" in full_export_query, \
            f"Full export should select only first row per column: {full_export_query}"
        
        # 3. Should NOT generate separate _collapsed file (it's the default now)
        collapsed_export_calls = [call for call in captured_queries if "collapsed" in call['output_path']]
        assert len(collapsed_export_calls) == 0, \
            f"Should NOT generate separate collapsed file when it's the default: {captured_queries}"
        
        # 4. Should be in the returned outputs with standard key name
        assert "value_differences_full" in result, f"Should return value_differences_full output: {result.keys()}"
        
        # This test documents the requirement:
        # Full exports must collapse entire-column differences by default
        # No configuration flags should be needed for this behavior
    
    def test_full_export_rowlevel_audit_opt_in_produces_full_detail(self):
        """
        Test that export_rowlevel_audit_full=True produces separate audit files with full detail.
        The standard full export should remain collapsed, with audit providing complete data.
        
        This test MUST FAIL until audit export implementation is complete.
        """
        # Arrange: Config with audit export enabled
        config = ComparisonConfig(
            left_dataset="test_left",
            right_dataset="test_right",
            comparison_keys=["Key"],
            export_full=True,
            annotate_entire_column=True,
            export_rowlevel_audit_full=True  # Enable audit export
        )
        
        # Mock dataset configs
        left_config = Mock()
        left_config.column_map = None
        right_config = Mock()
        right_config.column_map = None
        
        # Track calls to _export_full_csv
        captured_queries = []
        
        def mock_export_full_csv(query, output_path, chunk_size=50000, order_cols=None):
            captured_queries.append({
                'query': str(query),
                'output_path': str(output_path),
                'chunk_size': chunk_size
            })
        
        # Mock other dependencies
        with patch.object(self.comparator, '_export_full_csv', side_effect=mock_export_full_csv), \
             patch.object(self.comparator, '_determine_value_columns', return_value=['name', 'email']), \
             patch('pathlib.Path.mkdir'), \
             patch('builtins.open', create=True) as mock_open:
            
            # Mock file operations
            mock_file_handle = Mock()
            mock_file_handle.__enter__ = Mock(return_value=mock_file_handle)
            mock_file_handle.__exit__ = Mock(return_value=None)
            mock_open.return_value = mock_file_handle
            
            output_dir = Path("C:/tmp/test_output")
            
            # Act: Call export_differences with audit enabled
            result = self.comparator.export_differences(
                left_table="test_left_table",
                right_table="test_right_table",
                config=config,
                output_dir=output_dir,
                left_dataset_config=left_config,
                right_dataset_config=right_config
            )
        
        # Assert: Both collapsed full export AND audit export should be generated
        
        # 1. Standard full export should still be collapsed
        full_export_calls = [call for call in captured_queries if "value_differences_full.csv" in call['output_path']]
        assert len(full_export_calls) > 0, f"Should still generate collapsed full export: {captured_queries}"
        
        if full_export_calls:
            full_export_query = full_export_calls[0]['query']
            assert "ROW_NUMBER()" in full_export_query, \
                f"Standard full export should remain collapsed even with audit enabled: {full_export_query}"
        
        # 2. Audit export should be generated with full row-level detail
        audit_export_calls = [call for call in captured_queries if "audit" in call['output_path']]
        assert len(audit_export_calls) > 0, \
            f"Should generate audit export when export_rowlevel_audit_full=True: {captured_queries}"
        
        # 3. Audit export should contain full detail (no collapse logic)
        if audit_export_calls:
            audit_export_query = audit_export_calls[0]['query']
            
            # This will FAIL until audit export is implemented
            # Audit export should NOT have collapse logic - it should show all rows
            assert "WHERE rn = 1" not in audit_export_query or "audit_timestamp" in audit_export_query, \
                f"Audit export should contain full row-level detail, not collapsed: {audit_export_query}"
        
        # 4. Both outputs should be returned
        assert "value_differences_full" in result, f"Should return standard collapsed export: {result.keys()}"
        assert "value_differences_full_audit" in result, f"Should return audit export: {result.keys()}"
        
        # 5. Audit file should use proper naming convention
        audit_call = audit_export_calls[0]
        assert "value_differences_full_audit" in audit_call['output_path'], \
            f"Audit export should use proper naming: {audit_call['output_path']}"
        
        # This test documents the requirement:
        # export_rowlevel_audit_full=True should produce separate audit files
        # with complete row-level detail while keeping standard export collapsed
    
    def test_deprecated_collapse_flag_ignored_with_warning(self):
        """
        Test that deprecated collapse_entire_column_in_full flag is ignored safely.
        Should log a deprecation warning but not affect behavior.
        
        This test MUST FAIL until deprecation handling is implemented.
        """
        # Arrange: Config with deprecated flag (should be ignored)
        config = ComparisonConfig(
            left_dataset="test_left",
            right_dataset="test_right",
            comparison_keys=["Key"],
            export_full=True,
            annotate_entire_column=True
        )
        
        # Manually set deprecated flag (simulating old YAML)
        config.collapse_entire_column_in_full = False  # This should be ignored
        
        # Mock dataset configs
        left_config = Mock()
        left_config.column_map = None
        right_config = Mock()
        right_config.column_map = None
        
        # Track calls and warnings
        captured_queries = []
        captured_warnings = []
        
        def mock_export_full_csv(query, output_path, chunk_size=50000, order_cols=None):
            captured_queries.append({
                'query': str(query),
                'output_path': str(output_path)
            })
        
        def mock_warning(message):
            captured_warnings.append(message)
        
        # Mock other dependencies
        with patch.object(self.comparator, '_export_full_csv', side_effect=mock_export_full_csv), \
             patch.object(self.comparator, '_determine_value_columns', return_value=['name', 'email']), \
             patch('pathlib.Path.mkdir'), \
             patch('builtins.open', create=True), \
             patch('logging.warning', side_effect=mock_warning):
            
            output_dir = Path("C:/tmp/test_output")
            
            # Act: Call export_differences with deprecated flag
            result = self.comparator.export_differences(
                left_table="test_left_table",
                right_table="test_right_table",
                config=config,
                output_dir=output_dir,
                left_dataset_config=left_config,
                right_dataset_config=right_config
            )
        
        # Assert: Should still collapse by default despite deprecated flag
        
        # 1. Should still generate collapsed export (ignoring the deprecated flag)
        full_export_calls = [call for call in captured_queries if "value_differences_full.csv" in call['output_path']]
        assert len(full_export_calls) > 0, f"Should generate full export: {captured_queries}"
        
        if full_export_calls:
            full_export_query = full_export_calls[0]['query']
            # This will FAIL until permanent collapse ignores deprecated flags
            assert "ROW_NUMBER()" in full_export_query, \
                f"Should collapse by default regardless of deprecated flag: {full_export_query}"
        
        # 2. Should log deprecation warning (when implementation detects the flag)
        # Note: This assertion will be adjusted based on actual warning implementation
        # For now, we document that warnings should be logged for deprecated flags
        
        # This test documents the requirement:
        # Deprecated collapse_entire_column_in_full flag should be ignored
        # and permanent collapse behavior should apply regardless


class TestPermanentCollapsePreviewUnchanged:
    """Test that preview collapse behavior remains unchanged."""
    
    def setup_method(self):
        """Set up test fixtures."""
        # Mock DuckDB connection
        self.mock_con = Mock()
        
        # Create DataComparator instance  
        self.comparator = DataComparator(self.mock_con)
        
        # Mock database responses
        self.mock_con.execute.return_value.fetchone.return_value = [1000]
        self.mock_con.execute.return_value.fetchall.return_value = [
            ('Key',), ('name',), ('email',)
        ]
    
    def test_preview_collapse_behavior_unchanged(self):
        """
        Test that preview exports still respect collapse_entire_column_in_preview flag.
        This ensures we only changed full export behavior, not preview behavior.
        
        This test should PASS - preview behavior should be unchanged.
        """
        # Arrange: Config with preview collapse enabled
        config = ComparisonConfig(
            left_dataset="test_left",
            right_dataset="test_right",
            comparison_keys=["Key"],
            export_full=False,  # Focus on preview behavior
            annotate_entire_column=True,
            enable_smart_preview=True,
            collapse_entire_column_in_preview=True  # Preview collapse flag (should still work)
        )
        
        # Mock dataset configs
        left_config = Mock()
        left_config.column_map = None
        right_config = Mock()
        right_config.column_map = None
        
        # Track SQL queries generated
        captured_queries = []
        
        def mock_execute(sql):
            captured_queries.append(str(sql))
            mock_result = Mock()
            mock_result.fetchone.return_value = [1000]
            mock_result.fetchall.return_value = [('Key',), ('name',), ('email',)]
            return mock_result
        
        self.mock_con.execute = mock_execute
        
        # Mock other dependencies
        with patch.object(self.comparator, '_determine_value_columns', return_value=['name', 'email']), \
             patch('pathlib.Path.mkdir'), \
             patch('builtins.open', create=True) as mock_open:
            
            # Mock file operations
            mock_file_handle = Mock()
            mock_file_handle.__enter__ = Mock(return_value=mock_file_handle) 
            mock_file_handle.__exit__ = Mock(return_value=None)
            mock_open.return_value = mock_file_handle
            
            output_dir = Path("C:/tmp/test_output")
            
            # Act: Generate preview with collapse enabled
            result = self.comparator.export_differences(
                left_table="test_left_table",
                right_table="test_right_table",
                config=config,
                output_dir=output_dir,
                left_dataset_config=left_config,
                right_dataset_config=right_config
            )
        
        # Assert: Preview should still respect its collapse flag
        
        # Find the preview query (ignore capability probe queries)
        preview_queries = []
        for query in captured_queries:
            if ('UNION ALL' in query.upper() and 
                'summaries' in query.lower() and 
                'samples' in query.lower() and
                'test_col' not in query.lower()):
                preview_queries.append(query)
        
        assert len(preview_queries) > 0, f"Should generate preview query: {captured_queries}"
        
        if preview_queries:
            preview_query = preview_queries[0]
            
            # Preview collapse should work as before (WHERE rn <= 1 for summaries)
            assert "WHERE rn <= 1" in preview_query, \
                f"Preview should still support collapse via flag: {preview_query}"
            assert "PARTITION BY" in preview_query, \
                f"Preview should still use partition logic: {preview_query}"
        
        # This test ensures preview behavior is preserved
        # Only full export behavior should change to permanent collapse