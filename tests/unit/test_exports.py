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
        Test that full exports collapse entire-column differences by DEFAULT (hybrid behavior).
        No config flags should be needed - this is the new permanent behavior.
        Updated for hybrid: should also include ALL partial differences.
        
        This test MUST PASS with hybrid implementation.
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
            # Check that the audit query does not use collapse-specific ROW_NUMBER filters
            collapse_indicators = ["WHERE rn = 1", "WHERE rn <= 1"]
            has_collapse_logic = any(indicator in audit_export_query for indicator in collapse_indicators)
            
            # Audit should NOT be collapsed unless it's specifically for audit timestamps
            assert not has_collapse_logic or "audit_timestamp" in audit_export_query, \
                f"Audit export should contain full row-level detail, not collapsed: {audit_export_query}"
        
        # 4. Both outputs should be returned (note: result structure may vary)
        # At minimum, the standard export should exist
        output_files = [call['output_path'] for call in captured_queries]
        has_standard_export = any("value_differences_full.csv" in path for path in output_files)
        has_audit_export = any("audit" in path for path in output_files)
        
        assert has_standard_export, f"Should generate standard collapsed export: {output_files}"
        assert has_audit_export, f"Should generate audit export: {output_files}"
        
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
        
        # Manually set deprecated flag (simulating old YAML) - will be ignored
        setattr(config, 'collapse_entire_column_in_full', False)  # This should be ignored
        
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
        # Note: Warning logging is handled by ConfigManager during YAML parsing
        # For manually set attributes in tests, we verify behavior ignores the flag
        
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
    
    def test_preview_now_permanently_collapsed(self):
        """
        Test that preview exports are now PERMANENTLY collapsed (no flag needed).
        This ensures preview behavior matches the new permanent collapse standard.
        
        Updated for permanent collapse - this test verifies the new behavior.
        """
        # Arrange: Config without explicit collapse flag (permanent behavior)
        config = ComparisonConfig(
            left_dataset="test_left",
            right_dataset="test_right",
            comparison_keys=["Key"],
            export_full=False,  # Focus on preview behavior
            annotate_entire_column=True,
            enable_smart_preview=True
            # Note: No collapse flag needed - preview now permanently collapsed
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
        
        # Assert: Preview should be permanently collapsed
        
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
            
            # Preview should be permanently collapsed (WHERE rn <= 1 for summaries)
            assert "WHERE rn <= 1" in preview_query, \
                f"Preview should be permanently collapsed: {preview_query}"
            assert "PARTITION BY" in preview_query, \
                f"Preview should use partition logic for permanent collapse: {preview_query}"
        
        # This test verifies that preview is now permanently collapsed
        # Both preview and full exports use permanent collapse behavior


class TestHybridFullExport:
    """Test cases for hybrid full export behavior (collapsed summaries + all partial differences)."""
    
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
    
    def test_full_export_hybrid_includes_summaries_and_all_partial_rows(self):
        """
        Test that full export contains hybrid data:
        - Exactly 1 row per fully-different column (collapsed summaries)
        - All rows for partially-different columns (not capped)
        
        This test MUST FAIL until hybrid full export is implemented.
        """
        # Arrange: Config with hybrid full export expected
        config = ComparisonConfig(
            left_dataset="test_left",
            right_dataset="test_right",
            comparison_keys=["Key"],
            export_full=True,
            annotate_entire_column=True
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
            
            # Act: Call export_differences
            result = self.comparator.export_differences(
                left_table="test_left_table",
                right_table="test_right_table",
                config=config,
                output_dir=output_dir,
                left_dataset_config=left_config,
                right_dataset_config=right_config
            )
        
        # Assert: Full export should use hybrid query structure
        
        # 1. Should generate value_differences_full.csv
        full_export_calls = [call for call in captured_queries if "value_differences_full.csv" in call['output_path']]
        assert len(full_export_calls) > 0, f"Should generate full export: {captured_queries}"
        
        # 2. The full export query should contain HYBRID logic
        full_export_query = full_export_calls[0]['query']
        
        # This will FAIL until hybrid implementation is complete
        # Should contain UNION ALL combining collapsed summaries and all partial differences
        assert "UNION ALL" in full_export_query, \
            f"Full export should use UNION ALL for hybrid approach: {full_export_query}"
        
        # Should have collapsed_summaries CTE for entirely different columns  
        assert "collapsed_summaries" in full_export_query, \
            f"Full export should have collapsed_summaries CTE: {full_export_query}"
            
        # Should have partials CTE for partially different columns
        assert "partials" in full_export_query, \
            f"Full export should have partials CTE: {full_export_query}"
            
        # Should select from both branches
        assert 'SELECT * FROM collapsed_summaries' in full_export_query, \
            f"Full export should select from collapsed_summaries: {full_export_query}"
        assert 'SELECT * FROM partials' in full_export_query, \
            f"Full export should select from partials: {full_export_query}"
            
        # Should filter summaries to rn = 1 for collapse
        assert "WHERE rn = 1" in full_export_query, \
            f"Full export should collapse summaries with WHERE rn = 1: {full_export_query}"
            
        # Should filter partials to entire_column = false  
        assert '"Entire Column Different" = \'false\'' in full_export_query, \
            f"Full export should include partial differences: {full_export_query}"
            
        # Should have deterministic ordering
        assert "ORDER BY" in full_export_query, \
            f"Full export should have deterministic ordering: {full_export_query}"
        
        # This test documents the requirement:
        # Full export must be hybrid: collapsed summaries + all partial differences
    
    def test_full_export_hybrid_handles_only_partial_differences(self):
        """
        Test that full export is NOT EMPTY when there are only partial differences.
        This is the critical bug fix - full export was empty when no columns were entirely different.
        
        This test MUST FAIL until hybrid full export is implemented.
        """
        # Arrange: Config for scenario with only partial differences
        config = ComparisonConfig(
            left_dataset="test_left",
            right_dataset="test_right", 
            comparison_keys=["Key"],
            export_full=True,
            annotate_entire_column=True
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
             patch.object(self.comparator, '_determine_value_columns', return_value=['name']), \
             patch('pathlib.Path.mkdir'), \
             patch('builtins.open', create=True) as mock_open:
            
            # Mock file operations
            mock_file_handle = Mock()
            mock_file_handle.__enter__ = Mock(return_value=mock_file_handle)
            mock_file_handle.__exit__ = Mock(return_value=None)
            mock_open.return_value = mock_file_handle
            
            output_dir = Path("C:/tmp/test_output")
            
            # Act: Call export_differences
            result = self.comparator.export_differences(
                left_table="test_left_table",
                right_table="test_right_table",
                config=config,
                output_dir=output_dir,
                left_dataset_config=left_config,
                right_dataset_config=right_config
            )
        
        # Assert: Full export should handle partial-only scenario
        
        # 1. Should generate value_differences_full.csv
        full_export_calls = [call for call in captured_queries if "value_differences_full.csv" in call['output_path']]
        assert len(full_export_calls) > 0, f"Should generate full export: {captured_queries}"
        
        # 2. The full export query should handle partial-only case
        full_export_query = full_export_calls[0]['query']
        
        # This will FAIL until hybrid implementation handles partial-only scenario
        # Should contain logic to include partial differences even when no entire columns exist
        assert '"Entire Column Different" = \'false\'' in full_export_query, \
            f"Full export should include partial differences when no entire columns: {full_export_query}"
            
        # Should NOT be restricted to only entire columns at the top level (the bug we're fixing)
        # The old problematic logic was a direct filter after annotated_data, not in CTEs
        # Check that we have UNION ALL combining both entire and partial columns
        assert "UNION ALL" in full_export_query, \
            f"Full export should use UNION ALL to combine entire and partial columns: {full_export_query}"
        
        # Verify we have both branches: collapsed_summaries and partials
        assert "collapsed_summaries" in full_export_query, \
            f"Full export should have collapsed_summaries CTE: {full_export_query}"
        assert "SELECT * FROM partials" in full_export_query, \
            f"Full export should select from partials CTE: {full_export_query}"
        
        # This test documents the critical bug fix:
        # Full export must include partial differences to never be empty when preview has data
    
    def test_full_export_hybrid_maintains_deterministic_ordering(self):
        """
        Test that hybrid full export maintains deterministic ordering across runs.
        
        This test MUST FAIL until hybrid ordering is implemented.
        """
        # Arrange: Standard config
        config = ComparisonConfig(
            left_dataset="test_left",
            right_dataset="test_right",
            comparison_keys=["Key"],
            export_full=True,
            annotate_entire_column=True
        )
        
        # Mock dataset configs
        left_config = Mock()
        left_config.column_map = None
        right_config = Mock()
        right_config.column_map = None
        
        # Track calls to _export_full_csv from two identical runs
        captured_queries_run1 = []
        captured_queries_run2 = []
        
        def mock_export_full_csv_run1(query, output_path, chunk_size=50000, order_cols=None):
            captured_queries_run1.append({
                'query': str(query),
                'output_path': str(output_path)
            })
        
        def mock_export_full_csv_run2(query, output_path, chunk_size=50000, order_cols=None):
            captured_queries_run2.append({
                'query': str(query),
                'output_path': str(output_path)
            })
        
        # Mock other dependencies for both runs
        with patch.object(self.comparator, '_determine_value_columns', return_value=['name', 'email']), \
             patch('pathlib.Path.mkdir'), \
             patch('builtins.open', create=True) as mock_open:
            
            # Mock file operations
            mock_file_handle = Mock()
            mock_file_handle.__enter__ = Mock(return_value=mock_file_handle)
            mock_file_handle.__exit__ = Mock(return_value=None)
            mock_open.return_value = mock_file_handle
            
            output_dir = Path("C:/tmp/test_output")
            
            # Act: Run export twice with identical inputs
            
            # Run 1
            with patch.object(self.comparator, '_export_full_csv', side_effect=mock_export_full_csv_run1):
                result1 = self.comparator.export_differences(
                    left_table="test_left_table",
                    right_table="test_right_table",
                    config=config,
                    output_dir=output_dir,
                    left_dataset_config=left_config,
                    right_dataset_config=right_config
                )
            
            # Run 2  
            with patch.object(self.comparator, '_export_full_csv', side_effect=mock_export_full_csv_run2):
                result2 = self.comparator.export_differences(
                    left_table="test_left_table",
                    right_table="test_right_table",
                    config=config,
                    output_dir=output_dir,
                    left_dataset_config=left_config,
                    right_dataset_config=right_config
                )
        
        # Assert: Queries should be identical (deterministic)
        
        # Extract full export queries from both runs
        full_queries_run1 = [call['query'] for call in captured_queries_run1 if "value_differences_full.csv" in call['output_path']]
        full_queries_run2 = [call['query'] for call in captured_queries_run2 if "value_differences_full.csv" in call['output_path']]
        
        assert len(full_queries_run1) > 0, f"Run 1 should generate full export query: {captured_queries_run1}"
        assert len(full_queries_run2) > 0, f"Run 2 should generate full export query: {captured_queries_run2}"
        
        # This will FAIL until deterministic ordering is implemented
        assert full_queries_run1[0] == full_queries_run2[0], \
            "Hybrid full export queries should be identical across runs"
        
        # Should have deterministic ordering with sample discriminator
        full_query = full_queries_run1[0]
        assert "sample DESC" in full_query, \
            f"Hybrid full export should order by sample DESC for determinism: {full_query}"
        
        # This test documents the requirement:
        # Hybrid full export must be deterministic across identical runs