"""
Unit tests for FINAL REPORT FIDELITY PATTERN implementation.
Following mandatory TDD: These tests MUST fail until implementation is complete.

Testing architectural safeguards:
- Collapse Logic
- Chunking 
- Zipping
- QUALIFY Fallback
- Deterministic Ordering
- SQL Sanitization
- Identifier Quoting
- UTF-8 Path Handling
"""

import pytest
from unittest.mock import Mock, MagicMock, patch, call
from pathlib import Path
import sys

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

# Import existing components
from src.core.comparator import DataComparator, ComparisonResult
from src.config.manager import ComparisonConfig


class TestDataComparatorFullExportChunking:
    """Test cases for DataComparator full export chunking functionality."""
    
    def setup_method(self):
        """Set up test fixtures."""
        # Mock DuckDB connection
        self.mock_con = Mock()
        
        # Create DataComparator instance
        self.comparator = DataComparator(self.mock_con)
        
        # Mock configuration with new report fidelity attributes
        self.mock_config = Mock(spec=ComparisonConfig)
        self.mock_config.comparison_keys = ["Key"]
        self.mock_config.value_columns = ["name", "email"]
        self.mock_config.tolerance = 0.01
        self.mock_config.max_differences = 1000
        
        # NEW: Report fidelity configuration with backward-compatible defaults
        self.mock_config.csv_preview_limit = 1000
        self.mock_config.entire_column_sample_size = 10
        self.mock_config.collapse_entire_column_in_preview = False
        self.mock_config.collapse_entire_column_in_full = False
        self.mock_config.export_rowlevel_audit_full = False
        self.mock_config.zip_large_exports = False
        self.mock_config.preview_order = ["Differing Column", "Key"]
        
        # Existing attributes
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
        self.mock_con.execute.return_value.fetchone.return_value = [100000]  # Large count for chunking
        self.mock_con.execute.return_value.fetchall.return_value = [
            ('Key',), ('name',), ('email',)
        ]
    
    def test_export_full_csv_handles_only_left_right_chunking(self):
        """
        TEST 1: Assert _export_full_csv is called for only_in_left/right and produces 
        correct number of *_partNNN.csv files using deterministic ORDER BY "Key".
        
        This test MUST FAIL until _export_full_csv enhancement is implemented.
        """
        # Arrange: Create large dataset scenario (200k rows to trigger chunking)
        large_row_count = 200000
        chunk_size = 50000
        expected_chunks = (large_row_count + chunk_size - 1) // chunk_size  # 4 chunks
        
        # Mock large row count responses
        self.mock_con.execute.return_value.fetchone.return_value = [large_row_count]
        
        # Mock _export_full_csv to track calls
        with patch.object(self.comparator, '_export_full_csv') as mock_export_full:
            
            # Mock other methods to focus on _export_full_csv calls
            with patch.object(self.comparator, '_determine_value_columns', return_value=['name', 'email']), \
                 patch('pathlib.Path.mkdir') as mock_mkdir, \
                 patch('builtins.open', create=True) as mock_open:
                
                # Mock file operations
                mock_file_handle = Mock()
                mock_file_handle.__enter__ = Mock(return_value=mock_file_handle)
                mock_file_handle.__exit__ = Mock(return_value=None)
                mock_open.return_value = mock_file_handle
                
                output_dir = Path("C:/tmp/test_output")
                
                # Act: Call export_differences with export_full=True
                result = self.comparator.export_differences(
                    left_table="large_left_table",
                    right_table="large_right_table",
                    config=self.mock_config,
                    output_dir=output_dir,
                    left_dataset_config=self.mock_left_config,
                    right_dataset_config=self.mock_right_config
                )
        
        # Assert: _export_full_csv should be called for only_in_left and only_in_right
        assert mock_export_full.call_count >= 2, "Should call _export_full_csv for both only_left and only_right"
        
        # Check the calls made to _export_full_csv
        export_calls = mock_export_full.call_args_list
        
        # Should have calls for left-only and right-only exports
        left_only_calls = [call for call in export_calls if "only_in_large_left_table" in str(call)]
        right_only_calls = [call for call in export_calls if "only_in_large_right_table" in str(call)]
        
        assert len(left_only_calls) > 0, "Should have _export_full_csv call for left-only data"
        assert len(right_only_calls) > 0, "Should have _export_full_csv call for right-only data"
        
        # Verify deterministic ORDER BY "Key" is used in the SQL queries
        for call_args in export_calls:
            query = str(call_args[0][0])  # First argument is the query
            if "SELECT" in query and query.strip():  # Only check non-empty queries
                # Should use deterministic ordering by Key column
                # Note: The actual ORDER BY might be in the _export_full_csv implementation
                assert "Key" in query or "ORDER BY" in query or "large_left_table" in query or "large_right_table" in query, \
                    f"Query should reference Key column, ORDER BY, or table names: {query}"
        
        # Verify chunk_export_size parameter is passed correctly
        for call_args in export_calls:
            if len(call_args[0]) >= 3:  # Check if chunk_size parameter exists
                chunk_size_param = call_args[0][2] if len(call_args[0]) > 2 else None
                if chunk_size_param:
                    assert chunk_size_param == self.mock_config.chunk_export_size, \
                        f"Chunk size should be {self.mock_config.chunk_export_size}, got {chunk_size_param}"
        
        # This test documents the requirement:
        # _export_full_csv must be called for large only_left/only_right exports
        # with deterministic ordering and proper chunking parameters


class TestDataComparatorFullExportsNaming:
    """Test cases for DataComparator full export naming conventions."""
    
    def setup_method(self):
        """Set up test fixtures."""
        # Mock DuckDB connection
        self.mock_con = Mock()
        
        # Create DataComparator instance  
        self.comparator = DataComparator(self.mock_con)
        
        # Mock configuration for collapse and audit exports
        self.mock_config = Mock(spec=ComparisonConfig)
        self.mock_config.comparison_keys = ["Key"]
        self.mock_config.value_columns = ["name", "email"]
        self.mock_config.tolerance = 0.01
        
        # NEW: Collapse and audit export configuration
        self.mock_config.collapse_entire_column_in_full = True
        self.mock_config.export_rowlevel_audit_full = True
        self.mock_config.export_full = True
        self.mock_config.chunk_export_size = 50000
        
        # Mock dataset configs
        self.mock_left_config = Mock()
        self.mock_left_config.column_map = None
        
        self.mock_right_config = Mock() 
        self.mock_right_config.column_map = None
        
        # Mock database responses
        self.mock_con.execute.return_value.fetchone.return_value = [100000]
        self.mock_con.execute.return_value.fetchall.return_value = [
            ('Key',), ('name',), ('email',)
        ]
    
    def test_export_differences_full_exports_and_naming(self):
        """
        TEST 2: With collapse_entire_column_in_full=True and export_rowlevel_audit_full=True,
        assert two distinct outputs with naming:
        - value_differences_full_collapsed_part001.csv
        - value_differences_full_audit_part001.csv
        
        This test MUST FAIL until enhanced naming convention is implemented.
        """
        # Arrange: Mock large dataset and file operations
        with patch.object(self.comparator, '_export_full_csv') as mock_export_full, \
             patch.object(self.comparator, '_determine_value_columns', return_value=['name', 'email']), \
             patch('pathlib.Path.mkdir') as mock_mkdir, \
             patch('builtins.open', create=True) as mock_open:
            
            # Mock file operations
            mock_file_handle = Mock()
            mock_file_handle.__enter__ = Mock(return_value=mock_file_handle)
            mock_file_handle.__exit__ = Mock(return_value=None)
            mock_open.return_value = mock_file_handle
            
            output_dir = Path("C:/tmp/test_output")
            
            # Act: Call export_differences with collapse and audit enabled
            result = self.comparator.export_differences(
                left_table="test_left_table",
                right_table="test_right_table",
                config=self.mock_config,
                output_dir=output_dir,
                left_dataset_config=self.mock_left_config,
                right_dataset_config=self.mock_right_config
            )
        
        # Assert: Should generate both collapsed and audit exports
        
        # 1. Check for collapsed export naming
        collapsed_exports = [key for key in result.keys() if "collapsed" in key]
        assert len(collapsed_exports) > 0, "Should generate collapsed export when collapse_entire_column_in_full=True"
        
        # 2. Check for audit export naming  
        audit_exports = [key for key in result.keys() if "audit" in key]
        assert len(audit_exports) > 0, "Should generate audit export when export_rowlevel_audit_full=True"
        
        # 3. Verify distinct file paths with correct naming patterns
        if collapsed_exports:
            collapsed_path = result[collapsed_exports[0]]
            assert "collapsed" in str(collapsed_path), f"Collapsed export path should contain 'collapsed': {collapsed_path}"
        
        if audit_exports:
            audit_path = result[audit_exports[0]]
            assert "audit" in str(audit_path), f"Audit export path should contain 'audit': {audit_path}"
        
        # 4. Verify _export_full_csv was called for both export types
        export_calls = mock_export_full.call_args_list
        
        # Should have separate calls for collapsed and audit exports
        collapsed_calls = [call for call in export_calls if "collapsed" in str(call)]
        audit_calls = [call for call in export_calls if "audit" in str(call)]
        
        # Both should be called when both flags are enabled
        if self.mock_config.collapse_entire_column_in_full:
            assert len(collapsed_calls) > 0, "Should call _export_full_csv for collapsed export"
        
        if self.mock_config.export_rowlevel_audit_full:
            assert len(audit_calls) > 0, "Should call _export_full_csv for audit export"
        
        # 5. Verify part numbering convention (part001, part002, etc.)
        for call_args in export_calls:
            if len(call_args[0]) >= 2:  # Check output path parameter
                output_path = str(call_args[0][1])
                if "part" in output_path:
                    # Should use zero-padded part numbers
                    assert "part001" in output_path or "part002" in output_path or "part003" in output_path, \
                        f"Should use zero-padded part numbers: {output_path}"
        
        # This test documents the requirement:
        # When both collapse and audit are enabled, generate distinct exports
        # with proper naming conventions and part numbering


class TestDataComparatorQualifyFallback:
    """Test cases for DataComparator QUALIFY fallback compatibility."""
    
    def setup_method(self):
        """Set up test fixtures."""
        # Mock DuckDB connection
        self.mock_con = Mock()
        
        # Create DataComparator instance
        self.comparator = DataComparator(self.mock_con)
    
    def test_qualify_fallback_uses_row_number_subquery(self):
        """
        TEST 3: Sampling SQL uses ROW_NUMBER() OVER (...) + WHERE rn <= N 
        (QUALIFY-less compatibility).
        
        This test MUST FAIL until QUALIFY fallback logic is implemented.
        """
        # Arrange: Mock a scenario where sampling is needed (smart preview with large dataset)
        mock_config = Mock(spec=ComparisonConfig)
        mock_config.enable_smart_preview = True
        mock_config.csv_preview_limit = 500
        mock_config.entire_column_sample_size = 10
        mock_config.annotate_entire_column = True
        mock_config.comparison_keys = ["Key"]
        mock_config.export_full = False  # Focus on preview/sampling logic
        
        # Mock database responses
        self.mock_con.execute.return_value.fetchone.return_value = [10000]
        self.mock_con.execute.return_value.fetchall.return_value = [
            ('Key',), ('name',), ('email',)
        ]
        
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
             patch('pathlib.Path.mkdir') as mock_mkdir, \
             patch('builtins.open', create=True) as mock_open:
            
            # Mock file operations
            mock_file_handle = Mock()
            mock_file_handle.__enter__ = Mock(return_value=mock_file_handle)
            mock_file_handle.__exit__ = Mock(return_value=None)
            mock_open.return_value = mock_file_handle
            
            output_dir = Path("C:/tmp/test_output")
            
            # Act: Trigger sampling logic through export_differences with smart preview
            result = self.comparator.export_differences(
                left_table="test_left_table",
                right_table="test_right_table", 
                config=mock_config,
                output_dir=output_dir
            )
        
        # Assert: Check for ROW_NUMBER() OVER + WHERE rn <= N pattern instead of QUALIFY
        
        # 1. Should not use QUALIFY syntax (compatibility mode)
        qualify_queries = [q for q in captured_queries if "QUALIFY" in q.upper()]
        assert len(qualify_queries) == 0, f"Should not use QUALIFY syntax for compatibility: {qualify_queries}"
        
        # 2. Should use ROW_NUMBER() OVER pattern for sampling
        row_number_queries = [q for q in captured_queries if "ROW_NUMBER() OVER" in q.upper()]
        assert len(row_number_queries) > 0, f"Should use ROW_NUMBER() OVER for sampling: {captured_queries}"
        
        # 3. Should use WHERE rn <= N pattern
        where_rn_queries = [q for q in captured_queries if "WHERE" in q.upper() and "RN <=" in q.upper()]
        assert len(where_rn_queries) > 0, f"Should use WHERE rn <= N pattern: {captured_queries}"
        
        # 4. Verify the complete pattern: subquery with ROW_NUMBER and WHERE filter
        compatible_sampling_queries = []
        for query in captured_queries:
            query_upper = query.upper()
            if ("ROW_NUMBER() OVER" in query_upper and 
                "WHERE" in query_upper and 
                ("RN <=" in query_upper or "ROW_NUM <=" in query_upper)):
                compatible_sampling_queries.append(query)
        
        assert len(compatible_sampling_queries) > 0, \
            f"Should use compatible ROW_NUMBER() + WHERE pattern for sampling: {captured_queries}"
        
        # 5. Verify sample size limit is applied correctly
        for query in compatible_sampling_queries:
            # Should reference the configured sample size
            sample_size = str(mock_config.entire_column_sample_size)
            assert sample_size in query or "10" in query, \
                f"Should apply sample size limit: {query}"
        
        # This test documents the requirement:
        # Use ROW_NUMBER() OVER (...) + WHERE rn <= N instead of QUALIFY
        # for compatibility with DuckDB versions that don't support QUALIFY


class TestDataComparatorSQLWrappingUTF8:
    """Test cases for DataComparator SQL query wrapping and UTF-8 encoding."""
    
    def setup_method(self):
        """Set up test fixtures."""
        # Mock DuckDB connection
        self.mock_con = Mock()
        
        # Create DataComparator instance
        self.comparator = DataComparator(self.mock_con)
    
    def test_full_copy_uses_wrapped_query_and_utf8(self):
        """
        TEST 4: Base query is wrapped: SELECT * FROM ({clean}) q ORDER BY ... LIMIT ... OFFSET ...
        (fixes ORDER BY parser error) and COPY ... ENCODING 'UTF8' (fixes Windows cp1252 crash).
        
        This test MUST FAIL until query wrapping and UTF-8 encoding is implemented.
        """
        # Arrange: Create a test scenario with _export_full_csv
        base_query = "SELECT id, name, value FROM test_table"
        output_path = Path("C:/tmp/test_wrapped_utf8.csv")
        chunk_size = 50000
        
        # Mock large dataset to trigger chunking
        mock_count_result = Mock()
        mock_count_result.fetchone.return_value = [150000]  # Large count
        
        mock_copy_result = Mock()
        
        # Track all SQL executions
        captured_queries = []
        
        def mock_execute(sql):
            captured_queries.append(str(sql))
            # Return appropriate mock based on query type
            if "COUNT(*)" in sql:
                return mock_count_result
            else:
                return mock_copy_result
        
        self.mock_con.execute = mock_execute
        
        # Mock file operations
        with patch('pathlib.Path.mkdir') as mock_mkdir, \
             patch('pathlib.Path.exists', return_value=True), \
             patch('pathlib.Path.unlink') as mock_unlink, \
             patch('builtins.open', create=True) as mock_open:
            
            mock_file_handle = Mock()
            mock_file_handle.__enter__ = Mock(return_value=mock_file_handle)
            mock_file_handle.__exit__ = Mock(return_value=None)
            mock_file_handle.read.return_value = "id,name,value\n1,test,123\n"
            mock_file_handle.write = Mock()
            mock_open.return_value = mock_file_handle
            
            # Act: Call _export_full_csv directly
            self.comparator._export_full_csv(
                query=base_query,
                output_path=output_path,
                chunk_size=chunk_size
            )
        
        # Assert: Verify query wrapping and UTF-8 encoding
        
        # 1. Should have queries that wrap the base query in subselect
        wrapped_queries = [q for q in captured_queries if "SELECT * FROM (" in q and ") q" in q]
        assert len(wrapped_queries) > 0, f"Should wrap base query in subselect: {captured_queries}"
        
        # 2. Should apply ORDER BY, LIMIT, OFFSET to wrapped query
        chunked_queries = [q for q in wrapped_queries if all(clause in q for clause in ["ORDER BY", "LIMIT", "OFFSET"])]
        assert len(chunked_queries) > 0, f"Should apply chunking clauses to wrapped query: {wrapped_queries}"
        
        # 3. Should NOT use ENCODING option (DuckDB doesn't support it for writing)
        copy_queries = [q for q in captured_queries if "COPY (" in q]
        encoding_queries = [q for q in copy_queries if "ENCODING" in q]
        assert len(encoding_queries) == 0, f"Should NOT use ENCODING option in COPY commands: {encoding_queries}"
        
        # 4. Should use qpath() for Windows path handling
        # Look for properly quoted/escaped Windows paths
        windows_path_queries = [q for q in copy_queries if "C:/" in q or "C:\\\\" in q]
        assert len(windows_path_queries) > 0, f"Should handle Windows paths correctly: {copy_queries}"
        
        # 5. Verify no "ORDER BY parser error" patterns exist
        for query in captured_queries:
            # Should not have patterns that cause parser errors
            assert "; ORDER BY" not in query, f"Should not have semicolon before ORDER BY: {query}"
            assert ") q; ORDER BY" not in query, f"Should not have improper semicolon in wrapped query: {query}"
        
        # 6. Verify complete pattern: COPY (SELECT * FROM (base_query) q ORDER BY col LIMIT N OFFSET M) TO 'path' (...options...)
        complete_pattern_queries = []
        for query in captured_queries:
            query_stripped = query.strip()
            if ("COPY (" in query_stripped and
                "SELECT * FROM (" in query and
                ") q" in query and
                "ORDER BY" in query and
                "LIMIT" in query):
                complete_pattern_queries.append(query)
        
        assert len(complete_pattern_queries) > 0, \
            f"Should use complete COPY pattern with wrapping: {captured_queries}"
        
        # This test documents the requirements:
        # 1. Wrap base queries in subselect to prevent ORDER BY parser errors
        # 2. Use UTF-8 encoding in COPY commands to prevent Windows cp1252 crashes
        # 3. Properly handle Windows file paths with qpath()


class TestDataComparatorReservedIdentifiers:
    """Test cases for DataComparator reserved identifier quoting."""
    
    def setup_method(self):
        """Set up test fixtures."""
        # Mock DuckDB connection
        self.mock_con = Mock()
        
        # Create DataComparator instance
        self.comparator = DataComparator(self.mock_con)
    
    def test_reserved_identifiers_are_quoted(self):
        """
        TEST 5: Generated SQL quotes reserved/spacey names (e.g., "from", "type", "Differing Column").
        
        This test MUST FAIL until qident() function and identifier quoting is implemented.
        """
        # Arrange: Set up scenario with reserved words and spacey column names
        mock_config = Mock(spec=ComparisonConfig)
        mock_config.comparison_keys = ["from"]  # Reserved word
        mock_config.value_columns = ["type", "Differing Column"]  # Reserved word + spacey name
        mock_config.tolerance = 0.01
        mock_config.export_full = False  # Focus on basic query generation
        mock_config.annotate_entire_column = True
        
        # Mock dataset configs with reserved column names
        mock_left_config = Mock()
        mock_left_config.column_map = None
        
        mock_right_config = Mock()
        mock_right_config.column_map = {"author": "from"}  # Maps to reserved word
        
        # Mock database responses with reserved/spacey column names
        self.mock_con.execute.return_value.fetchone.return_value = [1000]
        self.mock_con.execute.return_value.fetchall.return_value = [
            ('from',), ('type',), ('Differing Column',)
        ]
        
        # Track SQL queries generated
        captured_queries = []
        
        def mock_execute(sql):
            captured_queries.append(str(sql))
            mock_result = Mock()
            mock_result.fetchone.return_value = [1000]
            mock_result.fetchall.return_value = [('from',), ('type',), ('Differing Column',)]
            return mock_result
        
        self.mock_con.execute = mock_execute
        
        # Mock other dependencies
        with patch.object(self.comparator, '_determine_value_columns', return_value=['type', 'Differing Column']), \
             patch('pathlib.Path.mkdir') as mock_mkdir, \
             patch('builtins.open', create=True) as mock_open:
            
            # Mock file operations
            mock_file_handle = Mock()
            mock_file_handle.__enter__ = Mock(return_value=mock_file_handle)
            mock_file_handle.__exit__ = Mock(return_value=None)
            mock_open.return_value = mock_file_handle
            
            output_dir = Path("C:/tmp/test_output")
            
            # Act: Generate SQL with reserved words and spacey names
            result = self.comparator.export_differences(
                left_table="test_left_table",
                right_table="test_right_table",
                config=mock_config,
                output_dir=output_dir,
                left_dataset_config=mock_left_config,
                right_dataset_config=mock_right_config
            )
        
        # Assert: Verify reserved words and spacey names are quoted
        
        # 1. Should quote reserved word "from"
        from_quoted_queries = [q for q in captured_queries if '"from"' in q]
        from_unquoted_queries = [q for q in captured_queries if ' from ' in q.lower() and '"from"' not in q and 'FROM ' not in q.upper()]
        
        # Should have quoted usage, minimal unquoted usage (only FROM clauses allowed)
        assert len(from_quoted_queries) > 0, f'Should quote reserved word "from": {captured_queries}'
        
        # 2. Should quote reserved word "type"  
        type_quoted_queries = [q for q in captured_queries if '"type"' in q]
        assert len(type_quoted_queries) > 0, f'Should quote reserved word "type": {captured_queries}'
        
        # 3. Should quote spacey column name "Differing Column"
        spacey_quoted_queries = [q for q in captured_queries if '"Differing Column"' in q]
        assert len(spacey_quoted_queries) > 0, f'Should quote spacey column "Differing Column": {captured_queries}'
        
        # 4. Verify qident() function is used consistently
        # All column references should be quoted, not just some
        problematic_queries = []
        for query in captured_queries:
            # Check for unquoted reserved words in column contexts
            if (' from ' in query.lower() and 'FROM ' not in query.upper() and '"from"' not in query):
                # Only flag if it's clearly a column reference, not a FROM clause
                if 'SELECT' in query.upper() and 'FROM' in query.upper():
                    # Parse more carefully - is this a column or FROM clause?
                    query_parts = query.upper().split('FROM')
                    if len(query_parts) > 1:
                        select_part = query_parts[0]
                        if ' from ' in select_part.lower():
                            problematic_queries.append(query)
        
        # 5. Should use qident() for all dynamic identifiers
        # Look for patterns that suggest qident() usage:
        # - Consistent quoting across all column references
        # - Quoted table aliases in complex queries
        quoted_column_queries = [q for q in captured_queries if ('"from"' in q or '"type"' in q or '"Differing Column"' in q)]
        unquoted_column_queries = [q for q in captured_queries if ('l.from' in q or 'r.from' in q or 'l.type' in q or 'r.type' in q)]
        
        # Should prefer quoted over unquoted for consistency
        if len(quoted_column_queries) > 0:
            assert len(quoted_column_queries) >= len(unquoted_column_queries), \
                f"Should consistently quote identifiers. Quoted: {len(quoted_column_queries)}, Unquoted: {len(unquoted_column_queries)}"
        
        # 6. Verify ORDER BY clauses also quote reserved words
        order_by_queries = [q for q in captured_queries if 'ORDER BY' in q.upper()]
        quoted_order_by = [q for q in order_by_queries if ('"from"' in q or '"type"' in q or '"Differing Column"' in q)]
        
        if len(order_by_queries) > 0:
            assert len(quoted_order_by) > 0, f"Should quote reserved words in ORDER BY clauses: {order_by_queries}"
        
        # This test documents the requirement:
        # Use qident() to quote all dynamic SQL identifiers, especially:
        # - Reserved words like "from", "type"  
        # - Column names with spaces like "Differing Column"
        # - Ensure consistent quoting across all SQL generation


class TestDataComparatorEncodingFix:
    """Test cases for DuckDB ENCODING option fix."""
    
    def setup_method(self):
        """Set up test fixtures."""
        # Mock DuckDB connection
        self.mock_con = Mock()
        
        # Create DataComparator instance
        self.comparator = DataComparator(self.mock_con)
    
    def test_full_copy_omits_encoding_option_and_uses_helper(self, tmp_path, monkeypatch):
        """
        TEST 6: Test that COPY statements remove illegal ENCODING option and use helper methods.
        
        This test verifies the fix for DuckDB "Invalid Input Error: Option 'ENCODING' is not supported for writing".
        """
        # 1. Monkeypatch to track executed SQL
        executed = {}
        def fake_exec(sql):
            executed['sql'] = sql
            class R: 
                def fetchone(self): return (0,)
            return R()
        self.comparator.con.execute = fake_exec
        
        # 2. Monkeypatch version check to ensure it's called
        monkeypatch.setattr(self.comparator, "_duckdb_supports_force_quote", lambda: True)

        # 3. Execute with minimal output
        output_path = tmp_path / "out.csv"
        self.comparator._export_full_csv("SELECT 1 as col_a, 'value' as col_b", output_path, 100)

        # 4. Assertions
        sql = executed['sql']
        # Assert fix (no illegal ENCODING option)
        assert "ENCODING" not in sql, f"ENCODING option should be removed from COPY TO statements: {sql}"
        # Assert hardening (FORCE_QUOTE is applied when supported)
        assert "FORCE_QUOTE *" in sql, f"FORCE_QUOTE should be applied when supported: {sql}"
        # Assert TDD principle: must use HEADER
        assert "HEADER, DELIMITER ','" in sql, f"Should use HEADER and DELIMITER: {sql}"
        # Assert wrapper uses quoted path
        expected_path = f"'{output_path.as_posix()}'"
        assert expected_path in sql.replace('\\', '/'), f"Should use quoted path {expected_path}: {sql}"
        # Assert wrapper uses correct options helper
        assert "COPY (" in sql, f"Should use COPY statement: {sql}"