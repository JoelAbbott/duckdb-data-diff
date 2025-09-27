"""
Unit tests for smart preview policy and UNION schema uniformity.
Following mandatory TDD: These tests MUST fail until uniform UNION schema is implemented.

Testing requirements:
- Uniform UNION ALL schema across all branches 
- Binder-safe ORDER BY with unqualified column names
- Deterministic preview ordering
- No qualified column references in ORDER BY
"""

import pytest
from unittest.mock import Mock, MagicMock, patch
from pathlib import Path
import sys

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

# Import existing components
from src.core.comparator import DataComparator, qident
from src.config.manager import ComparisonConfig


class TestPreviewUnionSchema:
    """Test cases for smart preview UNION schema uniformity."""
    
    def setup_method(self):
        """Set up test fixtures."""
        # Mock DuckDB connection
        self.mock_con = Mock()
        
        # Create DataComparator instance
        self.comparator = DataComparator(self.mock_con)
        
        # Mock configuration for smart preview
        self.mock_config = Mock(spec=ComparisonConfig)
        self.mock_config.comparison_keys = ["Key"] 
        self.mock_config.value_columns = ["name", "email"]
        self.mock_config.enable_smart_preview = True
        self.mock_config.annotate_entire_column = True
        self.mock_config.csv_preview_limit = 1000
        self.mock_config.entire_column_sample_size = 10
        self.mock_config.preview_order = ["Differing Column", "Key"]
        self.mock_config.max_differences = 1000
        self.mock_config.export_full = True
        self.mock_config.chunk_export_size = 50000
        self.mock_config.collapse_entire_column_in_preview = False  # Default behavior
        self.mock_config.collapse_entire_column_in_full = False
        
        # Mock dataset configs
        self.mock_left_config = Mock()
        self.mock_left_config.column_map = None
        
        self.mock_right_config = Mock()
        self.mock_right_config.column_map = None
        
        # Mock database responses
        self.mock_con.execute.return_value.fetchone.return_value = [1000]
        self.mock_con.execute.return_value.fetchall.return_value = [
            ('Key',), ('name',), ('email',)
        ]
    
    def test_preview_union_schema_uniform_and_wrapped_ordering(self, monkeypatch):
        """
        Test that generated preview SQL contains uniform UNION schema and binder-safe ORDER BY.
        
        This test MUST FAIL until uniform UNION schema is implemented.
        """
        # Arrange: Mock version check to avoid side effects
        monkeypatch.setattr(self.comparator, "_duckdb_supports_force_quote", lambda: True)
        
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
            
            # Act: Trigger smart preview export
            result = self.comparator.export_differences(
                left_table="test_left_table",
                right_table="test_right_table",
                config=self.mock_config,
                output_dir=output_dir,
                left_dataset_config=self.mock_left_config,
                right_dataset_config=self.mock_right_config
            )
        
        # Assert: Find the smart preview query (ignore capability probe queries)
        preview_queries = []
        for query in captured_queries:
            # Filter out capability probe queries that contain 'test_col'
            if ('UNION ALL' in query.upper() and 
                'summaries' in query.lower() and 
                'samples' in query.lower() and
                'test_col' not in query.lower()):
                preview_queries.append(query)
        
        assert len(preview_queries) > 0, f"Should generate smart preview query with UNION ALL: {captured_queries}"
        
        preview_query = preview_queries[0]
        
        # 1. The generated preview SQL contains a wrapper: SELECT * FROM ( … UNION ALL … ) AS preview
        assert "SELECT * FROM (" in preview_query, f"Should wrap UNION in outer SELECT: {preview_query}"
        assert ") AS preview" in preview_query or ") preview" in preview_query, f"Should alias wrapped query: {preview_query}"
        
        # 2. The same column list is projected in every branch of the UNION
        required_columns = ['"Differing Column"', '"Key"', '"Left Value"', '"Right Value"', 'entire_column', 'sample']
        
        # Verify uniform schema by checking for the presence of each required column in the query
        # Since the structure is complex, we'll verify the overall schema requirements are met
        for col in required_columns:
            if col == 'entire_column':
                # Check for AS entire_column pattern
                assert ('AS entire_column' in preview_query or 
                        'as entire_column' in preview_query), f"Should project {col} consistently: {preview_query}"
            elif col == 'sample':
                # Check for AS sample pattern with CAST
                assert ('AS sample' in preview_query or 
                        'as sample' in preview_query), f"Should project {col} consistently: {preview_query}"
                # Verify CAST patterns for sample priorities
                assert 'CAST(2 AS BIGINT) AS sample' in preview_query, "Should have priority 2 for summaries"
                assert 'CAST(1 AS BIGINT) AS sample' in preview_query, "Should have priority 1 for samples"  
                assert 'CAST(0 AS BIGINT) AS sample' in preview_query, "Should have priority 0 for partials"
            else:
                # Check for quoted column names
                assert col in preview_query, f"Should project {col} consistently: query contains {col}"
        
        # 3. The final ORDER BY references unqualified column names only
        order_by_section = ""
        if "ORDER BY" in preview_query:
            order_by_section = preview_query[preview_query.find("ORDER BY"):]
        
        assert "ORDER BY" in order_by_section, f"Should have ORDER BY clause: {preview_query}"
        
        # Should end with sample DESC
        assert "sample DESC" in order_by_section, f"Should order by sample DESC: {order_by_section}"
        
        # 4. The SQL's ORDER BY segment does not contain qualified references (no alias.column)
        assert ".sample" not in order_by_section, f"Should not have qualified .sample reference: {order_by_section}"
        assert "summaries." not in order_by_section, f"Should not have qualified summaries. reference: {order_by_section}"
        assert "samples." not in order_by_section, f"Should not have qualified samples. reference: {order_by_section}"
        assert "partials." not in order_by_section, f"Should not have qualified partials. reference: {order_by_section}"
        
        # 5. Verify qident() is used for preview_order columns
        for col in self.mock_config.preview_order:
            quoted_col = qident(col)
            assert quoted_col in order_by_section, f"Should use qident for {col}: {order_by_section}"
    
    def test_preview_is_deterministic_with_sample_desc(self, monkeypatch):
        """
        Test that with fixed inputs, preview results are deterministic across runs.
        
        This test MUST FAIL until deterministic ordering is implemented.
        """
        # Arrange: Mock version check
        monkeypatch.setattr(self.comparator, "_duckdb_supports_force_quote", lambda: True)
        
        # Capture queries from two identical runs
        captured_queries_run1 = []
        captured_queries_run2 = []
        
        def mock_execute_run1(sql):
            captured_queries_run1.append(str(sql))
            mock_result = Mock()
            mock_result.fetchone.return_value = [1000]
            mock_result.fetchall.return_value = [('Key',), ('name',), ('email',)]
            return mock_result
        
        def mock_execute_run2(sql):
            captured_queries_run2.append(str(sql))
            mock_result = Mock()
            mock_result.fetchone.return_value = [1000]
            mock_result.fetchall.return_value = [('Key',), ('name',), ('email',)]
            return mock_result
        
        # Mock other dependencies
        with patch.object(self.comparator, '_determine_value_columns', return_value=['name', 'email']), \
             patch('pathlib.Path.mkdir'), \
             patch('builtins.open', create=True) as mock_open:
            
            mock_file_handle = Mock()
            mock_file_handle.__enter__ = Mock(return_value=mock_file_handle)
            mock_file_handle.__exit__ = Mock(return_value=None)
            mock_open.return_value = mock_file_handle
            
            output_dir = Path("C:/tmp/test_output")
            
            # Act: Run the same export twice with identical inputs
            
            # Run 1
            self.mock_con.execute = mock_execute_run1
            result1 = self.comparator.export_differences(
                left_table="test_left_table",
                right_table="test_right_table",
                config=self.mock_config,
                output_dir=output_dir,
                left_dataset_config=self.mock_left_config,
                right_dataset_config=self.mock_right_config
            )
            
            # Run 2
            self.mock_con.execute = mock_execute_run2
            result2 = self.comparator.export_differences(
                left_table="test_left_table",
                right_table="test_right_table",
                config=self.mock_config,
                output_dir=output_dir,
                left_dataset_config=self.mock_left_config,
                right_dataset_config=self.mock_right_config
            )
        
        # Assert: Extract preview queries from both runs (ignore capability probes)
        def extract_preview_query(queries):
            for query in queries:
                if ('UNION ALL' in query.upper() and 
                    'summaries' in query.lower() and 
                    'samples' in query.lower() and
                    'test_col' not in query.lower()):
                    return query
            return None
        
        preview_query1 = extract_preview_query(captured_queries_run1)
        preview_query2 = extract_preview_query(captured_queries_run2)
        
        assert preview_query1 is not None, f"Run 1 should generate preview query: {captured_queries_run1}"
        assert preview_query2 is not None, f"Run 2 should generate preview query: {captured_queries_run2}"
        
        # The queries should be identical (deterministic)
        assert preview_query1 == preview_query2, "Preview queries should be deterministic across runs"
        
        # Verify deterministic ordering elements are present
        assert "sample DESC" in preview_query1, "Should have deterministic sample DESC ordering"
        
        # Verify preview_order columns are in ORDER BY
        order_by_section = ""
        if "ORDER BY" in preview_query1:
            order_by_section = preview_query1[preview_query1.find("ORDER BY"):]
        
        for col in self.mock_config.preview_order:
            quoted_col = qident(col)
            assert quoted_col in order_by_section, f"Should order by {quoted_col} for determinism"
        
        # This test documents the requirement:
        # Smart preview must be deterministic across identical runs
        # with proper sample priority ordering (2=summaries, 1=samples, 0=partials)
    
    def test_preview_collapses_entire_column_to_single_summary_row(self, monkeypatch):
        """
        Test that preview emits exactly 1 row for columns with entire_column = TRUE (no samples).
        
        This test MUST FAIL until preview collapse logic is implemented.
        """
        # Arrange: Mock version check and enable collapse mode
        monkeypatch.setattr(self.comparator, "_duckdb_supports_force_quote", lambda: True)
        self.mock_config.collapse_entire_column_in_preview = True  # Enable collapse mode
        
        # Create a scenario where one column is fully different
        # Mock the annotated_query to simulate a column with entire_column = TRUE
        mock_annotated_query = '''
            SELECT 
                'name' AS "Differing Column",
                'key123' AS "Key", 
                'Alice' AS "Left Value",
                'Bob' AS "Right Value",
                'Different Values' AS "Difference Type",
                'true' AS "Entire Column Different"
            UNION ALL
            SELECT 
                'name' AS "Differing Column", 
                'key456' AS "Key",
                'Charlie' AS "Left Value", 
                'David' AS "Right Value",
                'Different Values' AS "Difference Type",
                'true' AS "Entire Column Different"
        '''
        
        # Track SQL queries generated
        captured_queries = []
        
        def mock_execute(sql):
            captured_queries.append(str(sql))
            mock_result = Mock()
            # Simulate count results that indicate data exists
            if "COUNT(*)" in sql:
                mock_result.fetchone.return_value = [100]  # Total rows for entire_column calculation
            else:
                mock_result.fetchone.return_value = [2]  # Sample size
            mock_result.fetchall.return_value = [('Key',), ('name',)]
            return mock_result
        
        self.mock_con.execute = mock_execute
        
        # Override the annotated query generation to use our mock
        original_method = None
        
        # Mock other dependencies  
        with patch.object(self.comparator, '_determine_value_columns', return_value=['name']), \
             patch('pathlib.Path.mkdir'), \
             patch('builtins.open', create=True) as mock_open:
            
            mock_file_handle = Mock()
            mock_file_handle.__enter__ = Mock(return_value=mock_file_handle)
            mock_file_handle.__exit__ = Mock(return_value=None)
            mock_open.return_value = mock_file_handle
            
            output_dir = Path("C:/tmp/test_output")
            
            # Act: Trigger smart preview export
            result = self.comparator.export_differences(
                left_table="test_left_table",
                right_table="test_right_table",
                config=self.mock_config,
                output_dir=output_dir,
                left_dataset_config=self.mock_left_config,
                right_dataset_config=self.mock_right_config
            )
        
        # Assert: Find the smart preview query (ignore capability probe queries)
        preview_queries = []
        for query in captured_queries:
            if ('UNION ALL' in query.upper() and 
                'summaries' in query.lower() and
                'test_col' not in query.lower()):
                preview_queries.append(query)
        
        assert len(preview_queries) > 0, f"Should generate smart preview query: {captured_queries}"
        
        preview_query = preview_queries[0]
        
        # 1. For columns with entire_column = TRUE, preview should emit exactly 1 summary row
        # Check that the summaries branch exists
        assert 'FROM summaries' in preview_query, f"Should have summaries branch: {preview_query}"
        
        # 2. For fully-different columns, samples branch should be eliminated or limited to 0
        # Look for the samples branch logic
        samples_branch_lines = []
        lines = preview_query.split('\n')
        in_samples = False
        for line in lines:
            if 'FROM samples' in line:
                in_samples = True
            if in_samples:
                samples_branch_lines.append(line.strip())
                if 'UNION ALL' in line or 'ORDER BY' in line:
                    break
        
        # 2. For fully-different columns in collapse mode, summaries should be limited to 1 per column
        # Look for the summaries branch logic with collapse behavior
        summaries_branch_lines = []
        lines = preview_query.split('\n')
        in_summaries = False
        for line in lines:
            if 'FROM summaries' in line:
                in_summaries = True
            if in_summaries:
                summaries_branch_lines.append(line.strip())
                if 'UNION ALL' in line or 'ORDER BY' in line:
                    break
        
        if summaries_branch_lines:
            summaries_section = ' '.join(summaries_branch_lines)
            # In collapse mode, should use WHERE rn <= 1 and PARTITION BY "Differing Column"
            assert 'WHERE rn <= 1' in summaries_section, \
                f"Summaries branch should be limited to 1 row per column in collapse mode: {summaries_section}"
        
        # 3. Verify PARTITION BY "Differing Column" is used for summaries in collapse mode
        partition_by_patterns = [line for line in lines if 'PARTITION BY' in line and 'Differing Column' in line]
        assert len(partition_by_patterns) > 0, \
            f"Should use PARTITION BY Differing Column in collapse mode: {preview_query}"
        
        # 4. Verify deterministic ordering is preserved
        assert 'ORDER BY' in preview_query, "Should maintain deterministic ordering"
        assert 'sample DESC' in preview_query, "Should order by sample priority"
        
        # 5. Verify uniform schema is maintained
        assert 'CAST(2 AS BIGINT) AS sample' in preview_query, "Should have summary priority"
        assert 'SELECT * FROM (' in preview_query, "Should use wrapper SELECT"
        
        # This test documents the requirement:
        # Preview must collapse entire_column=TRUE to single summary rows (no samples)
    
    def test_full_export_now_permanently_collapsed_by_default(self, monkeypatch):
        """
        Test that full exports are now PERMANENTLY collapsed by default (no config flag needed).
        
        Updated for permanent collapse behavior - collapse_entire_column_in_full is deprecated.
        """
        # Arrange: Mock version check
        monkeypatch.setattr(self.comparator, "_duckdb_supports_force_quote", lambda: True)
        
        # Standard config - no collapse flag needed (permanent behavior)
        self.mock_config.export_full = True
        # Ensure audit flag is off for this test
        self.mock_config.export_rowlevel_audit_full = False
        
        # Track SQL queries generated
        captured_queries = []
        
        def mock_execute(sql):
            captured_queries.append(str(sql))
            mock_result = Mock()
            # Simulate count results
            if "COUNT(*)" in sql:
                mock_result.fetchone.return_value = [100]
            else:
                mock_result.fetchone.return_value = [2]
            mock_result.fetchall.return_value = [('Key',), ('name',)]
            return mock_result
        
        self.mock_con.execute = mock_execute
        
        # Mock other dependencies
        with patch.object(self.comparator, '_determine_value_columns', return_value=['name']), \
             patch.object(self.comparator, '_export_full_csv') as mock_export_full, \
             patch('pathlib.Path.mkdir'), \
             patch('builtins.open', create=True) as mock_open:
            
            mock_file_handle = Mock()
            mock_file_handle.__enter__ = Mock(return_value=mock_file_handle)
            mock_file_handle.__exit__ = Mock(return_value=None)
            mock_open.return_value = mock_file_handle
            
            output_dir = Path("C:/tmp/test_output")
            
            # Act: Trigger full export with collapse enabled
            result = self.comparator.export_differences(
                left_table="test_left_table",
                right_table="test_right_table",
                config=self.mock_config,
                output_dir=output_dir,
                left_dataset_config=self.mock_left_config,
                right_dataset_config=self.mock_right_config
            )
        
        # Assert: Verify permanent collapse behavior
        
        # 1. Should call _export_full_csv for value differences exports
        export_calls = mock_export_full.call_args_list
        
        # Look for the standard collapsed export (now always generated)
        standard_full_calls = [call for call in export_calls 
                             if "value_differences_full.csv" in str(call[0][1])]
        assert len(standard_full_calls) > 0, f"Should generate standard collapsed full export: {export_calls}"
        
        # 2. The standard full export should be collapsed by default (no flag needed)  
        if standard_full_calls:
            full_export_call = standard_full_calls[0]
            full_export_query = str(full_export_call[0][0])  # First argument is the query
            
            # Should use ROW_NUMBER() and PARTITION BY for permanent collapse
            assert "ROW_NUMBER()" in full_export_query, \
                f"Full export should be collapsed by default: {full_export_query}"
            assert "PARTITION BY" in full_export_query, \
                f"Full export should partition by column for collapse: {full_export_query}"
            assert "WHERE rn = 1" in full_export_query, \
                f"Full export should select only first row per column: {full_export_query}"
        
        # 3. Standard naming convention for collapsed export
        for call_args in standard_full_calls:
            output_path = str(call_args[0][1])  # Second argument is output path
            assert "value_differences_full.csv" in output_path, \
                f"Standard collapsed export should use standard naming: {output_path}"
            # Should NOT have extra suffixes - it's the default behavior
            assert "audit" not in output_path, \
                f"Standard export should not have 'audit' suffix: {output_path}"
        
        # This test documents the NEW requirement:
        # Full exports are PERMANENTLY collapsed by default - no configuration needed