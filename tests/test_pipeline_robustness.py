"""
Test suite to verify pipeline code robustness.
Ensures the pipeline handles edge cases automatically without manual YAML fixes.
"""

import pytest
import pandas as pd
import duckdb
import yaml
import tempfile
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent))

from pipeline import stage_dataset, register_udfs, boolean_t_f_py


class TestPipelineRobustness:
    """Test suite for pipeline code robustness."""
    
    def test_duckdb_column_name_transformation(self):
        """Test that pipeline handles DuckDB's automatic column name transformations."""
        # Create test CSV with problematic column names
        test_data = pd.DataFrame({
            'Internal ID': [1, 2, 3],
            'Internal ID.1': [100, 200, 300],  # DuckDB will transform to Internal ID_1
            'Column.With.Dots': ['a', 'b', 'c'],  # Will become Column_With_Dots
            'Normal Column': ['x', 'y', 'z']
        })
        
        # Save to temp CSV
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            test_data.to_csv(f, index=False)
            temp_path = f.name
        
        try:
            # Create config that expects the original names (with dots)
            config = {
                'path': temp_path,
                'keys': ['message_id', 'transaction_id'],
                'column_map': {
                    'Internal ID': 'message_id',
                    'Internal ID.1': 'transaction_id',  # Original name with dot
                    'Column.With.Dots': 'dotted_column',
                    'Normal Column': 'normal_column'
                },
                'dtypes': {
                    'message_id': 'int64',
                    'transaction_id': 'int64',
                    'dotted_column': 'string',
                    'normal_column': 'string'
                },
                'normalizers': {}
            }
            
            # Stage the dataset - pipeline should handle the transformation
            con = duckdb.connect(':memory:')
            register_udfs(con)
            
            # This should work even though DuckDB transforms the column names
            stage_dataset(con, 'test_dataset', config)
            
            # Verify columns were renamed correctly
            result = con.execute("SELECT * FROM test_dataset LIMIT 1").fetchall()
            columns = [r[0] for r in con.execute("DESCRIBE test_dataset").fetchall()]
            
            assert 'message_id' in columns, "message_id should exist after mapping"
            assert 'transaction_id' in columns, "transaction_id should exist after mapping"
            assert 'dotted_column' in columns, "dotted_column should exist after mapping"
            assert 'normal_column' in columns, "normal_column should exist after mapping"
            
        finally:
            Path(temp_path).unlink(missing_ok=True)
    
    def test_boolean_normalization_all_formats(self):
        """Test that boolean normalization handles all common boolean representations."""
        test_cases = [
            ('true', 't'),
            ('True', 't'),
            ('TRUE', 't'),
            ('t', 't'),
            ('T', 't'),
            ('1', 't'),
            ('false', 'f'),
            ('False', 'f'),
            ('FALSE', 'f'),
            ('f', 'f'),
            ('F', 'f'),
            ('0', 'f'),
            ('yes', 'yes'),  # Not recognized, should pass through
            ('no', 'no'),    # Not recognized, should pass through
            (None, None),     # Null handling
        ]
        
        for input_val, expected in test_cases:
            result = boolean_t_f_py(input_val)
            assert result == expected, f"boolean_t_f({input_val}) should return {expected}, got {result}"
    
    def test_boolean_fields_normalized_in_pipeline(self):
        """Test that boolean fields are properly normalized during staging."""
        # Create test data with various boolean formats
        test_data = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'is_active': ['true', 'false', 'True', 'FALSE', '1'],
            'has_data': ['t', 'f', 'T', 'F', '0']
        })
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            test_data.to_csv(f, index=False)
            temp_path = f.name
        
        try:
            config = {
                'path': temp_path,
                'keys': ['id'],
                'column_map': {},
                'dtypes': {
                    'id': 'int64',
                    'is_active': 'boolean',
                    'has_data': 'boolean'
                },
                'normalizers': {
                    'is_active': ['boolean_t_f'],
                    'has_data': ['boolean_t_f']
                }
            }
            
            con = duckdb.connect(':memory:')
            register_udfs(con)
            stage_dataset(con, 'test_booleans', config)
            
            # Check that all boolean values are normalized to 't' or 'f'
            result = con.execute("SELECT is_active, has_data FROM test_booleans").fetchall()
            
            for row in result:
                assert row[0] in ('t', 'f'), f"is_active should be 't' or 'f', got {row[0]}"
                assert row[1] in ('t', 'f'), f"has_data should be 't' or 'f', got {row[1]}"
            
            # Verify specific normalizations
            first_row = con.execute("SELECT is_active FROM test_booleans WHERE id = 1").fetchone()
            assert first_row[0] == 't', "true should normalize to 't'"
            
            second_row = con.execute("SELECT is_active FROM test_booleans WHERE id = 2").fetchone()
            assert second_row[0] == 'f', "false should normalize to 'f'"
            
        finally:
            Path(temp_path).unlink(missing_ok=True)
    
    def test_mixed_case_column_handling(self):
        """Test that pipeline handles mixed case column names correctly."""
        test_data = pd.DataFrame({
            'Message_ID': [1, 2, 3],
            'Transaction_Id': [100, 200, 300],
            'Email_Address': ['a@test.com', 'b@test.com', 'c@test.com']
        })
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            test_data.to_csv(f, index=False)
            temp_path = f.name
        
        try:
            config = {
                'path': temp_path,
                'keys': ['message_id', 'transaction_id'],
                'column_map': {
                    'Message_ID': 'message_id',
                    'Transaction_Id': 'transaction_id',
                    'Email_Address': 'email'
                },
                'dtypes': {
                    'message_id': 'int64',
                    'transaction_id': 'int64',
                    'email': 'string'
                },
                'normalizers': {
                    'email': ['unicode_clean', 'upper']
                }
            }
            
            con = duckdb.connect(':memory:')
            register_udfs(con)
            stage_dataset(con, 'test_mixed_case', config)
            
            # Verify the mapping worked
            columns = [r[0] for r in con.execute("DESCRIBE test_mixed_case").fetchall()]
            assert 'message_id' in columns
            assert 'transaction_id' in columns
            assert 'email' in columns
            
            # Verify normalization was applied
            emails = con.execute("SELECT email FROM test_mixed_case").fetchall()
            for email in emails:
                assert email[0].isupper(), f"Email should be uppercase: {email[0]}"
            
        finally:
            Path(temp_path).unlink(missing_ok=True)
    
    def test_special_characters_in_column_names(self):
        """Test handling of special characters in column names."""
        test_data = pd.DataFrame({
            'Column (with parens)': [1, 2, 3],
            'Column-with-dashes': [4, 5, 6],
            'Column_with_underscores': [7, 8, 9],
            'Column$with$dollars': [10, 11, 12]
        })
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            test_data.to_csv(f, index=False)
            temp_path = f.name
        
        try:
            con = duckdb.connect(':memory:')
            register_udfs(con)
            
            # Read the CSV to see how DuckDB handles these names
            con.execute(f"CREATE TABLE test_raw AS SELECT * FROM read_csv_auto('{temp_path}', header=TRUE, all_varchar=1)")
            actual_columns = [r[0] for r in con.execute("DESCRIBE test_raw").fetchall()]
            
            print(f"Original columns: {list(test_data.columns)}")
            print(f"DuckDB columns: {actual_columns}")
            
            # DuckDB should handle these gracefully
            assert len(actual_columns) == 4, "All columns should be present"
            
        finally:
            Path(temp_path).unlink(missing_ok=True)
    
    def test_key_validation_with_mapped_names(self):
        """Test that the pipeline validates keys use mapped names correctly."""
        test_data = pd.DataFrame({
            'ID': [1, 2, 3],
            'TransID': [100, 200, 300],
            'Value': ['a', 'b', 'c']
        })
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            test_data.to_csv(f, index=False)
            temp_path = f.name
        
        try:
            # Config uses mapped names for keys (correct approach)
            config = {
                'path': temp_path,
                'keys': ['message_id', 'transaction_id'],  # Using mapped names
                'column_map': {
                    'ID': 'message_id',
                    'TransID': 'transaction_id',
                    'Value': 'value'
                },
                'dtypes': {
                    'message_id': 'int64',
                    'transaction_id': 'int64',
                    'value': 'string'
                },
                'normalizers': {}
            }
            
            con = duckdb.connect(':memory:')
            register_udfs(con)
            
            # This should work with the mapped key names
            stage_dataset(con, 'test_keys', config)
            
            # Verify the keys are present
            columns = [r[0] for r in con.execute("DESCRIBE test_keys").fetchall()]
            assert 'message_id' in columns
            assert 'transaction_id' in columns
            
        finally:
            Path(temp_path).unlink(missing_ok=True)
    
    def test_null_value_handling(self):
        """Test that null values are handled correctly in normalization."""
        test_data = pd.DataFrame({
            'id': [1, 2, 3, 4],
            'email': ['test@example.com', None, 'another@test.com', ''],
            'is_active': ['true', None, 'false', '']
        })
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            test_data.to_csv(f, index=False)
            temp_path = f.name
        
        try:
            config = {
                'path': temp_path,
                'keys': ['id'],
                'column_map': {},
                'dtypes': {
                    'id': 'int64',
                    'email': 'string',
                    'is_active': 'boolean'
                },
                'normalizers': {
                    'email': ['unicode_clean', 'upper'],
                    'is_active': ['boolean_t_f']
                }
            }
            
            con = duckdb.connect(':memory:')
            register_udfs(con)
            
            # Should handle nulls without errors
            stage_dataset(con, 'test_nulls', config)
            
            # Verify nulls are preserved
            result = con.execute("SELECT id, email, is_active FROM test_nulls ORDER BY id").fetchall()
            
            # Row 2 should have null email
            assert result[1][1] is None or result[1][1] == '', "Null email should be preserved"
            
            # Row 4 has empty string - normalizers should handle it
            assert result[3][2] in ('', None, 'f'), "Empty string should be handled gracefully"
            
        finally:
            Path(temp_path).unlink(missing_ok=True)


if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v", "--tb=short"])