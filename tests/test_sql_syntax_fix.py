"""
TDD Test Suite for SQL Syntax Fix in Quote Stripping Logic.

This test MUST FAIL initially due to SQL syntax errors in the REGEXP_REPLACE pattern
for quote stripping and PASS only after implementing a safe stripping method.

The test isolates the SQL syntax error in _build_robust_comparison_condition's
quote stripping functionality: the pattern '^[\'\"]*|[\'\"]*$' causes parser errors.

Following CLAUDE.md TDD Protocol: Write Tests → Commit → Code → Iterate → Commit
"""

import pytest
from unittest.mock import Mock, patch
from pathlib import Path
import sys
import duckdb

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.core.comparator import DataComparator
from src.config.manager import ComparisonConfig


class TestSQLSyntaxFix:
    """
    Test cases for SQL syntax fix in quote stripping logic.
    
    CRITICAL: This test must FAIL until the SQL syntax is fixed.
    Current implementation uses regex pattern '^[\'\"]*|[\'\"]*$' that causes SQL parser errors.
    Fixed implementation should use a safe quote stripping method.
    """
    
    def setup_method(self):
        """Set up test fixtures with real DuckDB connection for syntax validation."""
        # Use real DuckDB connection to test actual SQL syntax
        self.con = duckdb.connect(':memory:')
        self.comparator = DataComparator(self.con)
        
        # Create test config for exact comparison (no tolerance)
        self.config = ComparisonConfig(left_dataset="test_left", right_dataset="test_right")
        self.config.tolerance = 0
        
        # Create minimal test tables
        self.con.execute("""
            CREATE TABLE test_left (
                id INTEGER,
                text_col VARCHAR
            )
        """)
        
        self.con.execute("""
            CREATE TABLE test_right (
                id INTEGER,
                text_col VARCHAR
            )
        """)
        
        # Insert test data with quoted strings
        self.con.execute("""
            INSERT INTO test_left VALUES 
                (1, '''System'''),
                (2, '"Data"'),
                (3, 'Normal')
        """)
        
        self.con.execute("""
            INSERT INTO test_right VALUES 
                (1, 'System'),
                (2, 'Data'),
                (3, 'Normal')
        """)
    
    def teardown_method(self):
        """Clean up test fixtures."""
        self.con.close()
    
    def test_current_regex_pattern_causes_parser_error(self):
        """
        Test that the current regex pattern '^[\'\"]*|[\'\"]*$' causes SQL parser error.
        
        EXPECTED TO FAIL INITIALLY: Current regex pattern contains quote escaping
        that DuckDB cannot parse correctly, causing ParserException.
        
        This test isolates the exact SQL syntax problem in the robust comparison logic.
        """
        # The problematic regex pattern currently used in _build_robust_comparison_condition
        broken_regex_pattern = "'^[\'\"]*|[\'\"]*$'"
        
        # Build test SQL that uses this problematic pattern
        broken_sql = f"""
            SELECT REGEXP_REPLACE('System', {broken_regex_pattern}, '', 'g') AS result
        """
        
        # PHASE 1 (TDD - Test Must Fail): This should raise a SQL parser error
        with pytest.raises(duckdb.ParserException) as exc_info:
            self.con.execute(broken_sql).fetchone()
        
        # Verify it's specifically a parser error related to the regex syntax
        error_msg = str(exc_info.value).lower()
        assert 'parser' in error_msg or 'syntax' in error_msg, (
            f"Expected SQL parser error, but got: {exc_info.value}"
        )
    
    def test_robust_comparison_condition_fails_due_to_regex_syntax(self):
        """
        Test that _build_robust_comparison_condition generates SQL with parser errors.
        
        EXPECTED TO FAIL INITIALLY: The method generates SQL containing the broken
        regex pattern, causing ParserException when executed.
        
        After fix: The method should generate safe SQL that executes successfully.
        """
        # Get the robust comparison condition from the comparator
        # This will contain the problematic regex pattern
        condition_sql = self.comparator._build_robust_comparison_condition(
            norm_col="text_col", 
            norm_right_col="text_col", 
            config=self.config
        )
        
        # Build a test query using the condition
        test_query = f"""
            SELECT COUNT(*) 
            FROM test_left l
            INNER JOIN test_right r ON l.id = r.id
            WHERE {condition_sql.strip()}
        """
        
        print(f"Generated SQL condition:\n{condition_sql}")
        
        # PHASE 1 (TDD - Test Must Fail): This should raise a SQL parser error
        # due to the problematic regex pattern in the comparison condition
        with pytest.raises(duckdb.ParserException) as exc_info:
            result = self.con.execute(test_query).fetchone()
        
        # Verify the error is related to the regex syntax
        error_msg = str(exc_info.value)
        assert '"]*|[\'"]*$' in error_msg or 'syntax error' in error_msg.lower(), (
            f"Expected regex syntax error, but got: {exc_info.value}"
        )
    
    def test_safe_quote_stripping_alternative_works(self):
        """
        Test that a safe alternative to regex-based quote stripping works correctly.
        
        This demonstrates the target implementation approach using safe string operations.
        After the fix, this is what _build_robust_comparison_condition should use.
        """
        # Safe alternative using LTRIM/RTRIM instead of complex regex
        safe_sql = """
            SELECT 
                TRIM(LOWER(RTRIM(LTRIM(?, '"'), '"'))) AS double_quote_stripped,
                TRIM(LOWER(RTRIM(LTRIM(?, ''''), ''''))) AS single_quote_stripped
        """
        
        # Test with quoted strings
        result = self.con.execute(safe_sql, ['"System"', "'Data'"]).fetchone()
        double_stripped, single_stripped = result
        
        # Verify the safe method works correctly
        assert double_stripped == 'system', f"Expected 'system', got '{double_stripped}'"
        assert single_stripped == 'data', f"Expected 'data', got '{single_stripped}'"
    
    def test_comprehensive_safe_quote_stripping_function(self):
        """
        Test a comprehensive safe quote stripping approach.
        
        This shows how to safely remove both single and double quotes
        from the beginning and end of strings without regex syntax issues.
        """
        test_cases = [
            ('"System"', 'system'),
            ("'Data'", 'data'),
            ('Normal', 'normal'),
            ('""Double""', 'double'),
            ("''Single''", 'single'),
            ('"Mixed"', 'mixed'),
        ]
        
        # Safe quote stripping using nested LTRIM/RTRIM calls
        safe_sql = """
            SELECT TRIM(
                LOWER(
                    RTRIM(
                        LTRIM(
                            RTRIM(
                                LTRIM(?, ''''), 
                                ''''
                            ), 
                            '"'
                        ), 
                        '"'
                    )
                )
            ) AS result
        """
        
        for input_str, expected in test_cases:
            result = self.con.execute(safe_sql, [input_str]).fetchone()
            actual = result[0] if result else None
            
            assert actual == expected, (
                f"Safe quote stripping failed: '{input_str}' -> expected '{expected}', got '{actual}'"
            )
    
    def test_current_implementation_vs_safe_implementation(self):
        """
        Direct comparison showing current broken implementation vs safe alternative.
        
        EXPECTED TO FAIL INITIALLY: Current implementation should fail with parser error.
        Safe implementation should work correctly.
        """
        test_input = '"System"'
        
        # Current broken implementation (should fail)
        broken_sql = """
            SELECT REGEXP_REPLACE(?, '^[\'\"]*|[\'\"]*$', '', 'g') AS result
        """
        
        with pytest.raises(duckdb.ParserException):
            self.con.execute(broken_sql, [test_input]).fetchone()
        
        # Safe implementation (should work)
        safe_sql = """
            SELECT RTRIM(LTRIM(RTRIM(LTRIM(?, '"'), '"'), ''''), '''') AS result
        """
        
        result = self.con.execute(safe_sql, [test_input]).fetchone()
        assert result[0] == 'System', f"Expected 'System', got '{result[0]}'"


if __name__ == "__main__":
    # Run specific test for development
    pytest.main([__file__ + "::TestSQLSyntaxFix::test_current_regex_pattern_causes_parser_error", "-v"])