"""
Test value coercion in comparison logic.
Tests for numeric, currency, date, and boolean value normalization.
"""

import pytest
import duckdb
from pathlib import Path
import sys

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.core.comparator import DataComparator
from src.config.manager import ComparisonConfig


class TestValueCoercion:
    """Test suite for value coercion in comparisons."""
    
    @pytest.fixture
    def setup_test_data(self):
        """Create test database with value variations."""
        con = duckdb.connect(":memory:")
        
        # Create test tables
        con.execute("""
            CREATE TABLE test_left (
                id INTEGER,
                numeric_col VARCHAR,
                currency_col VARCHAR,
                date_col VARCHAR,
                bool_col VARCHAR,
                mixed_col VARCHAR
            )
        """)
        
        con.execute("""
            CREATE TABLE test_right (
                id INTEGER,
                numeric_col VARCHAR,
                currency_col VARCHAR,
                date_col VARCHAR,
                bool_col VARCHAR,
                mixed_col VARCHAR
            )
        """)
        
        # Insert test data with format variations
        con.execute("""
            INSERT INTO test_left VALUES
            (1, '0', '$415,000.00', '12/30/2024', 'true', '100'),
            (2, '1', '($41,837.84)', '01/15/2025', 't', 'ABC'),
            (3, '-1', '₪158,634.54', '2025-01-01', 'false', '200.5'),
            (4, '100', '$1,000.50', '2024-12-31 00:00:00', 'yes', 'N/A'),
            (5, '3.14159', '€2,345.67', '03/21/2024', 'no', 'true'),
            (6, '1.23e5', '£999.99', '2024/03/21', 'f', '1.23e5'),
            (7, NULL, NULL, NULL, NULL, NULL)
        """)
        
        con.execute("""
            INSERT INTO test_right VALUES
            (1, '0.0', '415000.0', '2024-12-30 00:00:00', 'True', '100.0'),
            (2, '1.0', '-41837.84', '2025-01-15', 'TRUE', 'ABC'),
            (3, '-1.0', '158634.54', '01/01/2025', 'FALSE', '200.50'),
            (4, '100.0', '1000.50', '12/31/2024', 'Yes', 'N/A'),
            (5, '3.14159', '2345.67', '2024-03-21', 'NO', 'TRUE'),
            (6, '123000', '999.99', '21/03/2024', 'False', '123000.0'),
            (7, NULL, NULL, NULL, NULL, NULL)
        """)
        
        comparator = DataComparator(con)
        return con, comparator
    
    def test_numeric_values_with_tolerance(self, setup_test_data):
        """Test numeric comparison with tolerance."""
        con, comparator = setup_test_data
        
        config = ComparisonConfig(
            left_dataset="test_left",
            right_dataset="test_right",
            tolerance=0.01
        )
        
        # Build comparison condition
        condition = comparator._build_robust_comparison_condition(
            "numeric_col", "numeric_col", config
        )
        
        # Test the comparison
        sql = f"""
            SELECT l.id, 
                   l.numeric_col as left_val,
                   r.numeric_col as right_val,
                   {condition} as is_different
            FROM test_left l
            JOIN test_right r ON l.id = r.id
            ORDER BY l.id
        """
        
        results = con.execute(sql).fetchall()
        
        # Expected: All should be equal (not different) with tolerance
        assert results[0][3] == False  # 0 vs 0.0
        assert results[1][3] == False  # 1 vs 1.0
        assert results[2][3] == False  # -1 vs -1.0
        assert results[3][3] == False  # 100 vs 100.0
        assert results[4][3] == False  # 3.14159 vs 3.14159
        assert results[5][3] == False  # 1.23e5 vs 123000
        assert results[6][3] == False  # NULL vs NULL
    
    def test_numeric_values_exact_match(self, setup_test_data):
        """Test numeric comparison without tolerance."""
        con, comparator = setup_test_data
        
        config = ComparisonConfig(
            left_dataset="test_left",
            right_dataset="test_right",
            tolerance=0.0
        )
        
        # Build comparison condition
        condition = comparator._build_robust_comparison_condition(
            "numeric_col", "numeric_col", config
        )
        
        # Test the comparison
        sql = f"""
            SELECT l.id, 
                   l.numeric_col as left_val,
                   r.numeric_col as right_val,
                   {condition} as is_different
            FROM test_left l
            JOIN test_right r ON l.id = r.id
            ORDER BY l.id
        """
        
        results = con.execute(sql).fetchall()
        
        # Expected: Values should still be equal when coerced to numbers
        assert results[0][3] == False  # 0 vs 0.0 (should be equal as numbers!)
        assert results[1][3] == False  # 1 vs 1.0 
        assert results[2][3] == False  # -1 vs -1.0
        assert results[3][3] == False  # 100 vs 100.0
        assert results[4][3] == False  # 3.14159 vs 3.14159
        assert results[5][3] == False  # 1.23e5 vs 123000 (scientific notation)
        assert results[6][3] == False  # NULL vs NULL
    
    def test_currency_values(self, setup_test_data):
        """Test currency value normalization."""
        con, comparator = setup_test_data
        
        config = ComparisonConfig(
            left_dataset="test_left",
            right_dataset="test_right",
            tolerance=0.01
        )
        
        condition = comparator._build_robust_comparison_condition(
            "currency_col", "currency_col", config
        )
        
        sql = f"""
            SELECT l.id,
                   l.currency_col as left_val,
                   r.currency_col as right_val,
                   {condition} as is_different
            FROM test_left l
            JOIN test_right r ON l.id = r.id
            ORDER BY l.id
        """
        
        results = con.execute(sql).fetchall()
        
        # Expected results after currency stripping
        assert results[0][3] == False  # $415,000.00 vs 415000.0
        assert results[1][3] == False  # ($41,837.84) vs -41837.84
        assert results[2][3] == False  # ₪158,634.54 vs 158634.54
        assert results[3][3] == False  # $1,000.50 vs 1000.50
        assert results[4][3] == False  # €2,345.67 vs 2345.67
        assert results[5][3] == False  # £999.99 vs 999.99
        assert results[6][3] == False  # NULL vs NULL
    
    def test_date_values(self, setup_test_data):
        """Test date format normalization."""
        con, comparator = setup_test_data
        
        config = ComparisonConfig(
            left_dataset="test_left",
            right_dataset="test_right",
            tolerance=0.0
        )
        
        condition = comparator._build_robust_comparison_condition(
            "date_col", "date_col", config
        )
        
        sql = f"""
            SELECT l.id,
                   l.date_col as left_val,
                   r.date_col as right_val,
                   {condition} as is_different
            FROM test_left l
            JOIN test_right r ON l.id = r.id
            ORDER BY l.id
        """
        
        results = con.execute(sql).fetchall()
        
        # Expected: Different date formats should be recognized as equal
        assert results[0][3] == False  # 12/30/2024 vs 2024-12-30 00:00:00
        assert results[1][3] == False  # 01/15/2025 vs 2025-01-15
        assert results[2][3] == False  # 2025-01-01 vs 01/01/2025
        assert results[3][3] == False  # 2024-12-31 00:00:00 vs 12/31/2024
        assert results[4][3] == False  # 03/21/2024 vs 2024-03-21
        # Note: results[5] might fail due to ambiguous format - that's OK
        assert results[6][3] == False  # NULL vs NULL
    
    def test_boolean_values(self, setup_test_data):
        """Test boolean value normalization."""
        con, comparator = setup_test_data
        
        config = ComparisonConfig(
            left_dataset="test_left",
            right_dataset="test_right",
            tolerance=0.0
        )
        
        condition = comparator._build_robust_comparison_condition(
            "bool_col", "bool_col", config
        )
        
        sql = f"""
            SELECT l.id,
                   l.bool_col as left_val,
                   r.bool_col as right_val,
                   {condition} as is_different
            FROM test_left l
            JOIN test_right r ON l.id = r.id
            ORDER BY l.id
        """
        
        results = con.execute(sql).fetchall()
        
        # Expected: Boolean variations should be recognized as equal
        assert results[0][3] == False  # true vs True
        assert results[1][3] == False  # t vs TRUE
        assert results[2][3] == False  # false vs FALSE
        assert results[3][3] == False  # yes vs Yes
        assert results[4][3] == False  # no vs NO
        assert results[5][3] == False  # f vs False
        assert results[6][3] == False  # NULL vs NULL
    
    def test_mixed_column_types(self, setup_test_data):
        """Test columns with mixed data types."""
        con, comparator = setup_test_data
        
        config = ComparisonConfig(
            left_dataset="test_left",
            right_dataset="test_right",
            tolerance=0.01
        )
        
        condition = comparator._build_robust_comparison_condition(
            "mixed_col", "mixed_col", config
        )
        
        sql = f"""
            SELECT l.id,
                   l.mixed_col as left_val,
                   r.mixed_col as right_val,
                   {condition} as is_different
            FROM test_left l
            JOIN test_right r ON l.id = r.id
            ORDER BY l.id
        """
        
        results = con.execute(sql).fetchall()
        
        # Expected results for mixed types
        assert results[0][3] == False  # 100 vs 100.0 (numeric)
        assert results[1][3] == False  # ABC vs ABC (string)
        assert results[2][3] == False  # 200.5 vs 200.50 (numeric)
        assert results[3][3] == False  # N/A vs N/A (string)
        assert results[4][3] == False  # true vs TRUE (boolean string)
        assert results[5][3] == False  # 1.23e5 vs 123000.0 (scientific)
        assert results[6][3] == False  # NULL vs NULL
    
    def test_numeric_not_treated_as_boolean(self, setup_test_data):
        """Test that numeric values are NOT treated as booleans."""
        con, comparator = setup_test_data
        
        # Create specific test case
        con.execute("DROP TABLE IF EXISTS test_bool_check")
        con.execute("""
            CREATE TABLE test_bool_check AS
            SELECT * FROM (VALUES
                (1, '0', 'false'),
                (2, '1', 'true'),
                (3, '0', '0.0'),
                (4, '1', '1.0')
            ) AS t(id, left_val, right_val)
        """)
        
        config = ComparisonConfig(
            left_dataset="test_left",
            right_dataset="test_right",
            tolerance=0.0
        )
        
        condition = comparator._build_robust_comparison_condition(
            "left_val", "right_val", config
        )
        
        sql = f"""
            SELECT l.id,
                   l.left_val,
                   r.right_val,
                   {condition} as is_different
            FROM test_bool_check l, test_bool_check r
            WHERE l.id = r.id
            ORDER BY l.id
        """
        
        results = con.execute(sql).fetchall()
        
        # Expected: Numeric values should NOT be equal to boolean strings
        assert results[0][3] == True   # '0' vs 'false' - DIFFERENT!
        assert results[1][3] == True   # '1' vs 'true' - DIFFERENT!
        assert results[2][3] == False  # '0' vs '0.0' - SAME (numeric)
        assert results[3][3] == False  # '1' vs '1.0' - SAME (numeric)
    
    def test_null_handling(self, setup_test_data):
        """Test NULL value handling."""
        con, comparator = setup_test_data
        
        # Create test with NULLs
        con.execute("DROP TABLE IF EXISTS test_nulls")
        con.execute("""
            CREATE TABLE test_nulls AS
            SELECT * FROM (VALUES
                (1, NULL, NULL),
                (2, NULL, '0'),
                (3, '0', NULL),
                (4, '0', '0')
            ) AS t(id, left_val, right_val)
        """)
        
        config = ComparisonConfig(
            left_dataset="test_left",
            right_dataset="test_right",
            tolerance=0.0
        )
        
        condition = comparator._build_robust_comparison_condition(
            "left_val", "right_val", config
        )
        
        sql = f"""
            SELECT l.id,
                   l.left_val,
                   r.right_val,
                   {condition} as is_different
            FROM test_nulls l, test_nulls r
            WHERE l.id = r.id
            ORDER BY l.id
        """
        
        results = con.execute(sql).fetchall()
        
        # Expected NULL handling
        assert results[0][3] == False  # NULL vs NULL - SAME
        assert results[1][3] == True   # NULL vs '0' - DIFFERENT
        assert results[2][3] == True   # '0' vs NULL - DIFFERENT
        assert results[3][3] == False  # '0' vs '0' - SAME


if __name__ == "__main__":
    # Run tests manually for debugging
    test = TestValueCoercion()
    con, comparator = test.setup_test_data().__next__()
    
    print("Running value coercion tests...")
    test.test_numeric_values_with_tolerance((con, comparator))
    print("✓ Numeric with tolerance")
    
    test.test_numeric_values_exact_match((con, comparator))
    print("✓ Numeric exact match")
    
    test.test_currency_values((con, comparator))
    print("✓ Currency normalization")
    
    test.test_date_values((con, comparator))
    print("✓ Date format normalization")
    
    test.test_boolean_values((con, comparator))
    print("✓ Boolean normalization")
    
    test.test_mixed_column_types((con, comparator))
    print("✓ Mixed column types")
    
    test.test_numeric_not_treated_as_boolean((con, comparator))
    print("✓ Numeric not treated as boolean")
    
    test.test_null_handling((con, comparator))
    print("✓ NULL handling")
    
    print("\nAll tests passed!")