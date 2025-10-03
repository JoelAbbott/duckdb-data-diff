"""
Test additional value coercion fixes for date and currency formats.
"""

import pytest
import duckdb
from pathlib import Path
import sys

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.core.comparator import DataComparator
from src.config.manager import ComparisonConfig


class TestAdditionalCoercionFixes:
    """Test suite for additional date and currency coercion fixes."""
    
    @pytest.fixture
    def setup_test_data(self):
        """Create test database with specific format issues."""
        con = duckdb.connect(":memory:")
        
        # Create test tables
        con.execute("""
            CREATE TABLE test_left (
                id INTEGER,
                date_col VARCHAR,
                currency_col VARCHAR
            )
        """)
        
        con.execute("""
            CREATE TABLE test_right (
                id INTEGER,
                date_col VARCHAR,
                currency_col VARCHAR
            )
        """)
        
        # Insert test data with the specific formats we're fixing
        # Note: Only dates with 0:00 (midnight) should be equal to date-only format
        con.execute("""
            INSERT INTO test_left VALUES
            (1, '1/20/2025', '-€ 30,848.00'),
            (2, '3/15/2024', '- € 1,234.56'),
            (3, '12/31/2023', '$   100.00'),
            (4, '6/1/2025', '£999.99')
        """)
        
        con.execute("""
            INSERT INTO test_right VALUES
            (1, '1/20/2025 0:00', '-30848.0'),
            (2, '3/15/2024 0:00', '-1234.56'),
            (3, '12/31/2023 0:00', '100.0'),
            (4, '6/1/2025 0:00', '999.99')
        """)
        
        comparator = DataComparator(con)
        return con, comparator
    
    def test_date_with_midnight_time_format(self, setup_test_data):
        """Test that dates with '0:00' (midnight) time format are recognized as equal to date-only format."""
        con, comparator = setup_test_data
        
        config = ComparisonConfig(
            left_dataset="test_left",
            right_dataset="test_right",
            tolerance=0.0
        )
        
        # Build comparison condition for date column
        condition = comparator._build_robust_comparison_condition(
            "date_col", "date_col", config
        )
        
        # Test the comparison
        sql = f"""
            SELECT l.id,
                   l.date_col as left_date,
                   r.date_col as right_date,
                   {condition} as is_different
            FROM test_left l
            JOIN test_right r ON l.id = r.id
            ORDER BY l.id
        """
        
        results = con.execute(sql).fetchall()
        
        # Dates with 0:00 (midnight) time should be equal to date-only format
        # Both parse to the same timestamp (midnight)
        assert results[0][3] == False  # '1/20/2025' vs '1/20/2025 0:00' (both midnight)
        assert results[1][3] == False  # '3/15/2024' vs '3/15/2024 0:00' (both midnight)
        assert results[2][3] == False  # '12/31/2023' vs '12/31/2023 0:00' (both midnight)
        assert results[3][3] == False  # '6/1/2025' vs '6/1/2025 0:00' (both midnight)
    
    def test_currency_with_spaces(self, setup_test_data):
        """Test that currency values with spaces are handled correctly."""
        con, comparator = setup_test_data
        
        config = ComparisonConfig(
            left_dataset="test_left",
            right_dataset="test_right",
            tolerance=0.01
        )
        
        # Build comparison condition for currency column
        condition = comparator._build_robust_comparison_condition(
            "currency_col", "currency_col", config
        )
        
        # Test the comparison
        sql = f"""
            SELECT l.id,
                   l.currency_col as left_curr,
                   r.currency_col as right_curr,
                   {condition} as is_different
            FROM test_left l
            JOIN test_right r ON l.id = r.id
            ORDER BY l.id
        """
        
        results = con.execute(sql).fetchall()
        
        # All currency values should be equal after normalization
        assert results[0][3] == False  # '-€ 30,848.00' vs '-30848.0'
        assert results[1][3] == False  # '- € 1,234.56' vs '-1234.56'
        assert results[2][3] == False  # '$   100.00' vs '100.0'
        assert results[3][3] == False  # '£999.99' vs '999.99'
    
    def test_specific_reported_cases(self, setup_test_data):
        """Test the exact cases reported by the user."""
        con, comparator = setup_test_data
        
        # Test case 1: Date with 0:00 time
        con.execute("""
            DROP TABLE IF EXISTS specific_test;
            CREATE TABLE specific_test AS
            SELECT * FROM (VALUES
                (1, '1/20/2025', '1/20/2025 0:00'),
                (2, '-€ 30,848.00', '-30848.0')
            ) AS t(id, left_val, right_val)
        """)
        
        config = ComparisonConfig(
            left_dataset="test_left",
            right_dataset="test_right",
            tolerance=0.0
        )
        
        # Test date comparison
        date_condition = comparator._build_robust_comparison_condition(
            "left_val", "right_val", config
        )
        
        sql = f"""
            SELECT t.id, t.left_val, t.right_val,
                   {date_condition} as is_different
            FROM specific_test t
            JOIN (SELECT * FROM specific_test) l ON TRUE
            JOIN (SELECT * FROM specific_test) r ON TRUE
            WHERE t.id = 1
        """
        
        result = con.execute(sql).fetchone()
        assert result[3] == False, f"Date '1/20/2025' vs '1/20/2025 0:00' should be equal"
        
        # Test currency comparison
        config_with_tolerance = ComparisonConfig(
            left_dataset="test_left",
            right_dataset="test_right",
            tolerance=0.01
        )
        
        currency_condition = comparator._build_robust_comparison_condition(
            "left_val", "right_val", config_with_tolerance
        )
        
        sql = f"""
            SELECT t.id, t.left_val, t.right_val,
                   {currency_condition} as is_different
            FROM specific_test t
            JOIN (SELECT * FROM specific_test) l ON TRUE
            JOIN (SELECT * FROM specific_test) r ON TRUE
            WHERE t.id = 2
        """
        
        result = con.execute(sql).fetchone()
        assert result[3] == False, f"Currency '-€ 30,848.00' vs '-30848.0' should be equal"
    
    def test_no_regression_on_existing_formats(self, setup_test_data):
        """Ensure we don't break existing date/currency handling."""
        con, comparator = setup_test_data
        
        # Test that normal formats still work
        con.execute("""
            DROP TABLE IF EXISTS regression_test;
            CREATE TABLE regression_test AS
            SELECT * FROM (VALUES
                (1, '2024-01-01', '2024-01-01'),
                (2, '$100.00', '100.0'),
                (3, '12/25/2024', '2024-12-25'),
                (4, '(50.00)', '-50.0')
            ) AS t(id, left_val, right_val)
        """)
        
        config = ComparisonConfig(
            left_dataset="test_left",
            right_dataset="test_right",
            tolerance=0.01
        )
        
        condition = comparator._build_robust_comparison_condition(
            "left_val", "right_val", config
        )
        
        sql = f"""
            SELECT l.id, l.left_val, r.right_val,
                   {condition} as is_different
            FROM regression_test l, regression_test r
            WHERE l.id = r.id
            ORDER BY l.id
        """
        
        results = con.execute(sql).fetchall()
        
        # All should still be recognized as equal
        for i, row in enumerate(results):
            assert row[3] == False, f"Row {i+1}: '{row[1]}' vs '{row[2]}' should be equal"
    
    def test_quote_trimming(self, setup_test_data):
        """Test that leading/trailing quotes are trimmed in string comparison."""
        con, comparator = setup_test_data
        
        # Create test data with quote issues
        con.execute("DROP TABLE IF EXISTS quote_test")
        con.execute("""
            CREATE TABLE quote_test (
                id INTEGER,
                left_val VARCHAR,
                right_val VARCHAR
            )
        """)
        
        # Insert data using parameterized queries to avoid quote escaping issues
        con.execute("INSERT INTO quote_test VALUES (?, ?, ?)", (1, "'-System-", "-System-"))
        con.execute("INSERT INTO quote_test VALUES (?, ?, ?)", (2, '"Value"', "Value"))
        con.execute("INSERT INTO quote_test VALUES (?, ?, ?)", (3, "O'Brien", "O'Brien"))
        con.execute("INSERT INTO quote_test VALUES (?, ?, ?)", (4, "Normal", "Normal"))
        
        config = ComparisonConfig(
            left_dataset="test_left",
            right_dataset="test_right",
            tolerance=0.0
        )
        
        condition = comparator._build_robust_comparison_condition(
            "left_val", "right_val", config
        )
        
        sql = f"""
            SELECT l.id, l.left_val, r.right_val,
                   {condition} as is_different
            FROM quote_test l
            JOIN quote_test r ON l.id = r.id
            ORDER BY l.id
        """
        
        results = con.execute(sql).fetchall()
        
        # All should be equal after quote trimming
        assert results[0][3] == False, f"'-System- vs -System- should be equal after quote trimming"
        assert results[1][3] == False, f'"Value" vs Value should be equal after quote trimming'
        assert results[2][3] == False, f"O'Brien vs O'Brien should remain equal (internal quote preserved)"
        assert results[3][3] == False, f"Normal vs Normal should remain equal"


if __name__ == "__main__":
    # Run tests manually for debugging
    test = TestAdditionalCoercionFixes()
    con, comparator = test.setup_test_data().__next__()
    
    print("Running additional coercion tests...")
    test.test_date_with_time_format((con, comparator))
    print("✓ Date with time format")
    
    test.test_currency_with_spaces((con, comparator))
    print("✓ Currency with spaces")
    
    test.test_specific_reported_cases((con, comparator))
    print("✓ Specific reported cases")
    
    test.test_no_regression_on_existing_formats((con, comparator))
    print("✓ No regression on existing formats")
    
    print("\nAll additional tests passed!")