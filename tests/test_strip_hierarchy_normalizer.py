"""
Unit tests for strip_hierarchy normalizer functionality.

Tests the hierarchical data normalization logic used to eliminate false positive
differences in name and full_name columns by extracting the final component
from colon-delimited hierarchical strings.
"""

import pytest
from unittest.mock import Mock, patch
import duckdb

from src.utils.normalizers import strip_hierarchy
from src.config.manager import DatasetConfig


class TestStripHierarchyNormalizer:
    """
    Test suite for strip_hierarchy normalizer functionality.
    
    Validates the core logic that processes hierarchical department names
    like "100 - Operations : 110 Operations" → "110 Operations"
    """
    
    # Test data structure as defined in the plan
    test_data_hierarchy = [
        ("100 - Operations : 110 Operations", "110 Operations"),
        ("400 - Construction : 410 CTO", "410 CTO"), 
        ("400 - Construction : 420 - Construction Indirect", "420 - Construction Indirect"),
        ("Simple Department", "Simple Department"),
        ("Parent : Child : Grandchild", "Grandchild"),
        ("Level1 : Level2 : Level3 : Final", "Final"),
        ("", ""),  # Empty string
        ("OnlyName", "OnlyName"),  # No hierarchy
        ("Name : ", ""),  # Empty after colon
        (" : Final Part ", "Final Part"),  # Whitespace handling
    ]
    
    def test_strip_hierarchy_basic_functionality(self):
        """
        Test the core hierarchy stripping logic.
        
        Validates the primary use case from the data report:
        "100 - Operations : 110 Operations" should become "110 Operations"
        """
        input_value = "100 - Operations : 110 Operations"
        expected_output = "110 Operations"
        
        result = strip_hierarchy(input_value)
        
        assert result == expected_output, (
            f"Expected '{expected_output}', got '{result}' for input '{input_value}'"
        )
    
    def test_strip_hierarchy_construction_example(self):
        """
        Test the construction department example from the data report.
        
        Validates the specific case: "400 - Construction : 410 CTO" → "410 CTO"
        """
        input_value = "400 - Construction : 410 CTO"
        expected_output = "410 CTO"
        
        result = strip_hierarchy(input_value)
        
        assert result == expected_output, (
            f"Expected '{expected_output}', got '{result}' for input '{input_value}'"
        )
    
    def test_strip_hierarchy_multiple_levels(self):
        """
        Test handling of deep hierarchical structures.
        
        Validates that the function correctly extracts the final component
        from multi-level hierarchies with multiple colon delimiters.
        """
        test_cases = [
            ("400 - Construction : 420 - Construction Indirect : 421 Specific", "421 Specific"),
            ("Level1 : Level2 : Level3 : Final Component", "Final Component"),
            ("A : B : C : D : E", "E"),
        ]
        
        for input_value, expected_output in test_cases:
            result = strip_hierarchy(input_value)
            assert result == expected_output, (
                f"Expected '{expected_output}', got '{result}' for input '{input_value}'"
            )
    
    def test_strip_hierarchy_edge_cases(self):
        """
        Test edge cases and boundary conditions.
        
        Validates proper handling of:
        - Non-hierarchical strings (no colon)
        - Empty strings and whitespace
        - Non-string input types
        - Malformed hierarchical data
        """
        # Test non-hierarchical string (no colon)
        assert strip_hierarchy("Simple Name") == "Simple Name"
        
        # Test empty string after colon
        assert strip_hierarchy("Name : ") == ""
        
        # Test whitespace handling
        assert strip_hierarchy(" : Final Part ") == "Final Part"
        
        # Test empty string
        assert strip_hierarchy("") == ""
        
        # Test single colon
        assert strip_hierarchy(":") == ""
        
        # Test multiple consecutive colons
        assert strip_hierarchy("Start :: : End") == "End"
        
        # Test non-string input (should return unchanged)
        assert strip_hierarchy(None) == None
        assert strip_hierarchy(123) == 123
        assert strip_hierarchy([1, 2, 3]) == [1, 2, 3]
        
        # Test with only whitespace after colon
        assert strip_hierarchy("Parent :   ") == ""
        
        # Test with colon at the beginning
        assert strip_hierarchy(": Only Child") == "Only Child"
    
    @pytest.mark.parametrize("input_value,expected_output", test_data_hierarchy)
    def test_strip_hierarchy_with_test_data(self, input_value, expected_output):
        """
        Parameterized test using the test_data_hierarchy structure.
        
        Runs the strip_hierarchy function against all test cases defined
        in the test_data_hierarchy list to ensure comprehensive coverage.
        """
        result = strip_hierarchy(input_value)
        assert result == expected_output, (
            f"Expected '{expected_output}', got '{result}' for input '{input_value}'"
        )
    
    def test_strip_hierarchy_with_config_integration(self):
        """
        Integration test for strip_hierarchy with configuration system.
        
        Tests that the normalizer integrates correctly with the DatasetConfig
        and pipeline components. Uses mocking to avoid complex DuckDB setup.
        """
        # Create a mock dataset configuration with strip_hierarchy normalizer
        dataset_config = DatasetConfig(
            name="test_dataset",
            path="test/path.csv",
            type="csv",
            key_columns=["id"],
            normalizers={
                "name": "strip_hierarchy",
                "full_name": "strip_hierarchy"
            }
        )
        
        # Verify the configuration is set up correctly
        assert "name" in dataset_config.normalizers
        assert "full_name" in dataset_config.normalizers
        assert dataset_config.normalizers["name"] == "strip_hierarchy"
        assert dataset_config.normalizers["full_name"] == "strip_hierarchy"
        
        # Test the normalizer function with realistic hierarchical data
        test_data = [
            "100 - Operations : 110 Operations",
            "400 - Construction : 410 CTO", 
            "500 - Finance : 510 - Accounting : 511 Payroll"
        ]
        
        expected_results = [
            "110 Operations",
            "410 CTO",
            "511 Payroll"
        ]
        
        for input_val, expected in zip(test_data, expected_results):
            result = strip_hierarchy(input_val)
            assert result == expected, (
                f"Integration test failed: expected '{expected}', got '{result}'"
            )
    
    def test_strip_hierarchy_realistic_data_patterns(self):
        """
        Test with realistic data patterns found in business systems.
        
        Validates the normalizer works correctly with actual patterns that
        might be found in NetSuite or similar ERP systems.
        """
        realistic_patterns = [
            # NetSuite-style department hierarchies
            ("100 - Operations : 110 Operations", "110 Operations"),
            ("200 - Sales : 210 - Inside Sales : 211 Inbound", "211 Inbound"),
            ("300 - Engineering : 310 - Software : 311 Backend", "311 Backend"),
            
            # Different formatting variations
            ("HR : Human Resources : Recruiting", "Recruiting"), 
            ("Finance:Accounting:AP", "AP"),  # No spaces around colons
            ("Legal : : Compliance", "Compliance"),  # Empty middle section
            
            # Edge cases with numbers and special characters
            ("001 - Admin : 002 - IT : 003-Support", "003-Support"),
            ("Dept-A : Sub-Dept-B : Team-C-2024", "Team-C-2024"),
        ]
        
        for input_val, expected_output in realistic_patterns:
            result = strip_hierarchy(input_val)
            assert result == expected_output, (
                f"Realistic pattern test failed for '{input_val}': "
                f"expected '{expected_output}', got '{result}'"
            )
    
    def test_strip_hierarchy_preserves_original_for_non_hierarchical(self):
        """
        Test that non-hierarchical values are preserved unchanged.
        
        Ensures that department names without hierarchy markers are not
        modified, preventing data corruption for simple department names.
        """
        non_hierarchical_values = [
            "Operations",
            "Human Resources", 
            "Finance & Accounting",
            "IT Support",
            "Sales Team",
            "Executive Leadership",
            "R&D Department",
            "Customer Service",
            "Marketing Communications"
        ]
        
        for value in non_hierarchical_values:
            result = strip_hierarchy(value)
            assert result == value, (
                f"Non-hierarchical value should be preserved unchanged: "
                f"'{value}' became '{result}'"
            )
    
    def test_strip_hierarchy_consistency(self):
        """
        Test that the function produces consistent results.
        
        Ensures that repeated calls with the same input produce identical
        output, validating the deterministic nature of the function.
        """
        test_input = "100 - Operations : 110 Operations"
        
        # Run the function multiple times
        results = [strip_hierarchy(test_input) for _ in range(5)]
        
        # All results should be identical
        assert all(result == results[0] for result in results), (
            f"Inconsistent results detected: {results}"
        )
        
        # Verify the expected result
        assert results[0] == "110 Operations"


if __name__ == "__main__":
    # Allow running tests directly with: python test_strip_hierarchy_normalizer.py
    pytest.main([__file__, "-v"])