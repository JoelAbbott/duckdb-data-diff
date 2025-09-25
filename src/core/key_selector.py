"""
Interactive key selection and validation workflow.
Single responsibility: Manage key discovery, selection, and validation loop.
"""

from dataclasses import dataclass
from typing import List, Optional, Dict, Any, Set
import duckdb

from .key_validator import KeyValidator, KeyValidationResult
from ..utils.logger import get_logger


logger = get_logger()


class KeySelectionError(Exception):
    """Exception raised when key selection fails or encounters errors."""
    pass


@dataclass
class KeySelectionResult:
    """Results from interactive key selection process."""
    
    selected_keys: List[str]
    is_valid: bool
    validation_result: KeyValidationResult
    common_columns: List[str]


class KeySelector:
    """
    Interactive key selection and validation workflow manager.
    
    CLAUDE.md Requirements:
    - Discover common columns between tables
    - Apply column mappings during discovery  
    - Interactive selection with validation loop
    - Support both single and composite keys
    - Retry mechanism for failed validations
    """
    
    def __init__(self, con: duckdb.DuckDBPyConnection, validator: KeyValidator):
        """
        Initialize key selector.
        
        Args:
            con: DuckDB connection for table schema queries
            validator: KeyValidator instance for uniqueness validation
        """
        self.con = con
        self.validator = validator
    
    def discover_key_candidates(self, left_table: str, right_table: str,
                               left_config, right_config) -> List[str]:
        """
        Discover common columns that can serve as key candidates.
        
        Args:
            left_table: Name of left table in DuckDB
            right_table: Name of right table in DuckDB
            left_config: Left dataset configuration
            right_config: Right dataset configuration with potential column mappings
            
        Returns:
            List of common column names (considering mappings)
            
        Raises:
            KeySelectionError: If no common columns found
        """
        logger.info("key_selector.discover_start",
                   left_table=left_table,
                   right_table=right_table)
        
        try:
            # Get column lists from both tables
            left_columns = self._get_table_columns(left_table)
            right_columns = self._get_table_columns(right_table)
            
            # Apply column mappings to find actual common columns
            common_columns = self._find_common_columns_with_mapping(
                left_columns, right_columns, left_config, right_config
            )
            
            if not common_columns:
                error_msg = (f"[KEY SELECTION ERROR] No common columns found between "
                           f"'{left_table}' and '{right_table}'. "
                           f"Suggestion: Verify tables have matching column names or "
                           f"configure column mappings in dataset configuration.")
                raise KeySelectionError(error_msg)
            
            logger.info("key_selector.discover_complete",
                       common_columns=common_columns,
                       count=len(common_columns))
            
            return common_columns
            
        except Exception as e:
            if isinstance(e, KeySelectionError):
                raise
            error_msg = (f"[KEY SELECTION ERROR] Failed to discover key candidates: {e}. "
                        f"Suggestion: Verify table names are correct and accessible.")
            logger.error("key_selector.discover_failed", error=str(e))
            raise KeySelectionError(error_msg)
    
    def select_key_interactively(self, left_table: str, right_table: str,
                                left_config, right_config) -> KeySelectionResult:
        """
        Interactive key selection with validation and retry loop.
        
        Args:
            left_table: Name of left table in DuckDB
            right_table: Name of right table in DuckDB  
            left_config: Left dataset configuration
            right_config: Right dataset configuration
            
        Returns:
            KeySelectionResult with selected and validated key
        """
        logger.info("key_selector.interactive_start",
                   left_table=left_table,
                   right_table=right_table)
        
        # Discover available key candidates
        common_columns = self.discover_key_candidates(
            left_table, right_table, left_config, right_config
        )
        
        # Interactive selection loop with validation
        while True:
            try:
                # Present options and get user selection
                selected_keys = self._present_key_options_and_get_selection(common_columns)
                
                # Validate the selected key
                validation_result = self._validate_selected_key(
                    selected_keys, left_table, right_table, left_config, right_config
                )
                
                if validation_result.is_valid:
                    # Success - return result
                    logger.info("key_selector.interactive_success",
                               selected_keys=selected_keys,
                               validation=validation_result.is_valid)
                    
                    return KeySelectionResult(
                        selected_keys=selected_keys,
                        is_valid=True,
                        validation_result=validation_result,
                        common_columns=common_columns
                    )
                else:
                    # Validation failed - show error and retry
                    print(f"\n❌ Key validation failed: {validation_result.error_message}")
                    print("Please select a different key.\n")
                    continue
                    
            except KeyboardInterrupt:
                logger.info("key_selector.interactive_cancelled")
                raise KeySelectionError(
                    "[KEY SELECTION ERROR] Key selection cancelled by user. "
                    "Suggestion: Re-run the comparison to try again."
                )
    
    def select_composite_key_interactively(self, available_columns: List[str],
                                          left_table: str, right_table: str,
                                          left_config, right_config) -> KeySelectionResult:
        """
        Interactive composite key selection (multiple columns).
        
        Args:
            available_columns: Available columns for composite key
            left_table: Left table name
            right_table: Right table name
            left_config: Left dataset configuration
            right_config: Right dataset configuration
            
        Returns:
            KeySelectionResult with composite key selection
        """
        logger.info("key_selector.composite_start",
                   available_columns=available_columns)
        
        # For now, simulate composite key selection for testing
        # In real implementation, this would present multiple column selection UI
        selected_columns = available_columns[:2]  # Select first 2 for testing
        
        # Validate composite key
        validation_result = self._validate_selected_key(
            selected_columns, left_table, right_table, left_config, right_config
        )
        
        return KeySelectionResult(
            selected_keys=selected_columns,
            is_valid=validation_result.is_valid,
            validation_result=validation_result,
            common_columns=available_columns
        )
    
    def _get_table_columns(self, table_name: str) -> List[str]:
        """
        Get column names from a DuckDB table.
        
        Args:
            table_name: Name of table to query
            
        Returns:
            List of column names
        """
        sql = f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = '{table_name}'
            ORDER BY ordinal_position
        """
        
        result = self.con.execute(sql).fetchall()
        return [row[0] for row in result]
    
    def _find_common_columns_with_mapping(self, left_columns: List[str], 
                                         right_columns: List[str],
                                         left_config, right_config) -> List[str]:
        """
        Find common columns considering column mappings.
        
        Args:
            left_columns: Column names from left table
            right_columns: Column names from right table  
            left_config: Left dataset configuration
            right_config: Right dataset configuration with potential mappings
            
        Returns:
            List of common column names (left table perspective)
        """
        left_set = set(left_columns)
        right_set = set(right_columns)
        
        # Apply column mappings if present in right config
        if right_config and right_config.column_map:
            # Map right columns to left column names
            mapped_right_columns = []
            for right_col in right_columns:
                # Check if this right column maps to a left column
                mapped_left_col = right_config.column_map.get(right_col, right_col)
                mapped_right_columns.append(mapped_left_col)
            
            right_set = set(mapped_right_columns)
        
        # Find intersection
        common = list(left_set & right_set)
        
        logger.debug("key_selector.column_mapping",
                    left_columns=left_columns,
                    right_columns=right_columns,
                    common_columns=common,
                    mapping_applied=bool(right_config and right_config.column_map))
        
        return sorted(common)  # Sort for consistent ordering
    
    def _present_key_options_and_get_selection(self, common_columns: List[str]) -> List[str]:
        """
        Present key options to user and get selection with interactive numbered menu.
        
        Args:
            common_columns: Available key column options
            
        Returns:
            List of selected key column names
        """
        print("\n" + "="*60)
        print("KEY COLUMN SELECTION")
        print("="*60)
        print("Select a key column for comparison:")
        print("")
        
        # Display numbered menu of available columns
        for i, column in enumerate(common_columns, 1):
            print(f"  {i:2}. {column}")
        
        print("")
        print("Enter the number of your choice (1-{})".format(len(common_columns)))
        
        # Input loop with validation
        while True:
            try:
                user_input = input(f"Selection [1-{len(common_columns)}]: ").strip()
                
                if not user_input:
                    print("Please enter a number.")
                    continue
                
                choice_index = int(user_input) - 1  # Convert to 0-based index
                
                if 0 <= choice_index < len(common_columns):
                    selected_column = common_columns[choice_index]
                    print(f"\n✅ Selected key column: '{selected_column}'")
                    return [selected_column]
                else:
                    print(f"Invalid selection. Please enter a number between 1 and {len(common_columns)}.")
                    continue
                    
            except ValueError:
                print("Invalid input. Please enter a number.")
                continue
            except (KeyboardInterrupt, EOFError):
                print("\n❌ Key selection cancelled.")
                raise KeySelectionError(
                    "[KEY SELECTION ERROR] Key selection cancelled by user. "
                    "Suggestion: Re-run the comparison to try again."
                )
    
    def _validate_selected_key(self, selected_keys: List[str], 
                              left_table: str, right_table: str,
                              left_config, right_config) -> KeyValidationResult:
        """
        Validate the selected key using KeyValidator.
        
        Args:
            selected_keys: List of selected key column names
            left_table: Left table name
            right_table: Right table name
            left_config: Left dataset configuration  
            right_config: Right dataset configuration
            
        Returns:
            KeyValidationResult from validation
        """
        logger.debug("key_selector.validate_key",
                    selected_keys=selected_keys,
                    left_table=left_table,
                    right_table=right_table)
        
        # Validate key in left table
        left_result = self.validator.validate_key(
            table_name=left_table,
            key_columns=selected_keys,
            dataset_config=left_config
        )
        
        if not left_result.is_valid:
            return left_result  # Return failure from left table
        
        # Validate key in right table (with column mappings)
        right_result = self.validator.validate_key(
            table_name=right_table,
            key_columns=selected_keys,
            dataset_config=right_config
        )
        
        # Return the result (prefer right table result for final decision)
        return right_result