"""
Key uniqueness validation using DuckDB.
Single responsibility: Validate key column uniqueness for comparison tables.
"""

from dataclasses import dataclass
from typing import List, Optional, Dict, Any
import duckdb

from ..utils.logger import get_logger


logger = get_logger()


class KeyValidationError(Exception):
    """Exception raised when key validation fails or encounters errors."""
    pass


@dataclass
class KeyValidationResult:
    """Results from key uniqueness validation."""
    
    is_valid: bool
    total_rows: int
    unique_values: int
    duplicate_count: int
    error_message: Optional[str] = None


class KeyValidator:
    """
    Validates key column uniqueness using DuckDB queries.
    
    CLAUDE.md Requirements:
    - Use DuckDB to check uniqueness with specified query patterns
    - Support composite keys
    - Apply column mappings from dataset configs
    - Memory-efficient processing for large datasets
    """
    
    def __init__(self, con: duckdb.DuckDBPyConnection):
        """
        Initialize key validator.
        
        Args:
            con: DuckDB connection for executing validation queries
        """
        self.con = con
    
    def validate_key(self, table_name: str, key_columns: List[str], 
                    dataset_config) -> KeyValidationResult:
        """
        Validate uniqueness of key columns in a table.
        
        Args:
            table_name: Name of table in DuckDB
            key_columns: List of column names to validate as key
            dataset_config: Dataset configuration with potential column mappings
            
        Returns:
            KeyValidationResult with validation status and statistics
            
        Raises:
            KeyValidationError: If validation fails or encounters errors
        """
        logger.info("key_validator.validate_start",
                   table=table_name,
                   key_columns=key_columns)
        
        # Validate input parameters
        self._validate_inputs(table_name, key_columns)
        
        # Apply column mappings if present
        mapped_columns = self._apply_column_mappings(key_columns, dataset_config)
        
        try:
            if len(mapped_columns) == 1:
                # Single column validation
                result = self._validate_single_column(table_name, mapped_columns[0])
            else:
                # Composite key validation  
                result = self._validate_composite_key(table_name, mapped_columns)
                
            logger.info("key_validator.validate_complete",
                       table=table_name,
                       is_valid=result.is_valid,
                       duplicates=result.duplicate_count)
            
            return result
            
        except Exception as e:
            error_msg = (f"[KEY VALIDATION ERROR] Failed to validate keys in table '{table_name}': {e}. "
                        f"Suggestion: Verify table exists and columns {mapped_columns} are valid.")
            logger.error("key_validator.validate_failed",
                        table=table_name,
                        error=str(e))
            raise KeyValidationError(error_msg)
    
    def _validate_inputs(self, table_name: str, key_columns: List[str]) -> None:
        """
        Validate input parameters for key validation.
        
        Args:
            table_name: Name of table to validate
            key_columns: List of key column names
            
        Raises:
            KeyValidationError: If inputs are invalid
        """
        if not table_name or not table_name.strip():
            raise KeyValidationError(
                "[KEY VALIDATION ERROR] Table name cannot be empty. "
                "Suggestion: Provide a valid table name."
            )
        
        if not key_columns:
            raise KeyValidationError(
                "[KEY VALIDATION ERROR] key_columns cannot be empty. "
                "Suggestion: Provide at least one column name for key validation."
            )
    
    def _apply_column_mappings(self, key_columns: List[str], dataset_config) -> List[str]:
        """
        Apply column mappings from dataset configuration.
        
        Args:
            key_columns: Original key column names
            dataset_config: Dataset config with potential column_map
            
        Returns:
            List of mapped column names
        """
        if not dataset_config or not dataset_config.column_map:
            return key_columns
        
        mapped_columns = []
        for col in key_columns:
            # For right table: column_map maps right_col -> left_col
            # We need to find the right column name that maps to this left column
            mapped_col = self._get_mapped_column_name(col, dataset_config.column_map)
            mapped_columns.append(mapped_col)
            
        logger.debug("key_validator.column_mapping",
                    original=key_columns,
                    mapped=mapped_columns)
        
        return mapped_columns
    
    def _get_mapped_column_name(self, left_column: str, column_map: Dict[str, str]) -> str:
        """
        Get mapped column name for right table validation.
        
        Args:
            left_column: Left table column name
            column_map: Mapping from right column -> left column
            
        Returns:
            Right table column name (mapped) or original if no mapping
        """
        # Find the right column that maps to this left column
        for right_col, left_col in column_map.items():
            if left_col == left_column:
                return right_col
        
        # No mapping found, use original column name
        return left_column
    
    def _validate_single_column(self, table_name: str, column_name: str) -> KeyValidationResult:
        """
        Validate uniqueness of single column key.
        
        Args:
            table_name: Name of table to validate
            column_name: Column name to validate
            
        Returns:
            KeyValidationResult with validation status
        """
        # DuckDB query to check single column uniqueness
        quoted_column = self._quote_identifier(column_name)
        sql = f"""
            SELECT COUNT(*) as total_rows, 
                   COUNT(DISTINCT {quoted_column}) as unique_values
            FROM {table_name}
            WHERE {quoted_column} IS NOT NULL
        """
        
        logger.debug("key_validator.single_column_sql", sql=sql.strip())
        
        result = self.con.execute(sql).fetchone()
        total_rows, unique_values = result
        
        duplicate_count = total_rows - unique_values
        is_valid = duplicate_count == 0
        
        error_message = None
        if not is_valid:
            error_message = (f"{duplicate_count} duplicates detected in column '{column_name}'. "
                           f"Key validation failed.")
        
        return KeyValidationResult(
            is_valid=is_valid,
            total_rows=total_rows,
            unique_values=unique_values,
            duplicate_count=duplicate_count,
            error_message=error_message
        )
    
    def _validate_composite_key(self, table_name: str, key_columns: List[str]) -> KeyValidationResult:
        """
        Validate uniqueness of composite key columns.
        
        Args:
            table_name: Name of table to validate
            key_columns: List of column names forming composite key
            
        Returns:
            KeyValidationResult with validation status
        """
        # Build column list for GROUP BY with proper quoting
        quoted_columns = [self._quote_identifier(col) for col in key_columns]
        columns_str = ", ".join(quoted_columns)
        
        # Build WHERE clause for non-null check with proper quoting
        where_conditions = " AND ".join([f"{self._quote_identifier(col)} IS NOT NULL" for col in key_columns])
        
        # DuckDB query to find duplicate groups (CLAUDE.md specified pattern)
        duplicate_sql = f"""
            SELECT COUNT(*) as duplicate_groups
            FROM (
                SELECT {columns_str}, COUNT(*) as group_count
                FROM {table_name}
                WHERE {where_conditions}
                GROUP BY {columns_str}
                HAVING COUNT(*) > 1
            )
        """
        
        logger.debug("key_validator.composite_key_sql", sql=duplicate_sql.strip())
        
        duplicate_groups = self.con.execute(duplicate_sql).fetchone()[0]
        
        # Get total row counts for complete validation result
        total_sql = f"""
            SELECT COUNT(*) as total_rows,
                   COUNT(DISTINCT {columns_str}) as unique_combinations
            FROM {table_name}
            WHERE {where_conditions}
        """
        
        total_result = self.con.execute(total_sql).fetchone()
        total_rows, unique_values = total_result
        
        duplicate_count = total_rows - unique_values
        is_valid = duplicate_groups == 0
        
        error_message = None
        if not is_valid:
            error_message = (f"{duplicate_groups} duplicate groups found in composite key "
                           f"[{', '.join(key_columns)}]. Key validation failed.")
        
        return KeyValidationResult(
            is_valid=is_valid,
            total_rows=total_rows,
            unique_values=unique_values,
            duplicate_count=duplicate_count,
            error_message=error_message
        )
    
    def get_duplicate_examples(self, table_name: str, key_columns: List[str],
                              dataset_config, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get examples of duplicate key values for user inspection.
        
        Args:
            table_name: Name of table to analyze
            key_columns: Key columns to check for duplicates
            dataset_config: Dataset configuration with column mappings
            limit: Maximum number of examples to return
            
        Returns:
            List of dictionaries with duplicate key examples
        """
        mapped_columns = self._apply_column_mappings(key_columns, dataset_config)
        columns_str = ", ".join(mapped_columns)
        
        # CLAUDE.md specified query pattern for finding duplicates
        sql = f"""
            SELECT {columns_str}, COUNT(*) as duplicate_count
            FROM {table_name}
            GROUP BY {columns_str}
            HAVING COUNT(*) > 1
            ORDER BY duplicate_count DESC
            LIMIT {limit}
        """
        
        logger.debug("key_validator.duplicate_examples_sql", sql=sql.strip())
        
        results = self.con.execute(sql).fetchall()
        
        examples = []
        for row in results:
            example = {}
            for i, col in enumerate(mapped_columns):
                example[col] = row[i]
            example['duplicate_count'] = row[-1]  # Last column is always count
            examples.append(example)
        
        return examples
    
    def _quote_identifier(self, identifier: str) -> str:
        """
        Quote column names that contain spaces or special characters for DuckDB.
        
        Args:
            identifier: Column name or identifier to quote
            
        Returns:
            Quoted identifier safe for SQL
        """
        # If identifier contains spaces or special characters, quote it
        if ' ' in identifier or '-' in identifier or '.' in identifier:
            return f'"{identifier}"'
        return identifier