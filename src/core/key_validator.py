"""
Key uniqueness validation using DuckDB.
Single responsibility: Validate key column uniqueness for comparison tables.
"""

from dataclasses import dataclass
from typing import List, Optional, Dict, Any
import duckdb

from ..utils.logger import get_logger
from ..utils.normalizers import normalize_column_name


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
    discovered_keys: List[str]  # NEW: The actual staged column names used for validation
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
        
        # Get staged column names for key validation
        staged_columns = self._get_staged_key_columns(table_name, key_columns, dataset_config)
        
        try:
            if len(staged_columns) == 1:
                # Single column validation
                result = self._validate_single_column(table_name, staged_columns[0])
            else:
                # Composite key validation  
                result = self._validate_composite_key(table_name, staged_columns)
                
            logger.info("key_validator.validate_complete",
                       table=table_name,
                       is_valid=result.is_valid,
                       duplicates=result.duplicate_count)
            
            return result
            
        except Exception as e:
            error_msg = (f"[KEY VALIDATION ERROR] Failed to validate keys in table '{table_name}': {e}. "
                        f"Suggestion: Verify table exists and columns {staged_columns} are valid.")
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
    
    def _get_staged_key_columns(self, table_name: str, key_columns: List[str], dataset_config) -> List[str]:
        """
        Get staged column names for key validation in both left and right tables.
        
        STAGED KEY CONSISTENCY PATTERN:
        - Left table (no column_map): Normalize key column names to match staged table
        - Right table (with column_map): Apply column mapping then normalize
        
        Args:
            key_columns: Original key column names
            dataset_config: Dataset config with potential column_map
            
        Returns:
            List of staged column names ready for SQL validation
        """
        if not dataset_config or not dataset_config.column_map:
            # Case 1: No column_map (Left Table)
            # Discover actual staged column names that match user-selected keys
            staged_columns = []
            for col in key_columns:
                discovered_col = self._discover_staged_column(table_name, col)
                staged_columns.append(discovered_col)
            
            logger.debug("key_validator.left_table_discovery",
                        original=key_columns,
                        discovered=staged_columns)
        else:
            # Case 2: With column_map (Right Table) 
            # NORMALIZED INVERSE MAPPING PATTERN: Create fully normalized map for inverse lookup
            staged_columns = []
            
            # Step 2a: Create fully normalized map where both keys and values are normalized
            normalized_map = {}
            for right_col, left_col in dataset_config.column_map.items():
                # Normalize both sides of the mapping
                norm_right = normalize_column_name(right_col)
                norm_left = normalize_column_name(left_col)
                normalized_map[norm_right] = norm_left
            
            logger.debug("key_validator.normalized_mapping_created",
                        original_map=dataset_config.column_map,
                        normalized_map=normalized_map)
            
            for col in key_columns:
                # Step 2b: Normalize the input key_column (user's choice) to left_norm
                left_norm = normalize_column_name(col)
                
                # Step 2c: Use left_norm to perform inverse lookup against normalized map
                right_norm = self._get_mapped_column_name_normalized(left_norm, normalized_map)
                staged_columns.append(right_norm)
                
            logger.debug("key_validator.right_table_mapping",
                        original=key_columns,
                        mapped_and_normalized=staged_columns)
        
        return staged_columns
    
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
    
    def _get_mapped_column_name_normalized(self, left_norm: str, normalized_map: Dict[str, str]) -> str:
        """
        Get mapped column name using normalized inverse lookup.
        
        NORMALIZED INVERSE MAPPING PATTERN:
        Find the right column that maps to the given normalized left column.
        
        Args:
            left_norm: Normalized left table column name
            normalized_map: Fully normalized mapping from right column -> left column
            
        Returns:
            Right table column name (normalized) that maps to left_norm
        """
        # Find the right column that maps to this normalized left column
        for right_norm, left_mapped in normalized_map.items():
            if left_mapped == left_norm:
                return right_norm
        
        # No mapping found, use the normalized left column name
        # This handles cases where no explicit mapping exists
        return left_norm
    
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
            discovered_keys=[column_name],  # Return the discovered staged column name
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
            discovered_keys=key_columns,  # Return the discovered staged column names
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
        staged_columns = self._get_staged_key_columns(table_name, key_columns, dataset_config)
        columns_str = ", ".join(staged_columns)
        
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
            for i, col in enumerate(staged_columns):
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
    
    def _discover_staged_column(self, table_name: str, key_column: str) -> str:
        """
        Discover the actual staged column name that matches the user-selected key.
        
        For LEFT tables, the user selects a key column name during interactive mode,
        but the staged table may have different column names due to normalization.
        This method queries the table schema to find the correct staged column.
        
        Args:
            table_name: Name of staged table to query
            key_column: User-selected key column name
            
        Returns:
            Actual staged column name that can be used in SQL queries
            
        Raises:
            KeyValidationError: If no suitable staged column is found
        """
        try:
            # Query actual staged table columns
            columns_result = self.con.execute(f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = '{table_name}'
                ORDER BY column_name
            """).fetchall()
            
            actual_columns = [col[0] for col in columns_result]
            
            logger.debug("key_validator.schema_discovery",
                        table=table_name,
                        user_key=key_column,
                        actual_columns=actual_columns)
            
            # First try: exact match with user-selected column
            if key_column in actual_columns:
                logger.debug("key_validator.exact_match_found",
                            user_key=key_column)
                return key_column
            
            # Second try: exact match with normalized user-selected column
            normalized_key = normalize_column_name(key_column)
            if normalized_key in actual_columns:
                logger.debug("key_validator.normalized_match_found",
                            user_key=key_column,
                            normalized=normalized_key)
                return normalized_key
            
            # Third try: find staged column that normalizes to the same value
            for staged_col in actual_columns:
                if normalize_column_name(staged_col) == normalized_key:
                    logger.debug("key_validator.staged_match_found",
                                user_key=key_column,
                                staged_column=staged_col)
                    return staged_col
            
            # No match found - fail with informative error
            available_cols = ", ".join(actual_columns)
            raise KeyValidationError(
                f"[KEY VALIDATION ERROR] Column '{key_column}' not found in staged table '{table_name}'. "
                f"Available columns: {available_cols}. "
                f"Suggestion: Verify the column exists or check column mapping configuration."
            )
            
        except Exception as e:
            if isinstance(e, KeyValidationError):
                raise
            
            error_msg = (f"[KEY VALIDATION ERROR] Failed to discover staged column for '{key_column}' "
                        f"in table '{table_name}': {e}. "
                        f"Suggestion: Verify table exists and contains expected columns.")
            logger.error("key_validator.discovery_failed",
                        table=table_name,
                        key_column=key_column,
                        error=str(e))
            raise KeyValidationError(error_msg)