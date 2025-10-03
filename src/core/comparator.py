"""
Core data comparison logic.
Single responsibility: compare two datasets and identify differences.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional, Tuple
import duckdb
import pandas as pd
from pathlib import Path
import os
import math
import csv

from ..utils.logger import get_logger
from ..config.manager import ComparisonConfig
from ..utils.normalizers import normalize_column_name
from .key_validator import KeyValidator, KeyValidationError


logger = get_logger()


def qident(name: str) -> str:
    """
    Quote SQL identifiers for safe usage in DuckDB queries.
    Handles reserved words, spaces, and special characters.
    
    Args:
        name: SQL identifier (table name, column name, etc.)
        
    Returns:
        Quoted identifier safe for SQL usage
    """
    if not name:
        return name
    
    # Always quote identifiers for consistency and safety
    # This handles reserved words, spaces, and special characters
    return f'"{name}"'


def qpath(path: str) -> str:
    """
    Quote and normalize file paths for DuckDB COPY operations.
    Handles Windows path escaping and UTF-8 encoding.
    
    Args:
        path: File path string
        
    Returns:
        Properly quoted and escaped path for DuckDB
    """
    if not path:
        return path
    
    # Convert Path objects to string and normalize separators
    path_str = str(path).replace('\\', '/')
    
    # Quote the path for SQL safety
    return f"'{path_str}'"


def _strip_trailing_semicolon(sql: str) -> str:
    """
    Strip trailing semicolon from SQL query to prepare for safe wrapping.
    
    Args:
        sql: SQL query that may contain trailing semicolon
        
    Returns:
        SQL query without trailing semicolon, preserving other whitespace
    """
    if not sql:
        return sql
        
    # Strip whitespace from end, then check for semicolon
    sql_stripped = sql.rstrip()
    if sql_stripped.endswith(';'):
        sql_stripped = sql_stripped[:-1].rstrip()
    
    return sql_stripped


@dataclass
class ComparisonResult:
    """Results from dataset comparison."""
    
    total_left: int = 0
    total_right: int = 0
    matched_rows: int = 0
    only_in_left: int = 0
    only_in_right: int = 0
    value_differences: int = 0
    columns_compared: List[str] = field(default_factory=list)
    key_columns: List[str] = field(default_factory=list)
    summary: Dict[str, Any] = field(default_factory=dict)
    

class DataComparator:
    """
    Compare two datasets and identify differences.
    """
    
    def __init__(self, con: duckdb.DuckDBPyConnection):
        """
        Initialize comparator.
        
        Args:
            con: DuckDB connection
        """
        self.con = con
    
    def _duckdb_supports_force_quote(self) -> bool:
        """
        Check if the connected DuckDB version supports FORCE_QUOTE *.
        Implements version safety by gracefully handling unsupported syntax.
        
        Returns:
            True if FORCE_QUOTE * is supported, False otherwise
        """
        try:
            # Test FORCE_QUOTE support with a minimal query to a temporary location
            import tempfile
            import os
            
            with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp_file:
                temp_path = tmp_file.name
            
            try:
                # Try to use FORCE_QUOTE * - this will fail on older DuckDB versions
                self.con.execute(f"""
                    COPY (SELECT 1 as test_col) TO '{temp_path}' (HEADER, DELIMITER ',', FORCE_QUOTE *)
                """)
                return True
            except Exception:
                # FORCE_QUOTE * not supported in this DuckDB version
                return False
            finally:
                # Clean up temp file
                try:
                    os.unlink(temp_path)
                except:
                    pass
                    
        except Exception:
            # Default to False if any error occurs during version detection
            return False
    
    def _csv_copy_options(self, include_header: bool = True) -> str:
        """
        Generates version-safe CSV write options string.
        
        Args:
            include_header: Whether to include HEADER option
            
        Returns:
            CSV options string for COPY command
        """
        options = "DELIMITER ','"
        if include_header:
            options = "HEADER, " + options
        
        # VERSION-SAFE: Prefer FORCE_QUOTE * when supported
        if self._duckdb_supports_force_quote():
            options += ", FORCE_QUOTE *"
        
        return options
    
    def compare(self, left_table: str, right_table: str,
               config: ComparisonConfig, left_dataset_config=None, right_dataset_config=None, 
               validated_keys: Optional[List[str]] = None) -> ComparisonResult:
        """
        Compare two datasets.
        
        Args:
            left_table: Name of left table in DuckDB
            right_table: Name of right table in DuckDB
            config: Comparison configuration
            left_dataset_config: Left dataset configuration (for column mappings)
            right_dataset_config: Right dataset configuration (for column mappings)
            validated_keys: Pre-validated key columns (skips auto-detection if provided)
            
        Returns:
            Comparison results
        """
        logger.info("comparator.starting",
                   left=left_table,
                   right=right_table)
        
        result = ComparisonResult()
        
        # Store dataset configs for column mapping (BUG 3 fix)
        self.left_dataset_config = left_dataset_config
        self.right_dataset_config = right_dataset_config
        
        # Debug logging to verify configs are being passed
        logger.debug("comparator.configs.received",
                    left_config_exists=left_dataset_config is not None,
                    right_config_exists=right_dataset_config is not None,
                    left_column_map=left_dataset_config.column_map if left_dataset_config else None,
                    right_column_map=right_dataset_config.column_map if right_dataset_config else None)
        
        # Get row counts
        result.total_left = self._get_row_count(left_table)
        result.total_right = self._get_row_count(right_table)
        
        # Determine key columns - use validated keys if provided, otherwise auto-detect
        if validated_keys:
            # Normalize validated keys to match staged table columns
            key_columns = [normalize_column_name(key) for key in validated_keys]
            logger.info("comparator.using_validated_keys", 
                       original_keys=validated_keys,
                       normalized_keys=key_columns)
        else:
            key_columns = self._determine_keys(left_table, right_table, config)
        result.key_columns = key_columns
        
        # MANDATORY PRE-COMPARISON KEY UNIQUENESS VALIDATION
        # Enforce data integrity - fail fast if key columns contain duplicates
        validator = KeyValidator(self.con)
        
        try:
            # Validate left table key uniqueness
            left_validation = validator.validate_key(left_table, key_columns, left_dataset_config)
            if not left_validation.is_valid:
                error_msg = f"[KEY VALIDATION ERROR] Duplicates found in key column(s) {key_columns} in left dataset '{left_table}'. Found {left_validation.duplicate_count} duplicates. Suggestion: Use a composite key or clean source data."
                logger.error("comparator.key_validation_failed", 
                           table=left_table, 
                           key_columns=key_columns,
                           duplicates=left_validation.duplicate_count)
                raise KeyValidationError(error_msg)
            
            # Validate right table key uniqueness  
            right_validation = validator.validate_key(right_table, key_columns, right_dataset_config)
            if not right_validation.is_valid:
                error_msg = f"[KEY VALIDATION ERROR] Duplicates found in key column(s) {key_columns} in right dataset '{right_table}'. Found {right_validation.duplicate_count} duplicates. Suggestion: Use a composite key or clean source data."
                logger.error("comparator.key_validation_failed",
                           table=right_table,
                           key_columns=key_columns, 
                           duplicates=right_validation.duplicate_count)
                raise KeyValidationError(error_msg)
                
            # STAGED KEY PROPAGATION: Use discovered staged column names for all subsequent operations
            # This prevents "Binder Error: Referenced column not found" in SQL generation
            original_key_columns = key_columns  # Store original before update
            discovered_left_keys = left_validation.discovered_keys
            discovered_right_keys = right_validation.discovered_keys
            
            # Update key_columns to use the discovered left table keys as canonical names
            # This ensures consistent SQL generation throughout the comparison pipeline
            key_columns = discovered_left_keys
            result.key_columns = discovered_left_keys  # Update result to reflect discovered keys
            
            logger.info("comparator.key_validation_passed",
                       left_table=left_table,
                       right_table=right_table,
                       original_key_columns=original_key_columns,
                       discovered_left_keys=discovered_left_keys,
                       discovered_right_keys=discovered_right_keys)
                       
            # Store discovered keys for SQL generation methods that need table-specific column names
            self.discovered_left_keys = discovered_left_keys
            self.discovered_right_keys = discovered_right_keys
                       
        except KeyValidationError:
            # Re-raise KeyValidationError to halt comparison
            raise
        except Exception as e:
            # Handle unexpected validation errors
            error_msg = f"[KEY VALIDATION ERROR] Failed to validate key uniqueness: {str(e)}. Suggestion: Check key column names and table structure."
            logger.error("comparator.key_validation_error", error=str(e))
            raise KeyValidationError(error_msg)
        
        if not key_columns:
            logger.error("comparator.no_keys")
            raise ValueError("No key columns found for comparison")
        
        # Get columns to compare
        value_columns = self._determine_value_columns(
            left_table, right_table, config, key_columns
        )
        result.columns_compared = value_columns
        
        # Find matched rows
        matched_count = self._find_matches(
            left_table, right_table, key_columns
        )
        result.matched_rows = matched_count
        
        # Find only in left
        only_left_count = self._find_only_in_left(
            left_table, right_table, key_columns
        )
        result.only_in_left = only_left_count
        
        # Find only in right
        only_right_count = self._find_only_in_right(
            left_table, right_table, key_columns
        )
        result.only_in_right = only_right_count
        
        # Find value differences
        if value_columns and matched_count > 0:
            diff_count = self._find_value_differences(
                left_table, right_table, key_columns,
                value_columns, config
            )
            result.value_differences = diff_count
        
        # Calculate summary stats
        result.summary = self._calculate_summary(result)
        
        logger.info("comparator.complete",
                   matched=result.matched_rows,
                   only_left=result.only_in_left,
                   only_right=result.only_in_right,
                   differences=result.value_differences)
        
        return result
    
    def _get_row_count(self, table: str) -> int:
        """Get row count for table."""
        count = self.con.execute(
            f"SELECT COUNT(*) FROM {table}"
        ).fetchone()[0]
        return int(count)
    
    def _get_columns(self, table: str) -> List[str]:
        """Get column list for table."""
        result = self.con.execute(f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = '{table}'
            ORDER BY ordinal_position
        """).fetchall()
        
        return [row[0] for row in result]
    
    def _get_mapped_column(self, column: str, dataset_config) -> str:
        """
        Get mapped column name if mapping exists.
        
        Args:
            column: Original column name  
            dataset_config: Dataset configuration with potential column mapping
            
        Returns:
            Mapped column name or original if no mapping
        """
        if not dataset_config or not dataset_config.column_map:
            return column
        
        return dataset_config.column_map.get(column, column)
    
    def _get_right_column(self, left_column: str) -> str:
        """
        Get the corresponding right table column for a left table column.
        
        Args:
            left_column: Column name in left table (may be original or normalized)
            
        Returns:
            Corresponding column name in right table
        """
        logger.debug("comparator._get_right_column.lookup", 
                    left_column=left_column,
                    has_mapping=bool(self.right_dataset_config and self.right_dataset_config.column_map))
        
        # Check if there's a column mapping for this left column
        if self.right_dataset_config and self.right_dataset_config.column_map:
            column_map = self.right_dataset_config.column_map
            logger.debug("comparator._get_right_column.mapping_available", 
                        column_map_size=len(column_map),
                        sample_mappings=dict(list(column_map.items())[:3]))
            
            # First try exact match
            for right_col, left_col in column_map.items():
                if left_col == left_column:
                    logger.debug("comparator._get_right_column.exact_match", 
                               left_column=left_column, right_column=right_col)
                    return right_col
            
            # If no exact match, try normalized lookup
            # This handles the case where key_columns contains original names from comparison config
            normalized_left = normalize_column_name(left_column)
            logger.debug("comparator._get_right_column.trying_normalized", 
                       original_left=left_column,
                       normalized_left=normalized_left)
            
            for right_col, left_col in column_map.items():
                if left_col == normalized_left:
                    logger.debug("comparator._get_right_column.normalized_match", 
                               original_left=left_column, 
                               normalized_left=normalized_left,
                               right_column=right_col)
                    return right_col
        
        logger.debug("comparator._get_right_column.no_mapping", left_column=left_column)
        # No mapping, use same name
        return left_column
    
    def _determine_keys(self, left_table: str, right_table: str,
                       config: ComparisonConfig) -> List[str]:
        """
        Determine key columns for comparison.
        This considers column mappings when finding matching columns.
        """
        if config.comparison_keys:
            # Use configured keys
            return config.comparison_keys
        
        # Find columns that match (either directly or through mapping)
        left_cols = set(self._get_columns(left_table))
        right_cols = set(self._get_columns(right_table))
        
        # Find columns that can be matched
        matchable_cols = []
        for left_col in left_cols:
            # Check if this left column has a corresponding right column
            right_col = self._get_right_column(left_col)
            if right_col in right_cols:
                matchable_cols.append(left_col)
        
        if not matchable_cols:
            logger.error("comparator.no_matchable_columns",
                        left_count=len(left_cols),
                        right_count=len(right_cols))
            return []
        
        # Look for likely key columns among matchable columns
        key_indicators = ['id', 'key', 'code', 'number']
        potential_keys = []
        
        for col in matchable_cols:
            col_lower = col.lower()
            for indicator in key_indicators:
                if indicator in col_lower:
                    potential_keys.append(col)
                    break
        
        if potential_keys:
            logger.info("comparator.auto_detected_keys",
                       keys=potential_keys)
            return potential_keys[:3]  # Limit to 3 keys
        
        # Fall back to first matchable column
        logger.warning("comparator.using_first_column_as_key",
                      column=matchable_cols[0])
        return [matchable_cols[0]]
    
    def _determine_value_columns(self, left_table: str,
                                right_table: str,
                                config: ComparisonConfig,
                                key_columns: List[str]) -> List[str]:
        """
        Determine value columns to compare.
        This includes both exact matches AND mapped columns.
        """
        if config.value_columns:
            return config.value_columns
        
        # Get all columns from both tables
        left_cols = set(self._get_columns(left_table))
        right_cols = set(self._get_columns(right_table))
        
        # Find columns to compare: 
        # Include left columns that either match directly OR have a mapping
        value_cols = []
        
        # Debug: show what we're working with
        print(f"  DEBUG _determine_value_columns:")
        print(f"    Left columns: {len(left_cols)}")
        print(f"    Right columns: {len(right_cols)}")
        if self.right_dataset_config and self.right_dataset_config.column_map:
            print(f"    Column mappings available: {len(self.right_dataset_config.column_map)}")
        else:
            print(f"    No column mappings available")
        
        for left_col in left_cols:
            # Skip key columns
            if left_col in key_columns:
                continue
            
            # Get the mapped column name for the right table
            right_col = self._get_right_column(left_col)
            
            # Include this column if the mapped name exists in the right table
            if right_col in right_cols:
                value_cols.append(left_col)
                logger.debug("comparator.value_column_included",
                           left_col=left_col,
                           right_col=right_col,
                           is_mapped=(left_col != right_col))
        
        logger.info("comparator.value_columns_determined",
                   total_left_cols=len(left_cols),
                   total_right_cols=len(right_cols),
                   value_cols_count=len(value_cols),
                   value_cols=value_cols[:5])  # Log first 5 for debugging
        
        return value_cols
    
    def _find_matches(self, left_table: str, right_table: str,
                     key_columns: List[str]) -> int:
        """
        Find rows that match on key columns.
        """
        # BUG 3 fix: Use column mapping for join conditions with normalized columns
        # Normalize key columns to match staged table format
        key_conditions = []
        for col in key_columns:
            left_norm = normalize_column_name(col)
            right_col = self._get_right_column(col)
            right_norm = normalize_column_name(right_col)
            
            logger.debug("comparator._find_matches.key_join_generation",
                        original_key=col,
                        left_normalized=left_norm,
                        right_mapped=right_col,
                        right_normalized=right_norm)
            
            # Robust type coercion with mandatory trim: Cast to VARCHAR and remove whitespace
            key_conditions.append(f"TRIM(TRY_CAST(l.{left_norm} AS VARCHAR)) = TRIM(TRY_CAST(r.{right_norm} AS VARCHAR))")
        
        key_join = " AND ".join(key_conditions)
        
        sql = f"""
            SELECT COUNT(*)
            FROM {left_table} l
            INNER JOIN {right_table} r ON {key_join}
        """
        
        logger.debug("comparator.sql.find_matches", sql=sql.strip())
        
        # Use chunked processing for large datasets to avoid hanging
        if self._should_use_chunked_processing(left_table, right_table):
            print("🔄 Large dataset detected - using chunked processing for matches...")
            return self._find_matches_chunked(left_table, right_table, key_columns)
        
        count = self.con.execute(sql).fetchone()[0]
        return int(count)
    
    def _find_only_in_left(self, left_table: str, right_table: str,
                          key_columns: List[str]) -> int:
        """
        Find rows only in left dataset.
        """
        # BUG 3 fix: Use column mapping for join conditions with normalized columns
        # Robust type coercion with mandatory trim: Cast to VARCHAR and remove whitespace
        key_join = " AND ".join([
            f"TRIM(TRY_CAST(l.{normalize_column_name(col)} AS VARCHAR)) = TRIM(TRY_CAST(r.{normalize_column_name(self._get_right_column(col))} AS VARCHAR))" 
            for col in key_columns
        ])
        
        # Use mapped and normalized column name for NULL check
        right_key_col = normalize_column_name(self._get_right_column(key_columns[0]))
        
        sql = f"""
            SELECT COUNT(*)
            FROM {left_table} l
            LEFT JOIN {right_table} r ON {key_join}
            WHERE r.{right_key_col} IS NULL
        """
        
        logger.debug("comparator.sql.find_only_in_left", sql=sql.strip())
        
        # Use chunked processing for large datasets to avoid hanging
        if self._should_use_chunked_processing(left_table, right_table):
            print("🔄 Large dataset detected - using chunked processing for left-only records...")
            return self._find_only_in_left_chunked(left_table, right_table, key_columns)
        
        count = self.con.execute(sql).fetchone()[0]
        return int(count)
    
    def _find_only_in_right(self, left_table: str, right_table: str,
                           key_columns: List[str]) -> int:
        """
        Find rows only in right dataset.
        """
        # BUG 3 fix: Use column mapping for join conditions with normalized columns
        # Robust type coercion with mandatory trim: Cast to VARCHAR and remove whitespace
        key_join = " AND ".join([
            f"TRIM(TRY_CAST(l.{normalize_column_name(col)} AS VARCHAR)) = TRIM(TRY_CAST(r.{normalize_column_name(self._get_right_column(col))} AS VARCHAR))" 
            for col in key_columns
        ])
        
        sql = f"""
            SELECT COUNT(*)
            FROM {right_table} r
            LEFT JOIN {left_table} l ON {key_join}
            WHERE l.{normalize_column_name(key_columns[0])} IS NULL
        """
        
        logger.debug("comparator.sql.find_only_in_right", sql=sql.strip())
        
        # Use chunked processing for large datasets to avoid hanging
        if self._should_use_chunked_processing(left_table, right_table):
            print("🔄 Large dataset detected - using chunked processing for right-only records...")
            return self._find_only_in_right_chunked(left_table, right_table, key_columns)
        
        count = self.con.execute(sql).fetchone()[0]
        return int(count)
    
    def _build_robust_comparison_condition(self, norm_col: str, norm_right_col: str, 
                                         config: ComparisonConfig) -> str:
        """
        Build a robust comparison condition with proper value coercion.
        
        Handles multiple data type formats with priority-based comparison:
        1. Numeric values (with currency/percentage stripping)
        2. Date/timestamp values (multiple formats)
        3. Boolean values (only actual boolean strings, not numbers)
        4. String values (normalized with lowercase/trim)
        
        Args:
            norm_col: Normalized left column name
            norm_right_col: Normalized right column name
            config: Comparison configuration
            
        Returns:
            SQL condition string for robust comparison
        """
        # Build expressions for numeric coercion with currency stripping
        # This handles: $1,234.56, (123.45) for negative, currency symbols, commas
        left_numeric_expr = f"""
            TRY_CAST(
                TRIM(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(
                                REGEXP_REPLACE(
                                    TRY_CAST(l.{norm_col} AS VARCHAR),
                                    '\\s*[$£€¥₪₹¢]\\s*', '', 'g'
                                ),
                                ',', '', 'g'
                            ),
                            '^\\(', '-', 'g'
                        ),
                        '\\)$', '', 'g'
                    )
                ) AS DOUBLE
            )
        """
        
        right_numeric_expr = f"""
            TRY_CAST(
                TRIM(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(
                                REGEXP_REPLACE(
                                    TRY_CAST(r.{norm_right_col} AS VARCHAR),
                                    '\\s*[$£€¥₪₹¢]\\s*', '', 'g'
                                ),
                                ',', '', 'g'
                            ),
                            '^\\(', '-', 'g'
                        ),
                        '\\)$', '', 'g'
                    )
                ) AS DOUBLE
            )
        """
        
        # Build expressions for date coercion (multiple formats)
        # Must cast to VARCHAR first since TRY_STRPTIME requires VARCHAR input
        left_date_expr = f"""
            COALESCE(
                TRY_CAST(l.{norm_col} AS TIMESTAMP),
                TRY_STRPTIME(TRY_CAST(l.{norm_col} AS VARCHAR), '%m/%d/%Y'),
                TRY_STRPTIME(TRY_CAST(l.{norm_col} AS VARCHAR), '%m/%d/%Y %H:%M'),
                TRY_STRPTIME(TRY_CAST(l.{norm_col} AS VARCHAR), '%d/%m/%Y'),
                TRY_STRPTIME(TRY_CAST(l.{norm_col} AS VARCHAR), '%Y-%m-%d'),
                TRY_STRPTIME(TRY_CAST(l.{norm_col} AS VARCHAR), '%m-%d-%Y'),
                TRY_STRPTIME(TRY_CAST(l.{norm_col} AS VARCHAR), '%Y/%m/%d'),
                TRY_STRPTIME(TRY_CAST(l.{norm_col} AS VARCHAR), '%d-%m-%Y')
            )
        """
        
        right_date_expr = f"""
            COALESCE(
                TRY_CAST(r.{norm_right_col} AS TIMESTAMP),
                TRY_STRPTIME(TRY_CAST(r.{norm_right_col} AS VARCHAR), '%m/%d/%Y'),
                TRY_STRPTIME(TRY_CAST(r.{norm_right_col} AS VARCHAR), '%m/%d/%Y %H:%M'),
                TRY_STRPTIME(TRY_CAST(r.{norm_right_col} AS VARCHAR), '%d/%m/%Y'),
                TRY_STRPTIME(TRY_CAST(r.{norm_right_col} AS VARCHAR), '%Y-%m-%d'),
                TRY_STRPTIME(TRY_CAST(r.{norm_right_col} AS VARCHAR), '%m-%d-%Y'),
                TRY_STRPTIME(TRY_CAST(r.{norm_right_col} AS VARCHAR), '%Y/%m/%d'),
                TRY_STRPTIME(TRY_CAST(r.{norm_right_col} AS VARCHAR), '%d-%m-%Y')
            )
        """
        
        # Build the numeric comparison based on tolerance setting
        if config.tolerance > 0:
            numeric_comparison = f"""
                ABS({left_numeric_expr} - {right_numeric_expr}) > {config.tolerance}
            """
        else:
            numeric_comparison = f"""
                {left_numeric_expr} != {right_numeric_expr}
            """
        
        # Build the complete comparison condition with priority-based logic
        return f"""
            (
                -- NULL handling first
                (l.{norm_col} IS NULL AND r.{norm_right_col} IS NOT NULL) OR
                (l.{norm_col} IS NOT NULL AND r.{norm_right_col} IS NULL) OR
                (
                    l.{norm_col} IS NOT NULL AND r.{norm_right_col} IS NOT NULL AND
                    CASE
                        -- Priority 1: Try numeric comparison (with currency stripping)
                        WHEN {left_numeric_expr} IS NOT NULL AND {right_numeric_expr} IS NOT NULL THEN
                            {numeric_comparison}
                        
                        -- Priority 2: Try date comparison (multiple formats)
                        WHEN {left_date_expr} IS NOT NULL AND {right_date_expr} IS NOT NULL THEN
                            {left_date_expr} != {right_date_expr}
                        
                        -- Priority 3: Boolean comparison (only for actual boolean strings, not numbers)
                        -- Check that values are NOT purely numeric before treating as boolean
                        WHEN NOT (TRY_CAST(l.{norm_col} AS DOUBLE) IS NOT NULL) 
                             AND NOT (TRY_CAST(r.{norm_right_col} AS DOUBLE) IS NOT NULL)
                             AND LOWER(TRY_CAST(l.{norm_col} AS VARCHAR)) IN ('true', 'false', 't', 'f', 'yes', 'no')
                             AND LOWER(TRY_CAST(r.{norm_right_col} AS VARCHAR)) IN ('true', 'false', 't', 'f', 'yes', 'no') THEN
                            -- Compare as booleans
                            (LOWER(TRY_CAST(l.{norm_col} AS VARCHAR)) IN ('true', 't', 'yes')) != 
                            (LOWER(TRY_CAST(r.{norm_right_col} AS VARCHAR)) IN ('true', 't', 'yes'))
                        
                        -- Priority 4: String comparison (normalized with lowercase, trim, and quote removal)
                        ELSE
                            TRIM(LOWER(TRIM(TRY_CAST(l.{norm_col} AS VARCHAR))), '''\"') != 
                            TRIM(LOWER(TRIM(TRY_CAST(r.{norm_right_col} AS VARCHAR))), '''\"')
                    END
                )
            )
        """
    
    def _find_value_differences(self, left_table: str, right_table: str,
                               key_columns: List[str],
                               value_columns: List[str],
                               config: ComparisonConfig) -> int:
        """
        Find rows with value differences using robust comparison logic.
        
        This method now uses centralized robust comparison that eliminates false positives
        caused by date/time format differences and string formatting variations.
        """
        # BUG 3 fix: Use column mapping for join conditions with normalized columns
        # Normalize key columns to match staged table format
        key_join = " AND ".join([
            f"l.{normalize_column_name(col)} = r.{normalize_column_name(self._get_right_column(col))}" 
            for col in key_columns
        ])
        
        # Build robust comparison conditions using centralized logic
        comparisons = []
        for col in value_columns:
            # Normalize column names to match staged tables
            norm_col = normalize_column_name(col)
            right_col = self._get_right_column(col)  # Get mapped right column
            norm_right_col = normalize_column_name(right_col)
            
            # Use centralized robust comparison logic
            comparisons.append(self._build_robust_comparison_condition(
                norm_col, norm_right_col, config
            ))
        
        where_clause = " OR ".join(comparisons)
        
        sql = f"""
            SELECT COUNT(*)
            FROM {left_table} l
            INNER JOIN {right_table} r ON {key_join}
            WHERE {where_clause}
        """
        
        logger.debug("comparator.sql.find_value_differences_robust", sql=sql.strip())
        
        # Use chunked processing for large datasets to avoid hanging
        if self._should_use_chunked_processing(left_table, right_table):
            print("🔄 Large dataset detected - using chunked processing for better performance...")
            return self._find_value_differences_chunked(
                left_table, right_table, key_columns, value_columns, config
            )
        
        count = self.con.execute(sql).fetchone()[0]
        return int(count)
    
    def _should_use_chunked_processing(self, left_table: str, right_table: str) -> bool:
        """
        Determine if chunked processing should be used for large datasets.
        
        Args:
            left_table: Left table name
            right_table: Right table name
            
        Returns:
            True if chunked processing should be used
        """
        try:
            left_count = self._get_row_count(left_table)
            right_count = self._get_row_count(right_table)
            
            # Lower threshold: Use chunked processing for datasets > 25K rows
            row_threshold = 25_000
            should_chunk = left_count > row_threshold or right_count > row_threshold
            
            if should_chunk:
                logger.info("comparator.chunked_processing.enabled",
                           left_rows=left_count, 
                           right_rows=right_count,
                           row_threshold=row_threshold)
            
            return should_chunk
        except Exception as e:
            logger.warning("comparator.chunked_processing.check_failed", error=str(e))
            return False
    
    def _find_value_differences_chunked(self, left_table: str, right_table: str,
                                       key_columns: List[str], value_columns: List[str],
                                       config) -> int:
        """
        Find value differences using chunked processing for large datasets.
        
        Args:
            left_table: Left table name
            right_table: Right table name  
            key_columns: Key columns for joining
            value_columns: Value columns to compare
            config: Comparison configuration
            
        Returns:
            Total count of rows with value differences
        """
        logger.info("comparator.chunked_processing.start",
                   left_table=left_table,
                   right_table=right_table,
                   key_columns=key_columns,
                   value_columns=value_columns)
        
        chunk_size = 25_000  # Even smaller chunks for better responsiveness
        total_differences = 0
        
        # Get total rows in left table for progress tracking
        total_left_rows = self._get_row_count(left_table)
        processed_rows = 0
        
        # Process left table in chunks
        offset = 0
        chunk_num = 0
        
        while offset < total_left_rows:
            chunk_num += 1
            
            # BUG 3 fix: Use column mapping for join conditions with normalized columns
            key_join = " AND ".join([
                f"l.{normalize_column_name(col)} = r.{normalize_column_name(self._get_right_column(col))}" 
                for col in key_columns
            ])
            
            # Build robust comparison conditions using centralized logic
            comparisons = []
            for col in value_columns:
                # Normalize column names to match staged tables
                norm_col = normalize_column_name(col)
                right_col = self._get_right_column(col)  # Get mapped right column
                norm_right_col = normalize_column_name(right_col)
                
                # Use centralized robust comparison logic
                comparisons.append(self._build_robust_comparison_condition(
                    norm_col, norm_right_col, config
                ))
            
            where_clause = " OR ".join(comparisons)
            
            # Query for this chunk
            chunk_sql = f"""
                SELECT COUNT(*)
                FROM (
                    SELECT *
                    FROM {left_table}
                    LIMIT {chunk_size} OFFSET {offset}
                ) l
                INNER JOIN {right_table} r ON {key_join}
                WHERE {where_clause}
            """
            
            logger.debug("comparator.chunked_processing.chunk_sql", 
                        chunk=chunk_num, 
                        offset=offset,
                        sql=chunk_sql.strip())
            
            try:
                chunk_differences = self.con.execute(chunk_sql).fetchone()[0]
                total_differences += chunk_differences
                processed_rows += min(chunk_size, total_left_rows - offset)
                
                if chunk_num % 5 == 0:  # More frequent progress updates
                    progress_pct = round(100 * processed_rows / total_left_rows, 1)
                    print(f"  📊 Progress: {processed_rows:,}/{total_left_rows:,} rows ({progress_pct}%) - {total_differences:,} differences found")
                    logger.info("comparator.chunked_processing.progress",
                               chunk=chunk_num,
                               processed_rows=processed_rows,
                               total_rows=total_left_rows,
                               differences_found=total_differences)
                
            except Exception as e:
                logger.error("comparator.chunked_processing.chunk_failed",
                           chunk=chunk_num,
                           offset=offset, 
                           error=str(e))
                # Continue with next chunk
            
            offset += chunk_size
        
        logger.info("comparator.chunked_processing.complete",
                   total_chunks=chunk_num,
                   total_differences=total_differences)
        
        return total_differences
    
    def _find_matches_chunked(self, left_table: str, right_table: str,
                             key_columns: List[str]) -> int:
        """Find matches using chunked processing for large datasets."""
        logger.info("comparator.chunked_processing.matches.start",
                   left_table=left_table, right_table=right_table)
        
        chunk_size = 25_000
        total_matches = 0
        total_left_rows = self._get_row_count(left_table)
        processed_rows = 0
        offset = 0
        chunk_num = 0
        
        while offset < total_left_rows:
            chunk_num += 1
            
            # BUG 3 fix: Use column mapping for join conditions with normalized columns
            # Robust type coercion with mandatory trim: Cast to VARCHAR and remove whitespace
            key_join = " AND ".join([
                f"TRIM(TRY_CAST(l.{normalize_column_name(col)} AS VARCHAR)) = TRIM(TRY_CAST(r.{normalize_column_name(self._get_right_column(col))} AS VARCHAR))" 
                for col in key_columns
            ])
            
            chunk_sql = f"""
                SELECT COUNT(*)
                FROM (
                    SELECT *
                    FROM {left_table}
                    LIMIT {chunk_size} OFFSET {offset}
                ) l
                INNER JOIN {right_table} r ON {key_join}
            """
            
            logger.debug("comparator.chunked.matches.sql", 
                        chunk=chunk_num, 
                        key_join=key_join,
                        sql=chunk_sql.strip())
            
            try:
                chunk_matches = self.con.execute(chunk_sql).fetchone()[0]
                total_matches += chunk_matches
                processed_rows += min(chunk_size, total_left_rows - offset)
                
                if chunk_num % 5 == 0:
                    progress_pct = round(100 * processed_rows / total_left_rows, 1)
                    print(f"  📊 Matches Progress: {processed_rows:,}/{total_left_rows:,} rows ({progress_pct}%) - {total_matches:,} matches found")
                
            except Exception as e:
                logger.error("comparator.chunked_processing.matches.chunk_failed",
                           chunk=chunk_num, error=str(e))
            
            offset += chunk_size
        
        logger.info("comparator.chunked_processing.matches.complete",
                   total_matches=total_matches)
        return total_matches
    
    def _find_only_in_left_chunked(self, left_table: str, right_table: str,
                                  key_columns: List[str]) -> int:
        """Find left-only records using chunked processing for large datasets."""
        logger.info("comparator.chunked_processing.left_only.start",
                   left_table=left_table, right_table=right_table)
        
        chunk_size = 25_000
        total_left_only = 0
        total_left_rows = self._get_row_count(left_table)
        processed_rows = 0
        offset = 0
        chunk_num = 0
        
        while offset < total_left_rows:
            chunk_num += 1
            
            # BUG 3 fix: Use column mapping for join conditions with proper quoting
            # Robust type coercion with mandatory trim: Cast to VARCHAR and remove whitespace
            key_join = " AND ".join([
                f"TRIM(TRY_CAST(l.{normalize_column_name(col)} AS VARCHAR)) = TRIM(TRY_CAST(r.{normalize_column_name(self._get_right_column(col))} AS VARCHAR))" 
                for col in key_columns
            ])
            right_key_col = normalize_column_name(self._get_right_column(key_columns[0]))
            
            chunk_sql = f"""
                SELECT COUNT(*)
                FROM (
                    SELECT *
                    FROM {left_table}
                    LIMIT {chunk_size} OFFSET {offset}
                ) l
                LEFT JOIN {right_table} r ON {key_join}
                WHERE r.{right_key_col} IS NULL
            """
            
            logger.debug("comparator.chunked.left_only.sql", 
                        chunk=chunk_num, 
                        key_join=key_join,
                        right_key_col=right_key_col,
                        sql=chunk_sql.strip())
            
            try:
                chunk_left_only = self.con.execute(chunk_sql).fetchone()[0]
                total_left_only += chunk_left_only
                processed_rows += min(chunk_size, total_left_rows - offset)
                
                if chunk_num % 5 == 0:
                    progress_pct = round(100 * processed_rows / total_left_rows, 1)
                    print(f"  📊 Left-only Progress: {processed_rows:,}/{total_left_rows:,} rows ({progress_pct}%) - {total_left_only:,} left-only found")
                
            except Exception as e:
                logger.error("comparator.chunked_processing.left_only.chunk_failed",
                           chunk=chunk_num, error=str(e))
            
            offset += chunk_size
        
        logger.info("comparator.chunked_processing.left_only.complete",
                   total_left_only=total_left_only)
        return total_left_only
    
    def _find_only_in_right_chunked(self, left_table: str, right_table: str,
                                   key_columns: List[str]) -> int:
        """Find right-only records using chunked processing for large datasets."""
        logger.info("comparator.chunked_processing.right_only.start",
                   left_table=left_table, right_table=right_table)
        
        chunk_size = 25_000
        total_right_only = 0
        total_right_rows = self._get_row_count(right_table)
        processed_rows = 0
        offset = 0
        chunk_num = 0
        
        while offset < total_right_rows:
            chunk_num += 1
            
            # BUG 3 fix: Use column mapping for join conditions with normalized columns
            # Robust type coercion with mandatory trim: Cast to VARCHAR and remove whitespace
            key_join = " AND ".join([
                f"TRIM(TRY_CAST(l.{normalize_column_name(col)} AS VARCHAR)) = TRIM(TRY_CAST(r.{normalize_column_name(self._get_right_column(col))} AS VARCHAR))" 
                for col in key_columns
            ])
            
            chunk_sql = f"""
                SELECT COUNT(*)
                FROM (
                    SELECT *
                    FROM {right_table}
                    LIMIT {chunk_size} OFFSET {offset}
                ) r
                LEFT JOIN {left_table} l ON {key_join}
                WHERE l.{normalize_column_name(key_columns[0])} IS NULL
            """
            
            try:
                chunk_right_only = self.con.execute(chunk_sql).fetchone()[0]
                total_right_only += chunk_right_only
                processed_rows += min(chunk_size, total_right_rows - offset)
                
                if chunk_num % 5 == 0:
                    progress_pct = round(100 * processed_rows / total_right_rows, 1)
                    print(f"  📊 Right-only Progress: {processed_rows:,}/{total_right_rows:,} rows ({progress_pct}%) - {total_right_only:,} right-only found")
                
            except Exception as e:
                logger.error("comparator.chunked_processing.right_only.chunk_failed",
                           chunk=chunk_num, error=str(e))
            
            offset += chunk_size
        
        logger.info("comparator.chunked_processing.right_only.complete",
                   total_right_only=total_right_only)
        return total_right_only
    
    def _calculate_summary(self, result: ComparisonResult) -> Dict[str, Any]:
        """
        Calculate summary statistics.
        """
        total_unique = (result.total_left + result.total_right - 
                       result.matched_rows)
        
        match_rate = 0
        if total_unique > 0:
            match_rate = round(100 * result.matched_rows / total_unique, 2)
        
        return {
            "match_rate": match_rate,
            "total_unique_records": total_unique,
            "left_coverage": round(
                100 * (result.matched_rows / result.total_left)
                if result.total_left > 0 else 0, 2
            ),
            "right_coverage": round(
                100 * (result.matched_rows / result.total_right)
                if result.total_right > 0 else 0, 2
            ),
            "difference_rate": round(
                100 * (result.value_differences / result.matched_rows)
                if result.matched_rows > 0 else 0, 2
            )
        }
    
    def _export_full_csv(self, query: str, output_path: Path, chunk_size: int = 50000, order_cols: List[str] = None) -> None:
        """
        Export full results using chunked processing for large datasets.
        Implements SQL QUERY SANITIZATION PATTERN and UTF-8 encoding to prevent parser errors.
        
        Args:
            query: SQL query to execute (may contain trailing semicolon)
            output_path: Output file path
            chunk_size: Size of each chunk for processing
            order_cols: Columns for deterministic ordering (defaults to column 1)
        """
        logger.info("comparator.export_full_csv.start", 
                   output_path=str(output_path),
                   chunk_size=chunk_size)
        
        # CRITICAL: Sanitize the base query to prevent parser errors
        clean_query = _strip_trailing_semicolon(query)
        
        # Wrap sanitized query in subselect for safe chunking
        wrapped_query = f"SELECT * FROM ({clean_query}) q"
        
        # Get total count using properly wrapped query
        count_query = f"SELECT COUNT(*) FROM ({clean_query}) AS count_subquery"
        total_rows = self.con.execute(count_query).fetchone()[0]
        
        # Use qpath() for Windows path handling
        quoted_path = qpath(str(output_path))
        
        if total_rows == 0:
            # Create empty file with headers using wrapped query
            self.con.execute(f"""
                COPY (
                    {wrapped_query} LIMIT 0
                ) TO {quoted_path} ({self._csv_copy_options()})
            """)
            logger.warning("comparator.export_full_csv.empty", 
                          output_path=str(output_path),
                          note="No data to export - created empty file with headers")
            return
            
        if total_rows <= chunk_size:
            # Small dataset - export normally using wrapped query
            self.con.execute(f"""
                COPY (
                    {wrapped_query}
                ) TO {quoted_path} ({self._csv_copy_options()})
            """)
            logger.info("comparator.export_full_csv.small_dataset",
                       output_path=str(output_path),
                       total_rows=total_rows)
            return
        
        # Large dataset - use chunked export with properly wrapped query
        offset = 0
        chunk_num = 0
        
        # Build deterministic ORDER BY clause
        if order_cols:
            quoted_order_cols = [qident(col) for col in order_cols]
            order_by_clause = f"ORDER BY {', '.join(quoted_order_cols)}"
        else:
            order_by_clause = "ORDER BY 1"
        
        while offset < total_rows:
            chunk_num += 1
            is_first_chunk = offset == 0
            
            # Chunked query with proper ORDER BY, LIMIT, and OFFSET
            # This is now safe because wrapped_query has no trailing semicolon
            chunked_query = f"""
                {wrapped_query}
                {order_by_clause}
                LIMIT {chunk_size} OFFSET {offset}
            """
            
            if is_first_chunk:
                # First chunk includes headers
                self.con.execute(f"""
                    COPY (
                        {chunked_query}
                    ) TO {quoted_path} ({self._csv_copy_options()})
                """)
            else:
                # Subsequent chunks append without headers
                temp_chunk_path = output_path.parent / f"{output_path.stem}_chunk_{chunk_num}{output_path.suffix}"
                quoted_temp_path = qpath(str(temp_chunk_path))
                
                self.con.execute(f"""
                    COPY (
                        {chunked_query}
                    ) TO {quoted_temp_path} ({self._csv_copy_options(include_header=False)})
                """)
                
                # Append chunk to main file with proper encoding
                try:
                    with open(temp_chunk_path, 'r', encoding='utf-8', newline='') as chunk_file:
                        with open(output_path, 'a', encoding='utf-8', newline='') as main_file:
                            main_file.write(chunk_file.read())
                except UnicodeDecodeError:
                    # Fallback to detect encoding
                    with open(temp_chunk_path, 'r', encoding='latin-1', newline='') as chunk_file:
                        with open(output_path, 'a', encoding='utf-8', newline='') as main_file:
                            main_file.write(chunk_file.read())
                
                # Clean up temp file
                temp_chunk_path.unlink()
            
            offset += chunk_size
            
            if chunk_num % 10 == 0:
                progress_pct = round(100 * offset / total_rows, 1)
                print(f"  📊 Export Progress: {offset:,}/{total_rows:,} rows ({progress_pct}%)")
                
        logger.info("comparator.export_full_csv.complete",
                   output_path=str(output_path),
                   total_rows=total_rows,
                   chunks=chunk_num)

    def export_differences(self, left_table: str, right_table: str,
                          config: ComparisonConfig,
                          output_dir: Path, left_dataset_config=None, right_dataset_config=None) -> Dict[str, Path]:
        """
        Export differences to files with REPORT FIDELITY PATTERN support.
        
        Args:
            left_table: Left table name
            right_table: Right table name
            config: Comparison configuration
            output_dir: Output directory
            left_dataset_config: Left dataset configuration (for column mappings)
            right_dataset_config: Right dataset configuration (for column mappings)
            
        Returns:
            Dictionary of output file paths
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Store dataset configs for column mapping (BUG 3 fix)
        self.left_dataset_config = left_dataset_config
        self.right_dataset_config = right_dataset_config
        
        outputs = {}
        key_columns = config.comparison_keys
        
        # Validate key columns exist (CLAUDE.md: Fail fast with clear messages)
        if not key_columns:
            raise ValueError(
                f"No key columns configured for comparison. "
                f"Cannot export differences without join keys."
            )
        
        # BUG 3 fix: Use column mapping for join conditions with normalized and quoted columns
        key_join = " AND ".join([
            f"TRIM(TRY_CAST(l.{qident(normalize_column_name(col))} AS VARCHAR)) = TRIM(TRY_CAST(r.{qident(normalize_column_name(self._get_right_column(col)))} AS VARCHAR))" 
            for col in key_columns
        ])
        
        # Export only in left (keep existing behavior)
        only_left_path = output_dir / f"only_in_{left_table}.csv"
        right_key_col = qident(normalize_column_name(self._get_right_column(key_columns[0])))
        
        if hasattr(config, 'export_full') and config.export_full:
            # Full export using chunked processing
            only_left_query = f"""
                SELECT l.*
                FROM {left_table} l
                LEFT JOIN {right_table} r ON {key_join}
                WHERE r.{right_key_col} IS NULL
            """
            self._export_full_csv(only_left_query, only_left_path, getattr(config, 'chunk_export_size', 50000))
        else:
            # Preview export with limit
            quoted_only_left_path = qpath(str(only_left_path))
            self.con.execute(f"""
                COPY (
                    SELECT l.*
                    FROM {left_table} l
                    LEFT JOIN {right_table} r ON {key_join}
                    WHERE r.{right_key_col} IS NULL
                    LIMIT {config.max_differences}
                ) TO {quoted_only_left_path} ({self._csv_copy_options()})
            """)
        outputs["only_left"] = only_left_path
        
        # Export only in right (keep existing behavior)
        only_right_path = output_dir / f"only_in_{right_table}.csv"
        
        if hasattr(config, 'export_full') and config.export_full:
            # Full export using chunked processing
            only_right_query = f"""
                SELECT r.*
                FROM {right_table} r
                LEFT JOIN {left_table} l ON {key_join}
                WHERE l.{qident(normalize_column_name(key_columns[0]))} IS NULL
            """
            self._export_full_csv(only_right_query, only_right_path, getattr(config, 'chunk_export_size', 50000))
        else:
            # Preview export with limit
            quoted_only_right_path = qpath(str(only_right_path))
            self.con.execute(f"""
                COPY (
                    SELECT r.*
                    FROM {right_table} r
                    LEFT JOIN {left_table} l ON {key_join}
                    WHERE l.{qident(normalize_column_name(key_columns[0]))} IS NULL
                    LIMIT {config.max_differences}
                ) TO {quoted_only_right_path} ({self._csv_copy_options()})
            """)
        outputs["only_right"] = only_right_path
        
        # NEW: Enhanced value differences export with REPORT FIDELITY PATTERN
        value_diff_path = output_dir / "value_differences.csv"
        
        # Get value columns to compare
        value_columns = self._determine_value_columns(
            left_table, right_table, config, key_columns
        )
        
        if value_columns:
            # Create temporary config for exact comparison (no tolerance)
            export_config = ComparisonConfig(left_dataset="temp_left", right_dataset="temp_right")
            export_config.tolerance = 0  # Always exact comparison for export
            
            # NEW: Build ANNOTATION SQL with CTEs for entire_column flag
            # This implements the core REPORT FIDELITY PATTERN functionality
            if hasattr(config, 'annotate_entire_column') and config.annotate_entire_column:
                
                # Step 1: Build CTEs to compute entire_column flags for each column
                column_summary_ctes = []
                union_queries = []
                
                for col in value_columns:
                    norm_col_unquoted = normalize_column_name(col)
                    right_col = self._get_right_column(col)
                    norm_right_col_unquoted = normalize_column_name(right_col)
                    
                    # For SQL generation, use quoted versions
                    norm_col = qident(norm_col_unquoted)
                    norm_right_col = qident(norm_right_col_unquoted)
                    
                    # Build the robust difference condition for filtering (uses quoted names)
                    difference_condition = self._build_robust_comparison_condition(
                        norm_col, norm_right_col, export_config
                    ).strip()
                    
                    # CTE to compute summary statistics for this column (use unquoted for name)
                    cte_name = f"col_{norm_col_unquoted}_summary"
                    column_summary_ctes.append(f"""
                        {cte_name} AS (
                            SELECT 
                                COUNT(*) as total_matched_rows,
                                SUM(CASE WHEN {difference_condition} THEN 1 ELSE 0 END) as different_rows
                            FROM {left_table} l
                            INNER JOIN {right_table} r ON {key_join}
                        )
                    """)
                    
                    # Build key columns selection
                    key_selects = []
                    for key in key_columns:
                        norm_key = qident(normalize_column_name(key))
                        key_selects.append(f"l.{norm_key}")
                    
                    # Union query with entire_column annotation
                    union_query = f"""
                        SELECT 
                            {', '.join(key_selects)} AS "Key",
                            '{col}' AS "Differing Column",
                            TRY_CAST(l.{norm_col} AS VARCHAR) AS "Left Value",
                            TRY_CAST(r.{norm_right_col} AS VARCHAR) AS "Right Value",
                            CASE 
                                WHEN l.{norm_col} IS NULL AND r.{norm_right_col} IS NOT NULL THEN 'Missing in Left'
                                WHEN l.{norm_col} IS NOT NULL AND r.{norm_right_col} IS NULL THEN 'Missing in Right'
                                ELSE 'Different Values'
                            END AS "Difference Type",
                            CASE 
                                WHEN s.different_rows = s.total_matched_rows AND s.total_matched_rows > 0 THEN 'true'
                                ELSE 'false'
                            END AS "Entire Column Different"
                        FROM {left_table} l
                        INNER JOIN {right_table} r ON {key_join}
                        CROSS JOIN {cte_name} s
                        WHERE {difference_condition}
                    """
                    
                    union_queries.append(union_query)
                
                # Combine CTEs and union queries
                if column_summary_ctes and union_queries:
                    annotated_query = f"""
                        WITH {', '.join(column_summary_ctes)}
                        {' UNION ALL '.join(union_queries)}
                        ORDER BY "Key", "Differing Column"
                    """
                else:
                    # Fallback for no columns
                    annotated_query = f"""
                        SELECT 
                            TRY_CAST(NULL AS VARCHAR) AS "Key",
                            TRY_CAST(NULL AS VARCHAR) AS "Differing Column", 
                            TRY_CAST(NULL AS VARCHAR) AS "Left Value",
                            TRY_CAST(NULL AS VARCHAR) AS "Right Value",
                            TRY_CAST(NULL AS VARCHAR) AS "Difference Type",
                            TRY_CAST(NULL AS VARCHAR) AS "Entire Column Different"
                        WHERE FALSE
                    """
                
                # NEW: Generate both full and preview exports with enhanced naming
                if hasattr(config, 'export_full') and config.export_full:
                    
                    # HYBRID FULL EXPORT: Always generate hybrid export (collapsed summaries + all partial differences)
                    # This ensures full export is never empty when preview has data
                    # Use UNION ALL to combine: (1) collapsed summaries for entire columns, (2) all partial differences
                    hybrid_query = f"""
                        WITH annotated_data AS (
                            {annotated_query}
                        ),
                        summaries AS (
                            SELECT 
                                {qident("Key")},
                                {qident("Differing Column")},
                                {qident("Left Value")},
                                {qident("Right Value")},
                                {qident("Difference Type")},
                                {qident("Entire Column Different")},
                                2::BIGINT AS sample,
                                ROW_NUMBER() OVER (
                                    PARTITION BY {qident("Differing Column")} 
                                    ORDER BY {qident("Key")}
                                ) as rn
                            FROM annotated_data 
                            WHERE {qident("Entire Column Different")} = 'true'
                        ),
                        collapsed_summaries AS (
                            SELECT * EXCLUDE (rn)
                            FROM summaries
                            WHERE rn = 1
                        ),
                        partials AS (
                            SELECT 
                                {qident("Key")},
                                {qident("Differing Column")},
                                {qident("Left Value")},
                                {qident("Right Value")},
                                {qident("Difference Type")},
                                {qident("Entire Column Different")},
                                0::BIGINT AS sample
                            FROM annotated_data 
                            WHERE {qident("Entire Column Different")} = 'false'
                        )
                        SELECT * EXCLUDE (sample) FROM (
                            SELECT * FROM collapsed_summaries
                            UNION ALL
                            SELECT * FROM partials
                        ) AS full_out
                        ORDER BY {qident("Differing Column")}, {qident("Key")}, sample DESC
                    """
                    
                    # HYBRID: Standard full export is now hybrid (collapsed summaries + all partial differences)
                    value_diff_full_path = output_dir / "value_differences_full.csv"
                    self._export_full_csv(hybrid_query, value_diff_full_path, 
                                        getattr(config, 'chunk_export_size', 50000),
                                        order_cols=["Differing Column", "Key"])
                    outputs["value_differences_full"] = value_diff_full_path
                    
                    # Check for audit functionality
                    if hasattr(config, 'export_rowlevel_audit_full') and config.export_rowlevel_audit_full:
                        # Generate audit export - include all row-level differences with metadata
                        audit_query = f"""
                            WITH annotated_data AS (
                                {annotated_query}
                            )
                            SELECT 
                                *,
                                CURRENT_TIMESTAMP AS audit_timestamp,
                                'row_level_audit' AS audit_type
                            FROM annotated_data 
                            ORDER BY {qident("Key")}, {qident("Differing Column")}
                        """
                        
                        # Use enhanced naming for audit exports
                        value_diff_audit_path = output_dir / "value_differences_full_audit_part001.csv"
                        self._export_full_csv(audit_query, value_diff_audit_path, 
                                            getattr(config, 'chunk_export_size', 50000),
                                            order_cols=["Key", "Differing Column"])
                        outputs["value_differences_full_audit"] = value_diff_audit_path
                    
                    # NOTE: Removed old fallback logic - collapse is now permanent for all full exports
                
                # NEW: Enhanced smart preview logic with QUALIFY fallback and configurable ordering
                if hasattr(config, 'enable_smart_preview') and config.enable_smart_preview:
                    # Smart preview with summaries, samples, and partials
                    preview_limit = getattr(config, 'csv_preview_limit', 1000)
                    sample_size = getattr(config, 'entire_column_sample_size', 10)
                    
                    # Ensure we have integer values (not Mock objects)
                    if not isinstance(preview_limit, int):
                        preview_limit = 1000
                    if not isinstance(sample_size, int):
                        sample_size = 10
                    
                    # Get configurable preview order (defaults to ["Differing Column", "Key"])
                    preview_order = getattr(config, 'preview_order', ["Differing Column", "Key"])
                    # Ensure we use the standardized column names that exist in annotated_data
                    # The annotated_data CTE produces: "Key", "Differing Column", "Left Value", "Right Value", etc.
                    quoted_order_cols = [qident(col) for col in preview_order]
                    order_clause = f"ORDER BY {', '.join(quoted_order_cols)}"
                    
                    # PERMANENT COLLAPSE: Preview now always uses collapsed mode
                    # No configuration flag needed - this is the new permanent behavior
                    
                    # Use ROW_NUMBER() for compatibility with uniform UNION schema - permanent collapse
                    # PERMANENT COLLAPSE MODE: For entire_column=TRUE, show only 1 summary row per column (no samples)
                    smart_preview_query = f"""
                            SELECT * FROM (
                                WITH annotated_data AS (
                                    {annotated_query}
                                ),
                                summaries AS (
                                    SELECT *, ROW_NUMBER() OVER (PARTITION BY {qident("Differing Column")} {order_clause}) as rn
                                    FROM annotated_data 
                                    WHERE {qident("Entire Column Different")} = 'true'
                                ),
                                samples AS (
                                    SELECT *, ROW_NUMBER() OVER ({order_clause}) as rn
                                    FROM annotated_data 
                                    WHERE {qident("Entire Column Different")} = 'false'
                                ),
                                partials AS (
                                    SELECT *, ROW_NUMBER() OVER ({order_clause}) as rn
                                    FROM annotated_data
                                    WHERE {qident("Key")} NOT IN (
                                        SELECT {qident("Key")} FROM summaries WHERE rn <= 1
                                        UNION ALL
                                        SELECT {qident("Key")} FROM samples WHERE rn <= {sample_size}
                                    )
                                )
                                -- UNIFORM SCHEMA: All branches project identical columns with compatible types
                                SELECT 
                                    {qident("Differing Column")},
                                    {qident("Key")},
                                    {qident("Left Value")},
                                    {qident("Right Value")},
                                    {qident("Difference Type")},
                                    {qident("Entire Column Different")} AS entire_column,
                                    CAST(2 AS BIGINT) AS sample
                                FROM summaries 
                                WHERE rn <= 1
                                UNION ALL
                                SELECT 
                                    {qident("Differing Column")},
                                    {qident("Key")},
                                    {qident("Left Value")},
                                    {qident("Right Value")},
                                    {qident("Difference Type")},
                                    {qident("Entire Column Different")} AS entire_column,
                                    CAST(1 AS BIGINT) AS sample
                                FROM samples 
                                WHERE rn <= {sample_size}
                                UNION ALL
                                SELECT 
                                    {qident("Differing Column")},
                                    {qident("Key")},
                                    {qident("Left Value")},
                                    {qident("Right Value")},
                                    {qident("Difference Type")},
                                    {qident("Entire Column Different")} AS entire_column,
                                    CAST(0 AS BIGINT) AS sample
                                FROM partials 
                                WHERE rn <= {preview_limit - sample_size - 1}
                            ) AS preview
                            ORDER BY {', '.join(quoted_order_cols)}, sample DESC
                        """
                    
                    # Use qpath for Windows path safety
                    quoted_preview_path = qpath(str(value_diff_path))
                    self.con.execute(f"""
                        COPY (
                            {smart_preview_query}
                        ) TO {quoted_preview_path} ({self._csv_copy_options()})
                    """)
                else:
                    # Simple preview with limit
                    preview_query = f"""
                        {annotated_query}
                        LIMIT {getattr(config, 'csv_preview_limit', 500)}
                    """
                    
                    # Use qpath for Windows path safety
                    quoted_preview_path = qpath(str(value_diff_path))
                    self.con.execute(f"""
                        COPY (
                            {preview_query}
                        ) TO {quoted_preview_path} ({self._csv_copy_options()})
                    """)
                    
            else:
                # Fallback to original logic without annotation
                comparisons = []
                for col in value_columns:
                    norm_col = qident(normalize_column_name(col))
                    right_col = self._get_right_column(col)
                    norm_right_col = qident(normalize_column_name(right_col))
                    
                    comparisons.append(self._build_robust_comparison_condition(
                        norm_col, norm_right_col, export_config
                    ))
                
                where_clause = " OR ".join(comparisons) if comparisons else "FALSE"
                
                union_queries = []
                for col in value_columns:
                    norm_col = qident(normalize_column_name(col))
                    right_col = self._get_right_column(col)
                    norm_right_col = qident(normalize_column_name(right_col))
                    
                    difference_condition = self._build_robust_comparison_condition(
                        norm_col, norm_right_col, export_config
                    ).strip()
                    
                    key_selects = []
                    for key in key_columns:
                        norm_key = qident(normalize_column_name(key))
                        key_selects.append(f"l.{norm_key}")
                    
                    union_query = f"""
                        SELECT 
                            {', '.join(key_selects)} AS "Key",
                            '{col}' AS "Differing Column",
                            TRY_CAST(l.{norm_col} AS VARCHAR) AS "Left Value",
                            TRY_CAST(r.{norm_right_col} AS VARCHAR) AS "Right Value",
                            CASE 
                                WHEN l.{norm_col} IS NULL AND r.{norm_right_col} IS NOT NULL THEN 'Missing in Left'
                                WHEN l.{norm_col} IS NOT NULL AND r.{norm_right_col} IS NULL THEN 'Missing in Right'
                                ELSE 'Different Values'
                            END AS "Difference Type"
                        FROM {left_table} l
                        INNER JOIN {right_table} r ON {key_join}
                        WHERE {difference_condition}
                    """
                    
                    union_queries.append(union_query)
                
                if union_queries:
                    final_query = f"""
                        SELECT * FROM (
                            {' UNION ALL '.join(union_queries)}
                        ) ORDER BY "Key", "Differing Column"
                        LIMIT {config.max_differences}
                    """
                else:
                    final_query = f"""
                        SELECT 
                            TRY_CAST(NULL AS VARCHAR) AS "Key",
                            TRY_CAST(NULL AS VARCHAR) AS "Differing Column", 
                            TRY_CAST(NULL AS VARCHAR) AS "Left Value",
                            TRY_CAST(NULL AS VARCHAR) AS "Right Value",
                            TRY_CAST(NULL AS VARCHAR) AS "Difference Type"
                        WHERE FALSE
                    """
                
                # Use qpath for Windows path safety
                quoted_value_diff_path = qpath(str(value_diff_path))
                self.con.execute(f"""
                    COPY (
                        {final_query}
                    ) TO {quoted_value_diff_path} ({self._csv_copy_options()})
                """)
            
            outputs["value_differences"] = value_diff_path
            
            # Also create a summary report
            summary_path = output_dir / "comparison_summary.txt"
            self._export_summary_report(
                summary_path, left_table, right_table, 
                key_columns, value_columns, config
            )
            outputs["summary"] = summary_path
        
        # NEW: Zipping and manifest generation for large exports
        if hasattr(config, 'zip_large_exports') and config.zip_large_exports:
            zip_info = self._create_zip_archive_and_manifest(outputs, output_dir, config)
            if zip_info:
                outputs.update(zip_info)
        
        logger.info("comparator.exported_differences",
                   files=list(outputs.keys()))
        
        return outputs
    
    def _create_zip_archive_and_manifest(self, outputs: Dict[str, Path], output_dir: Path, config: ComparisonConfig) -> Dict[str, Path]:
        """
        Create ZIP archive and manifest for large exports.
        
        Args:
            outputs: Dictionary of output file paths
            output_dir: Output directory
            config: Comparison configuration
            
        Returns:
            Dictionary with zip and manifest paths, or empty dict if not needed
        """
        import zipfile
        import json
        from datetime import datetime
        
        # Determine which files to zip (only large exports)
        files_to_zip = []
        file_sizes = {}
        total_size = 0
        
        for file_type, file_path in outputs.items():
            if file_path.exists():
                file_size = file_path.stat().st_size
                file_sizes[file_type] = file_size
                total_size += file_size
                
                # Zip full exports and large files (> 10MB or when requested)
                if "full" in file_type or file_size > 10_000_000:
                    files_to_zip.append((file_type, file_path))
        
        if not files_to_zip:
            return {}
        
        # Create ZIP archive
        zip_path = output_dir / "data_comparison_exports.zip"
        
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for file_type, file_path in files_to_zip:
                # Add file to zip with a clean name
                archive_name = f"{file_type}_{file_path.name}"
                zip_file.write(file_path, archive_name)
                logger.info("comparator.zip.added_file", 
                           file_type=file_type,
                           file_path=str(file_path),
                           archive_name=archive_name)
        
        # Create manifest
        manifest_data = {
            "created_at": datetime.now().isoformat(),
            "zip_file": str(zip_path.name),
            "configuration": {
                "csv_preview_limit": getattr(config, 'csv_preview_limit', 1000),
                "entire_column_sample_size": getattr(config, 'entire_column_sample_size', 10),
                "collapse_entire_column_in_preview": getattr(config, 'collapse_entire_column_in_preview', False),
                "collapse_entire_column_in_full": getattr(config, 'collapse_entire_column_in_full', False),
                "export_rowlevel_audit_full": getattr(config, 'export_rowlevel_audit_full', False),
                "zip_large_exports": getattr(config, 'zip_large_exports', False),
                "preview_order": getattr(config, 'preview_order', ["Differing Column", "Key"]),
                "export_full": getattr(config, 'export_full', True),
                "annotate_entire_column": getattr(config, 'annotate_entire_column', True),
                "chunk_export_size": getattr(config, 'chunk_export_size', 50000),
                "enable_smart_preview": getattr(config, 'enable_smart_preview', True)
            },
            "files": {
                "zipped_files": [
                    {
                        "type": file_type,
                        "original_path": str(file_path),
                        "archive_name": f"{file_type}_{file_path.name}",
                        "size_bytes": file_sizes[file_type]
                    }
                    for file_type, file_path in files_to_zip
                ],
                "unzipped_files": [
                    {
                        "type": file_type,
                        "path": str(file_path),
                        "size_bytes": file_sizes.get(file_type, 0)
                    }
                    for file_type, file_path in outputs.items()
                    if (file_type, file_path) not in files_to_zip and file_path.exists()
                ]
            },
            "totals": {
                "total_files": len(outputs),
                "zipped_files": len(files_to_zip),
                "total_size_bytes": total_size,
                "zip_size_bytes": zip_path.stat().st_size if zip_path.exists() else 0
            },
            "notes": {
                "preview_note": "Preview CSVs may be truncated/summarized; full chunked outputs contain complete row-level data.",
                "collapse_logic": "Collapsed exports show only columns where all values differ.",
                "deterministic_ordering": f"All exports use deterministic ordering: {getattr(config, 'preview_order', ['Differing Column', 'Key'])}",
                "chunking": f"Large datasets are automatically chunked with size: {getattr(config, 'chunk_export_size', 50000)} rows"
            }
        }
        
        # Write manifest
        manifest_path = output_dir / "report_manifest.json"
        with open(manifest_path, 'w', encoding='utf-8') as f:
            json.dump(manifest_data, f, indent=2, ensure_ascii=False)
        
        logger.info("comparator.zip.complete",
                   zip_path=str(zip_path),
                   manifest_path=str(manifest_path),
                   files_zipped=len(files_to_zip),
                   total_size=total_size)
        
        return {
            "zip_archive": zip_path,
            "manifest": manifest_path
        }
    
    def _quote_identifier(self, identifier: str) -> str:
        """
        Quote SQL identifiers to handle spaces and special characters in DuckDB.
        
        Args:
            identifier: Column name or identifier to quote
            
        Returns:
            Quoted identifier safe for SQL usage
        """
        # Always quote identifiers for safety and consistency
        return f'"{identifier}"'
    
    def _export_summary_report(self, summary_path: Path, left_table: str, 
                              right_table: str, key_columns: List[str],
                              value_columns: List[str], config: ComparisonConfig):
        """Export a human-readable summary report."""
        
        # Get statistics
        left_count = self._get_row_count(left_table)
        right_count = self._get_row_count(right_table)
        
        # Get match statistics using normalized and quoted keys
        # Robust type coercion with mandatory trim: Cast to VARCHAR and remove whitespace
        key_join = " AND ".join([
            f"TRIM(TRY_CAST(l.{qident(normalize_column_name(col))} AS VARCHAR)) = TRIM(TRY_CAST(r.{qident(normalize_column_name(self._get_right_column(col)))} AS VARCHAR))" 
            for col in key_columns
        ])
        
        matched_count = self.con.execute(f"""
            SELECT COUNT(*) FROM {left_table} l 
            INNER JOIN {right_table} r ON {key_join}
        """).fetchone()[0]
        
        right_key_col = normalize_column_name(self._get_right_column(key_columns[0]))
        only_left_count = self.con.execute(f"""
            SELECT COUNT(*) FROM {left_table} l 
            LEFT JOIN {right_table} r ON {key_join}
            WHERE r.{right_key_col} IS NULL
        """).fetchone()[0]
        
        only_right_count = self.con.execute(f"""
            SELECT COUNT(*) FROM {right_table} r 
            LEFT JOIN {left_table} l ON {key_join}
            WHERE l.{normalize_column_name(key_columns[0])} IS NULL
        """).fetchone()[0]
        
        # Count value differences using robust comparison logic
        value_diff_count = 0
        if value_columns and matched_count > 0:
            # Create a temporary config for exact comparison (no tolerance)
            summary_config = ComparisonConfig(left_dataset="temp_left", right_dataset="temp_right")
            summary_config.tolerance = 0  # Always exact comparison for summary
            
            comparisons = []
            for col in value_columns:
                norm_col = normalize_column_name(col)
                right_col = self._get_right_column(col)
                norm_right_col = normalize_column_name(right_col)
                
                # Use centralized robust comparison logic for consistent results
                comparisons.append(self._build_robust_comparison_condition(
                    norm_col, norm_right_col, summary_config
                ))
            
            where_clause = " OR ".join(comparisons)
            
            value_diff_count = self.con.execute(f"""
                SELECT COUNT(*) FROM {left_table} l
                INNER JOIN {right_table} r ON {key_join}
                WHERE {where_clause}
            """).fetchone()[0]
        
        # Write summary report
        with open(summary_path, 'w') as f:
            f.write("=" * 70 + "\n")
            f.write("DATA COMPARISON SUMMARY REPORT\n")
            f.write("=" * 70 + "\n\n")
            
            # Use friendly dataset names
            left_name = self._get_friendly_dataset_name(left_table)
            right_name = self._get_friendly_dataset_name(right_table)
            
            f.write(f"Left Dataset:  {left_name} ({left_table})\n")
            f.write(f"Right Dataset: {right_name} ({right_table})\n\n")
            
            f.write(f"Key Columns: {', '.join(key_columns)}\n")
            f.write(f"Value Columns Compared: {len(value_columns)}\n\n")
            
            f.write("-" * 70 + "\n")
            f.write("STATISTICS\n")
            f.write("-" * 70 + "\n\n")
            
            f.write(f"Total rows in left dataset:  {left_count:,}\n")
            f.write(f"Total rows in right dataset: {right_count:,}\n\n")
            
            f.write(f"Matched rows (same keys):     {matched_count:,}\n")
            f.write(f"Only in left dataset:         {only_left_count:,}\n")
            f.write(f"Only in right dataset:        {only_right_count:,}\n")
            f.write(f"Rows with value differences:  {value_diff_count:,}\n\n")
            
            # Calculate percentages
            if matched_count > 0:
                diff_rate = (value_diff_count / matched_count) * 100
                f.write(f"Difference rate: {diff_rate:.2f}% of matched rows have differences\n")
            
            total_unique = left_count + right_count - matched_count
            if total_unique > 0:
                match_rate = (matched_count / total_unique) * 100
                f.write(f"Match rate: {match_rate:.2f}% of unique records match on keys\n")
            
            f.write("\n" + "=" * 70 + "\n")
            f.write("REPORT CONFIGURATION & FIDELITY\n") 
            f.write("=" * 70 + "\n\n")
            
            # Report fidelity configuration - use safe type conversion to handle Mock objects
            f.write("Report Configuration:\n")
            
            # Safely extract config values with proper type conversion
            csv_preview_limit = getattr(config, 'csv_preview_limit', 1000)
            if not isinstance(csv_preview_limit, int):
                csv_preview_limit = 1000
                
            entire_column_sample_size = getattr(config, 'entire_column_sample_size', 10)
            if not isinstance(entire_column_sample_size, int):
                entire_column_sample_size = 10
                
            chunk_export_size = getattr(config, 'chunk_export_size', 50000)
            if not isinstance(chunk_export_size, int):
                chunk_export_size = 50000
                
            preview_order = getattr(config, 'preview_order', ['Differing Column', 'Key'])
            if not isinstance(preview_order, list):
                preview_order = ['Differing Column', 'Key']
            
            f.write(f"  Preview limit: {csv_preview_limit:,} rows\n")
            f.write(f"  Entire column sample size: {entire_column_sample_size}\n")
            f.write(f"  Collapse entire columns in preview: {getattr(config, 'collapse_entire_column_in_preview', False)}\n")
            f.write(f"  Collapse entire columns in full: {getattr(config, 'collapse_entire_column_in_full', False)}\n")
            f.write(f"  Export row-level audit full: {getattr(config, 'export_rowlevel_audit_full', False)}\n")
            f.write(f"  ZIP large exports: {getattr(config, 'zip_large_exports', False)}\n")
            f.write(f"  Deterministic ordering: {', '.join(preview_order)}\n")
            f.write(f"  Export full datasets: {getattr(config, 'export_full', True)}\n")
            f.write(f"  Annotate entire columns: {getattr(config, 'annotate_entire_column', True)}\n")
            f.write(f"  Chunk export size: {chunk_export_size:,} rows\n")
            f.write(f"  Smart preview enabled: {getattr(config, 'enable_smart_preview', True)}\n\n")
            
            f.write("-" * 70 + "\n")
            f.write("REPORT FILES GENERATED\n") 
            f.write("-" * 70 + "\n\n")
            
            # Core files
            f.write("Core Reports:\n")
            f.write("1. value_differences.csv - Preview of rows with different values\n")
            f.write(f"2. only_in_{left_table}.csv - Rows only in left dataset\n")
            f.write(f"3. only_in_{right_table}.csv - Rows only in right dataset\n")
            f.write("4. comparison_summary.txt - This summary report\n\n")
            
            # Enhanced exports (when enabled)
            enhanced_files = []
            if hasattr(config, 'export_full') and config.export_full:
                if hasattr(config, 'collapse_entire_column_in_full') and config.collapse_entire_column_in_full:
                    enhanced_files.append("5. value_differences_full_collapsed_part001.csv - Full export of entirely different columns")
                if hasattr(config, 'export_rowlevel_audit_full') and config.export_rowlevel_audit_full:
                    enhanced_files.append("6. value_differences_full_audit_part001.csv - Full export with audit metadata")
                if not enhanced_files:  # Standard full export
                    enhanced_files.append("5. value_differences_full.csv - Complete row-level differences")
            
            if enhanced_files:
                f.write("Enhanced Exports:\n")
                for file_desc in enhanced_files:
                    f.write(f"{file_desc}\n")
                f.write("\n")
            
            # Archive files (when enabled)
            if hasattr(config, 'zip_large_exports') and config.zip_large_exports:
                f.write("Archive Files:\n")
                f.write("7. data_comparison_exports.zip - Compressed archive of large exports\n")
                f.write("8. report_manifest.json - Detailed metadata and file inventory\n\n")
            
            # Important notes
            f.write("-" * 70 + "\n")
            f.write("IMPORTANT NOTES\n") 
            f.write("-" * 70 + "\n\n")
            
            f.write("Data Fidelity:\n")
            f.write("• Preview CSVs may be truncated/summarized for performance\n")
            f.write("• Full chunked outputs contain complete row-level data\n")
            f.write("• All exports use deterministic ordering for reproducibility\n")
            if hasattr(config, 'annotate_entire_column') and config.annotate_entire_column:
                f.write("• 'Entire Column Different' flag indicates columns where ALL values differ\n")
            if hasattr(config, 'enable_smart_preview') and config.enable_smart_preview:
                f.write("• Smart preview combines summaries, samples, and partial data\n")
            f.write("\n")
            
            f.write("Processing:\n")
            if hasattr(config, 'chunk_export_size'):
                # Use the safe chunk_export_size we already computed above
                f.write(f"• Large datasets automatically chunked at {chunk_export_size:,} rows\n")
            f.write("• All SQL identifiers properly quoted for reserved words/spaces\n")
            f.write("• UTF-8 encoding used for all exports (Windows cp1252 safe)\n")
            f.write("• ROW_NUMBER() used instead of QUALIFY for DuckDB compatibility\n")
            
            f.write("\n" + "=" * 70 + "\n")
            
        logger.info("comparator.summary_exported", path=str(summary_path))
    
    def _get_friendly_dataset_name(self, table_name: str) -> str:
        """
        Extract a friendly, human-readable name from the dataset table name.
        Makes any table name more readable by formatting it nicely.
        
        Examples:
        - "customer_data_v1" -> "Customer Data V1"
        - "sales_2024_q1" -> "Sales 2024 Q1"
        - "employee_records" -> "Employee Records"
        
        Args:
            table_name: The internal table name
            
        Returns:
            A friendly, formatted dataset name
        """
        # Remove file extensions if present
        clean_name = table_name.replace('.csv', '').replace('.xlsx', '').replace('.xls', '').replace('.parquet', '')
        
        # Replace underscores and hyphens with spaces
        clean_name = clean_name.replace('_', ' ').replace('-', ' ')
        
        # Split into words and capitalize each word properly
        words = clean_name.split()
        formatted_words = []
        
        for word in words:
            # Check if it's an acronym (all uppercase and more than 1 char)
            if word.isupper() and len(word) > 1:
                formatted_words.append(word)  # Keep acronyms as-is
            # Check if it contains numbers
            elif any(char.isdigit() for char in word):
                # Keep mixed alphanumeric as-is but capitalize first letter if it's a letter
                if word[0].isalpha():
                    formatted_words.append(word[0].upper() + word[1:])
                else:
                    formatted_words.append(word)
            else:
                # Regular word - capitalize first letter
                formatted_words.append(word.capitalize())
        
        return ' '.join(formatted_words)