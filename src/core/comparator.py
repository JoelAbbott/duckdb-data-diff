"""
Core data comparison logic.
Single responsibility: compare two datasets and identify differences.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional, Tuple
import duckdb
import pandas as pd
from pathlib import Path

from ..utils.logger import get_logger
from ..config.manager import ComparisonConfig
from ..utils.normalizers import normalize_column_name


logger = get_logger()


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
            
            key_conditions.append(f"l.{left_norm} = r.{right_norm}")
        
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
        # Normalize key columns to match staged table format
        key_join = " AND ".join([
            f"l.{normalize_column_name(col)} = r.{normalize_column_name(self._get_right_column(col))}" 
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
        # Normalize key columns to match staged table format
        key_join = " AND ".join([
            f"l.{normalize_column_name(col)} = r.{normalize_column_name(self._get_right_column(col))}" 
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
        Build a robust comparison condition that handles date/time and string normalization.
        
        This centralizes the comparison logic to eliminate false positives caused by:
        1. Date/time format differences (e.g., '2024-01-01 00:00:00' vs '1/1/2024')
        2. String whitespace and case differences (e.g., ' VALUE A  ' vs 'value a')
        
        Args:
            norm_col: Normalized left column name
            norm_right_col: Normalized right column name
            config: Comparison configuration
            
        Returns:
            SQL condition string for robust comparison
        """
        if config.tolerance > 0:
            # Numeric comparison with tolerance
            return f"""
                (
                    (l.{norm_col} IS NULL AND r.{norm_right_col} IS NOT NULL) OR
                    (l.{norm_col} IS NOT NULL AND r.{norm_right_col} IS NULL) OR
                    (
                        TRY_CAST(l.{norm_col} AS DOUBLE) IS NOT NULL AND
                        TRY_CAST(r.{norm_right_col} AS DOUBLE) IS NOT NULL AND
                        ABS(TRY_CAST(l.{norm_col} AS DOUBLE) - 
                            TRY_CAST(r.{norm_right_col} AS DOUBLE)) > {config.tolerance}
                    ) OR
                    (
                        TRY_CAST(l.{norm_col} AS DOUBLE) IS NULL AND
                        l.{norm_col} != r.{norm_right_col}
                    )
                )
            """
        else:
            # Robust exact comparison with date/time and string normalization
            return f"""
                (
                    (l.{norm_col} IS NULL AND r.{norm_right_col} IS NOT NULL) OR
                    (l.{norm_col} IS NOT NULL AND r.{norm_right_col} IS NULL) OR
                    (
                        -- First try timestamp comparison for date/time values
                        CASE 
                            WHEN TRY_CAST(l.{norm_col} AS TIMESTAMP) IS NOT NULL 
                                 AND TRY_CAST(r.{norm_right_col} AS TIMESTAMP) IS NOT NULL THEN
                                -- Both can be cast to timestamp - compare as timestamps
                                TRY_CAST(l.{norm_col} AS TIMESTAMP) != TRY_CAST(r.{norm_right_col} AS TIMESTAMP)
                            ELSE
                                -- Fall back to robust string comparison with normalization
                                (
                                    CASE 
                                        -- Boolean normalization
                                        WHEN LOWER(CAST(l.{norm_col} AS VARCHAR)) IN ('true', 't', '1', 'yes') THEN 't'
                                        WHEN LOWER(CAST(l.{norm_col} AS VARCHAR)) IN ('false', 'f', '0', 'no', '') THEN 'f'
                                        -- String normalization: lowercase, trim, collapse whitespace
                                        ELSE REGEXP_REPLACE(TRIM(LOWER(CAST(l.{norm_col} AS VARCHAR))), '\\s+', ' ', 'g')
                                    END != 
                                    CASE 
                                        -- Boolean normalization
                                        WHEN LOWER(CAST(r.{norm_right_col} AS VARCHAR)) IN ('true', 't', '1', 'yes') THEN 't'
                                        WHEN LOWER(CAST(r.{norm_right_col} AS VARCHAR)) IN ('false', 'f', '0', 'no', '') THEN 'f'
                                        -- String normalization: lowercase, trim, collapse whitespace
                                        ELSE REGEXP_REPLACE(TRIM(LOWER(CAST(r.{norm_right_col} AS VARCHAR))), '\\s+', ' ', 'g')
                                    END
                                )
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
            key_join = " AND ".join([
                f"l.{normalize_column_name(col)} = r.{normalize_column_name(self._get_right_column(col))}" 
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
            key_join = " AND ".join([
                f"l.{normalize_column_name(col)} = r.{normalize_column_name(self._get_right_column(col))}" 
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
            key_join = " AND ".join([
                f"l.{normalize_column_name(col)} = r.{normalize_column_name(self._get_right_column(col))}" 
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
    
    def export_differences(self, left_table: str, right_table: str,
                          config: ComparisonConfig,
                          output_dir: Path, left_dataset_config=None, right_dataset_config=None) -> Dict[str, Path]:
        """
        Export differences to files.
        
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
        
        # BUG 3 fix: Use column mapping for join conditions with normalized columns
        # Normalize key columns to match staged table format
        key_join = " AND ".join([
            f"l.{normalize_column_name(col)} = r.{normalize_column_name(self._get_right_column(col))}" 
            for col in key_columns
        ])
        
        # Export only in left
        only_left_path = output_dir / f"only_in_{left_table}.csv"
        right_key_col = normalize_column_name(self._get_right_column(key_columns[0]))
        self.con.execute(f"""
            COPY (
                SELECT l.*
                FROM {left_table} l
                LEFT JOIN {right_table} r ON {key_join}
                WHERE r.{right_key_col} IS NULL
                LIMIT {config.max_differences}
            ) TO '{only_left_path}' (HEADER, DELIMITER ',')
        """)
        outputs["only_left"] = only_left_path
        
        # Export only in right
        only_right_path = output_dir / f"only_in_{right_table}.csv"
        self.con.execute(f"""
            COPY (
                SELECT r.*
                FROM {right_table} r
                LEFT JOIN {left_table} l ON {key_join}
                WHERE l.{normalize_column_name(key_columns[0])} IS NULL
                LIMIT {config.max_differences}
            ) TO '{only_right_path}' (HEADER, DELIMITER ',')
        """)
        outputs["only_right"] = only_right_path
        
        # Export value differences (the most important report!)
        # This shows rows that match on keys but have different values
        value_diff_path = output_dir / "value_differences.csv"
        
        # Get value columns to compare
        value_columns = self._determine_value_columns(
            left_table, right_table, config, key_columns
        )
        
        if value_columns:
            # Use generic Left/Right naming for universal compatibility
            # but show the actual dataset names in the summary
            left_dataset_name = "Left"
            right_dataset_name = "Right"
            
            # Build robust comparison to find differences using centralized logic
            # Create a temporary config for exact comparison (no tolerance)
            export_config = ComparisonConfig(left_dataset="temp_left", right_dataset="temp_right")
            export_config.tolerance = 0  # Always exact comparison for export
            
            comparisons = []
            for col in value_columns:
                norm_col = normalize_column_name(col)
                right_col = self._get_right_column(col)
                norm_right_col = normalize_column_name(right_col)
                
                # Use centralized robust comparison logic for consistent results
                comparisons.append(self._build_robust_comparison_condition(
                    norm_col, norm_right_col, export_config
                ))
            
            where_clause = " OR ".join(comparisons) if comparisons else "FALSE"
            
            # Build dynamic column selection for the report with filtering
            # This implements the simplified dynamic filtering approach:
            # Only include columns that have differences (Status != 'Matched')
            select_columns = []
            
            # Add key columns with friendly names (always included)
            for key in key_columns:
                norm_key = normalize_column_name(key)
                # Use the original key name in the header
                select_columns.append(f'l.{norm_key} AS "{key}"')
            
            # Create a dynamic columns expression using CASE statements
            # This conditionally includes value columns only if they have differences
            dynamic_columns = []
            
            for col in value_columns:
                norm_col = normalize_column_name(col)
                right_col = self._get_right_column(col)
                norm_right_col = normalize_column_name(right_col)
                
                # Build the difference condition using robust comparison logic
                difference_condition = self._build_robust_comparison_condition(
                    norm_col, norm_right_col, export_config
                ).strip()
                
                # Add conditional columns that only appear if there's a difference
                # Use CASE to conditionally show the column values
                dynamic_columns.extend([
                    f"""
                    CASE 
                        WHEN {difference_condition} THEN l.{norm_col}
                        ELSE NULL
                    END AS "{left_dataset_name} {col}"
                    """,
                    f"""
                    CASE 
                        WHEN {difference_condition} THEN r.{norm_right_col}
                        ELSE NULL
                    END AS "{right_dataset_name} {col}"
                    """,
                    f"""
                    CASE 
                        WHEN {difference_condition} THEN
                            CASE 
                                WHEN l.{norm_col} IS NULL AND r.{norm_right_col} IS NOT NULL THEN 'Missing in Left'
                                WHEN l.{norm_col} IS NOT NULL AND r.{norm_right_col} IS NULL THEN 'Missing in Right'
                                ELSE 'Different Values'
                            END
                        ELSE NULL
                    END AS "{col} Status"
                    """
                ])
            
            # Add the dynamic columns to the select list
            select_columns.extend(dynamic_columns)
            
            select_stmt = ", ".join(select_columns)
            
            # Export the differences
            self.con.execute(f"""
                COPY (
                    SELECT {select_stmt}
                    FROM {left_table} l
                    INNER JOIN {right_table} r ON {key_join}
                    WHERE {where_clause}
                    LIMIT {config.max_differences}
                ) TO '{value_diff_path}' (HEADER, DELIMITER ',')
            """)
            
            outputs["value_differences"] = value_diff_path
            
            # Also create a summary report
            summary_path = output_dir / "comparison_summary.txt"
            self._export_summary_report(
                summary_path, left_table, right_table, 
                key_columns, value_columns, config
            )
            outputs["summary"] = summary_path
        
        logger.info("comparator.exported_differences",
                   files=list(outputs.keys()))
        
        return outputs
    
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
        
        # Get match statistics using normalized keys
        key_join = " AND ".join([
            f"l.{normalize_column_name(col)} = r.{normalize_column_name(self._get_right_column(col))}" 
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
            f.write("REPORT FILES GENERATED\n") 
            f.write("=" * 70 + "\n\n")
            
            f.write("1. value_differences.csv - Rows with different values\n")
            f.write(f"2. only_in_{left_table}.csv - Rows only in left dataset\n")
            f.write(f"3. only_in_{right_table}.csv - Rows only in right dataset\n")
            f.write("4. comparison_summary.txt - This summary report\n")
            
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