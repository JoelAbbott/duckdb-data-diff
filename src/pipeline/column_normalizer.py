"""
Column normalization pipeline.
Single responsibility: normalize column names across datasets consistently.
"""

import re
from typing import Dict, List, Set, Tuple
import duckdb
import pandas as pd

from ..utils.logger import get_logger
from ..utils.normalizers import normalize_column_name


logger = get_logger()


class ColumnNormalizer:
    """
    Handles comprehensive column normalization across datasets.
    Ensures ALL columns are normalized consistently.
    """
    
    def __init__(self):
        """Initialize column normalizer."""
        self.normalization_map: Dict[str, str] = {}
        self.conflicts: List[Tuple[str, str, str]] = []
    
    def normalize_dataframe_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize all columns in a DataFrame.
        
        Args:
            df: DataFrame with columns to normalize
            
        Returns:
            DataFrame with normalized column names
        """
        logger.debug("column_normalizer.dataframe.start",
                    original_columns=list(df.columns))
        
        # Create mapping
        new_columns = {}
        seen_normalized = {}
        
        for col in df.columns:
            normalized = normalize_column_name(col)
            
            # Handle conflicts (multiple columns normalize to same name)
            if normalized in seen_normalized:
                # Add suffix to make unique
                suffix = 2
                base_normalized = normalized
                while f"{base_normalized}_{suffix}" in seen_normalized:
                    suffix += 1
                normalized = f"{base_normalized}_{suffix}"
                
                logger.warning("column_normalizer.conflict",
                             original1=seen_normalized.get(base_normalized),
                             original2=col,
                             normalized=base_normalized,
                             resolved=normalized)
                
                self.conflicts.append((
                    seen_normalized.get(base_normalized),
                    col,
                    normalized
                ))
            
            new_columns[col] = normalized
            seen_normalized[normalized] = col
            self.normalization_map[col] = normalized
        
        # Rename columns
        df = df.rename(columns=new_columns)
        
        logger.info("column_normalizer.dataframe.complete",
                   original_count=len(new_columns),
                   normalized_count=len(seen_normalized),
                   conflicts=len(self.conflicts))
        
        return df
    
    def normalize_table_columns(self, con: duckdb.DuckDBPyConnection,
                               table_name: str) -> str:
        """
        Normalize all columns in a DuckDB table.
        
        Args:
            con: DuckDB connection
            table_name: Table to normalize
            
        Returns:
            Name of normalized table (may be different if conflicts)
        """
        logger.info("column_normalizer.table.start", table=table_name)
        
        # Get current columns
        result = con.execute(f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = '{table_name}'
            ORDER BY ordinal_position
        """).fetchall()
        
        if not result:
            logger.error("column_normalizer.table.not_found",
                        table=table_name)
            raise ValueError(f"Table {table_name} not found")
        
        # Create normalization map
        column_map = {}
        seen_normalized = set()
        conflicts = []
        
        for col_name, data_type in result:
            normalized = normalize_column_name(col_name)
            original_normalized = normalized
            
            # Handle conflicts
            if normalized in seen_normalized:
                suffix = 2
                while f"{original_normalized}_{suffix}" in seen_normalized:
                    suffix += 1
                normalized = f"{original_normalized}_{suffix}"
                conflicts.append((col_name, original_normalized, normalized))
                
                logger.warning("column_normalizer.table.conflict",
                             column=col_name,
                             target=original_normalized,
                             resolved=normalized)
            
            column_map[col_name] = normalized
            seen_normalized.add(normalized)
        
        # Build SELECT statement with renamed columns
        select_parts = []
        for original, normalized in column_map.items():
            if original != normalized:
                # Need to quote original name and alias to normalized
                select_parts.append(f'"{original}" AS {normalized}')
            else:
                # Already normalized, just select
                select_parts.append(f'"{original}"')
        
        # Create new table with normalized columns
        normalized_table = f"{table_name}_normalized"
        
        create_sql = f"""
            CREATE OR REPLACE TABLE {normalized_table} AS
            SELECT {', '.join(select_parts)}
            FROM "{table_name}"
        """
        
        con.execute(create_sql)
        
        # Drop original and rename
        con.execute(f'DROP TABLE IF EXISTS "{table_name}"')
        con.execute(f'ALTER TABLE {normalized_table} RENAME TO {table_name}')
        
        # Log results
        logger.info("column_normalizer.table.complete",
                   table=table_name,
                   columns_normalized=len(column_map),
                   conflicts_resolved=len(conflicts))
        
        if conflicts:
            for original, target, resolved in conflicts:
                logger.debug("column_normalizer.conflict.detail",
                           original=original,
                           target=target,
                           resolved=resolved)
        
        return table_name
    
    def get_common_columns(self, con: duckdb.DuckDBPyConnection,
                          left_table: str, right_table: str) -> List[str]:
        """
        Get common columns between two tables after normalization.
        
        Args:
            con: DuckDB connection
            left_table: First table name
            right_table: Second table name
            
        Returns:
            List of common normalized column names
        """
        # Get columns from both tables
        left_cols = con.execute(f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = '{left_table}'
        """).fetchall()
        
        right_cols = con.execute(f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = '{right_table}'
        """).fetchall()
        
        # Normalize and find common
        left_normalized = {normalize_column_name(col[0]) for col in left_cols}
        right_normalized = {normalize_column_name(col[0]) for col in right_cols}
        
        common = list(left_normalized & right_normalized)
        
        logger.info("column_normalizer.common_columns",
                   left_count=len(left_normalized),
                   right_count=len(right_normalized),
                   common_count=len(common))
        
        return sorted(common)
    
    def create_column_mapping_report(self) -> Dict[str, any]:
        """
        Create a report of all column normalizations.
        
        Returns:
            Report dictionary with mappings and statistics
        """
        report = {
            "total_columns": len(self.normalization_map),
            "columns_changed": sum(
                1 for orig, norm in self.normalization_map.items()
                if orig != norm
            ),
            "conflicts_resolved": len(self.conflicts),
            "mappings": self.normalization_map,
            "conflicts": [
                {
                    "original1": c[0],
                    "original2": c[1],
                    "resolved_as": c[2]
                }
                for c in self.conflicts
            ]
        }
        
        return report
    
    def validate_normalization(self, con: duckdb.DuckDBPyConnection,
                              table_name: str) -> bool:
        """
        Validate that all columns in table are properly normalized.
        
        Args:
            con: DuckDB connection
            table_name: Table to validate
            
        Returns:
            True if all columns are normalized
        """
        columns = con.execute(f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = '{table_name}'
        """).fetchall()
        
        non_normalized = []
        for (col,) in columns:
            expected = normalize_column_name(col)
            if col != expected:
                non_normalized.append((col, expected))
        
        if non_normalized:
            logger.warning("column_normalizer.validation.failed",
                         table=table_name,
                         non_normalized_count=len(non_normalized))
            for original, expected in non_normalized[:5]:
                logger.debug("column_normalizer.validation.detail",
                           column=original,
                           should_be=expected)
            return False
        
        logger.info("column_normalizer.validation.passed",
                   table=table_name,
                   columns=len(columns))
        return True