"""
Data staging pipeline.
Single responsibility: stage data efficiently to Parquet format.
"""

from pathlib import Path
from typing import Dict, Any, Optional, List
import duckdb
import pandas as pd

from ..adapters.file_reader import UniversalFileReader
from ..utils.logger import get_logger
from ..utils.normalizers import normalize_column_name
from ..config.manager import DatasetConfig


logger = get_logger()


class DataStager:
    """
    Stage data to Parquet format for efficient processing.
    """
    
    def __init__(self, staging_dir: Optional[Path] = None,
                 chunk_size: int = 10000):
        """
        Initialize data stager.
        
        Args:
            staging_dir: Directory for staging files
            chunk_size: Rows per chunk for large files
        """
        self.staging_dir = Path(staging_dir or "data/staging")
        self.staging_dir.mkdir(parents=True, exist_ok=True)
        self.chunk_size = chunk_size
        self.file_reader = UniversalFileReader()
    
    def stage_dataset(self, con: duckdb.DuckDBPyConnection,
                     config: DatasetConfig,
                     force_restage: bool = False) -> str:
        """
        Stage dataset to Parquet.
        
        Args:
            con: DuckDB connection
            config: Dataset configuration
            force_restage: Force restaging even if file exists
            
        Returns:
            Name of staged table in DuckDB
        """
        staging_path = self.staging_dir / f"{config.name}.parquet"
        
        # Check if already staged
        if staging_path.exists() and not force_restage:
            logger.info("stager.using_existing",
                       dataset=config.name,
                       path=str(staging_path))
            
            # Load existing parquet into DuckDB
            con.execute(f"""
                CREATE OR REPLACE TABLE {config.name}_temp AS
                SELECT * FROM '{staging_path}'
            """)
            
            # Normalize column names for the loaded parquet
            self._normalize_columns(con, f"{config.name}_temp")
            
            # Rename to final table name
            con.execute(f"""
                CREATE OR REPLACE TABLE {config.name} AS
                SELECT * FROM {config.name}_temp
            """)
            
            # Clean up temp table
            con.execute(f"DROP TABLE IF EXISTS {config.name}_temp")
            
            return config.name
        
        logger.info("stager.staging_dataset",
                   dataset=config.name,
                   path=config.path)
        
        # Load data
        file_path = Path(config.path)
        
        if config.custom_sql:
            # Use custom SQL for loading
            self._stage_with_sql(con, config, staging_path)
        else:
            # Standard loading
            self._stage_standard(con, config, file_path, staging_path)
        
        # Apply normalizations
        self._apply_normalizations(con, config)
        
        # Apply conversions
        self._apply_conversions(con, config)
        
        # Normalize column names
        self._normalize_columns(con, config.name)
        
        # Save to Parquet
        con.execute(f"""
            COPY {config.name} TO '{staging_path}' 
            (FORMAT PARQUET, COMPRESSION 'snappy')
        """)
        
        # Get final stats
        row_count = con.execute(
            f"SELECT COUNT(*) FROM {config.name}"
        ).fetchone()[0]
        
        logger.info("stager.staged_complete",
                   dataset=config.name,
                   rows=row_count,
                   path=str(staging_path))
        
        return config.name
    
    def _stage_standard(self, con: duckdb.DuckDBPyConnection,
                       config: DatasetConfig,
                       file_path: Path,
                       staging_path: Path):
        """
        Standard file staging.
        """
        suffix = file_path.suffix.lower()
        
        if suffix in ['.xlsx', '.xls']:
            # Read Excel via pandas
            df = self.file_reader.read_excel(file_path)
            # Apply text normalization to handle encoding issues
            from ..utils.text_normalizer import normalize_dataframe_text
            df = normalize_dataframe_text(df)
            con.register(config.name, df)
        elif suffix == '.csv':
            # Use file reader for better encoding handling
            df = self.file_reader.read_csv(file_path)
            con.register(config.name, df)
        elif suffix == '.parquet':
            # Direct Parquet read
            con.execute(f"""
                CREATE OR REPLACE TABLE {config.name} AS
                SELECT * FROM '{file_path}'
            """)
        else:
            # Fall back to pandas
            df = self.file_reader.read(file_path)
            con.register(config.name, df)
    
    def _stage_with_sql(self, con: duckdb.DuckDBPyConnection,
                       config: DatasetConfig,
                       staging_path: Path):
        """
        Stage using custom SQL.
        """
        logger.debug("stager.using_custom_sql",
                    dataset=config.name)
        
        # First load the raw file
        file_path = Path(config.path)
        temp_name = f"{config.name}_raw"
        
        # Load raw data
        suffix = file_path.suffix.lower()
        if suffix == '.csv':
            con.execute(f"""
                CREATE OR REPLACE TABLE {temp_name} AS
                SELECT * FROM read_csv_auto('{file_path}')
            """)
        else:
            df = self.file_reader.read(file_path)
            con.register(temp_name, df)
        
        # Apply custom SQL
        sql = config.custom_sql.replace("{table}", temp_name)
        con.execute(f"""
            CREATE OR REPLACE TABLE {config.name} AS
            {sql}
        """)
        
        # Clean up temp table
        con.execute(f"DROP TABLE IF EXISTS {temp_name}")
    
    def _apply_normalizations(self, con: duckdb.DuckDBPyConnection,
                             config: DatasetConfig):
        """
        Apply normalization functions to columns.
        """
        if not config.normalizers:
            return
        
        logger.debug("stager.applying_normalizers",
                    dataset=config.name,
                    count=len(config.normalizers))
        
        for column, normalizer in config.normalizers.items():
            if normalizer == "strip_hierarchy":
                # Use CREATE TABLE AS SELECT for robust, permanent transformation
                # Strips everything up to and including the last colon, then trims whitespace
                temp_table = f"{config.name}_temp"
                
                # Get all column names for SELECT *
                columns_result = con.execute(f"""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = '{config.name}'
                """).fetchall()
                
                all_columns = [col[0] for col in columns_result]
                
                # Build SELECT with transformation for target column
                select_parts = []
                for col in all_columns:
                    if col == normalize_column_name(column):
                        # Apply strip_hierarchy transformation to this column
                        select_parts.append(f"""
                            CASE 
                                WHEN "{col}" IS NULL THEN NULL
                                WHEN POSITION(':' IN "{col}") = 0 THEN "{col}"
                                ELSE TRIM(REGEXP_REPLACE(
                                    REGEXP_REPLACE("{col}", '^.*: *', ''),
                                    '^\\s+|\\s+$', '', 'g'
                                ))
                            END AS "{col}"
                        """)
                    else:
                        # Keep other columns unchanged
                        select_parts.append(f'"{col}"')
                
                # Execute transformation sequence
                con.execute(f"""
                    CREATE TABLE {temp_table} AS
                    SELECT {', '.join(select_parts)}
                    FROM {config.name}
                """)
                
                # Drop the original table/view safely (same logic as _normalize_columns)
                try:
                    # Try dropping as table first (most common case)
                    con.execute(f"DROP TABLE IF EXISTS {config.name}")
                except:
                    # If that fails, try dropping as view
                    try:
                        con.execute(f"DROP VIEW IF EXISTS {config.name}")
                    except:
                        # If both fail, it doesn't exist (which is fine)
                        pass
                
                con.execute(f"ALTER TABLE {temp_table} RENAME TO {config.name}")
                
            elif normalizer == "unicode_clean":
                # For simplicity, using basic cleaning
                con.execute(f"""
                    UPDATE {config.name}
                    SET "{column}" = TRIM(REGEXP_REPLACE("{column}", '\\s+', ' '))
                """)
            elif normalizer == "collapse_spaces":
                con.execute(f"""
                    UPDATE {config.name}
                    SET "{column}" = TRIM(REGEXP_REPLACE("{column}", '\\s+', ' '))
                """)
    
    def _apply_conversions(self, con: duckdb.DuckDBPyConnection,
                          config: DatasetConfig):
        """
        Apply type conversions to columns.
        """
        if not config.converters:
            return
        
        logger.debug("stager.applying_converters",
                    dataset=config.name,
                    count=len(config.converters))
        
        for column, converter in config.converters.items():
            if converter == "currency_usd":
                con.execute(f"""
                    UPDATE {config.name}
                    SET "{column}" = TRY_CAST(
                        REPLACE(REPLACE(REPLACE("{column}", '$', ''), ',', ''), '(', '-')
                        AS DECIMAL(18,2)
                    )
                """)
            elif converter == "boolean_t_f":
                con.execute(f"""
                    UPDATE {config.name}
                    SET "{column}" = CASE
                        WHEN LOWER("{column}") IN ('t', 'true', '1', 'yes')
                        THEN 't'
                        WHEN LOWER("{column}") IN ('f', 'false', '0', 'no')
                        THEN 'f'
                        ELSE NULL
                    END
                """)
    
    def _normalize_columns(self, con: duckdb.DuckDBPyConnection,
                          table_name: str):
        """
        Normalize all column names.
        """
        logger.debug("stager.normalizing_columns", table=table_name)
        
        # Get current columns
        columns = con.execute(f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = '{table_name}'
        """).fetchall()
        
        # Build rename statement - ALWAYS normalize to ensure consistency
        renames = []
        for (col,) in columns:
            normalized = normalize_column_name(col)
            # Always use normalized name, even if it's the same
            renames.append(f'"{col}" AS {normalized}')
        
        if renames:
            # Create a temp table with normalized columns to avoid duplicates
            # when the original table is a registered DataFrame view
            temp_table = f"{table_name}_normalized_temp"
            
            rename_sql = f"""
                CREATE TABLE {temp_table} AS
                SELECT {', '.join(renames)}
                FROM {table_name}
            """
            con.execute(rename_sql)
            
            # Drop the original table/view safely
            # DuckDB is strict - we need to check what type it is first
            try:
                # Try dropping as table first (most common case)
                con.execute(f"DROP TABLE IF EXISTS {table_name}")
            except:
                # If that fails, try dropping as view
                try:
                    con.execute(f"DROP VIEW IF EXISTS {table_name}")
                except:
                    # If both fail, it doesn't exist (which is fine)
                    pass
            
            # Rename temp table to original name
            con.execute(f"ALTER TABLE {temp_table} RENAME TO {table_name}")
    
    def stage_chunked(self, file_path: Path,
                     config: DatasetConfig) -> Path:
        """
        Stage large file in chunks to avoid memory issues.
        
        Args:
            file_path: Path to large file
            config: Dataset configuration
            
        Returns:
            Path to staged Parquet file
        """
        staging_path = self.staging_dir / f"{config.name}.parquet"
        
        logger.info("stager.chunked.starting",
                   file=str(file_path),
                   chunk_size=self.chunk_size)
        
        # Process in chunks
        chunks_processed = 0
        
        if file_path.suffix.lower() == '.csv':
            # Use pandas chunked reader for CSV
            for chunk in pd.read_csv(file_path, chunksize=self.chunk_size):
                if chunks_processed == 0:
                    # First chunk - create file
                    chunk.to_parquet(staging_path, index=False)
                else:
                    # Append to existing
                    chunk.to_parquet(staging_path, index=False,
                                   mode='append')
                
                chunks_processed += 1
                
                if chunks_processed % 10 == 0:
                    logger.debug("stager.chunked.progress",
                               chunks=chunks_processed,
                               rows=chunks_processed * self.chunk_size)
        else:
            # For non-CSV, read entire file (for now)
            df = self.file_reader.read(file_path)
            df.to_parquet(staging_path, index=False)
            chunks_processed = 1
        
        logger.info("stager.chunked.complete",
                   chunks=chunks_processed,
                   path=str(staging_path))
        
        return staging_path