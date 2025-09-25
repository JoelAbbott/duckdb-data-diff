"""
Universal file reader with caching.
Single responsibility: read any file type efficiently.
"""

import hashlib
import json
from pathlib import Path
from typing import Optional, Dict, Any, Tuple
import pandas as pd
import duckdb

from ..utils.logger import get_logger


logger = get_logger()


class UniversalFileReader:
    """
    Handles reading of various file formats with caching.
    """
    
    def __init__(self, cache_dir: Optional[Path] = None):
        """
        Initialize file reader.
        
        Args:
            cache_dir: Directory for caching converted files
        """
        if cache_dir:
            self.cache_dir = Path(cache_dir)
        else:
            self.cache_dir = Path("data/cached")
        
        self.cache_dir.mkdir(parents=True, exist_ok=True)
    
    def _get_file_hash(self, file_path: Path) -> str:
        """
        Calculate file hash for cache key.
        
        Args:
            file_path: Path to file
            
        Returns:
            SHA256 hash of file
        """
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()[:16]
    
    def _get_cache_path(self, file_path: Path) -> Path:
        """
        Get cache file path for given input file.
        
        Args:
            file_path: Original file path
            
        Returns:
            Path to cached CSV file
        """
        file_hash = self._get_file_hash(file_path)
        cache_name = f"{file_path.stem}_{file_hash}.csv"
        return self.cache_dir / cache_name
    
    def _is_cache_valid(self, file_path: Path, cache_path: Path) -> bool:
        """
        Check if cache is still valid.
        
        Args:
            file_path: Original file path
            cache_path: Cached file path
            
        Returns:
            True if cache is valid
        """
        if not cache_path.exists():
            return False
        
        # Check if original file is newer than cache
        orig_mtime = file_path.stat().st_mtime
        cache_mtime = cache_path.stat().st_mtime
        
        return cache_mtime > orig_mtime
    
    def read_excel(self, file_path: Path, sheet_name=0) -> pd.DataFrame:
        """
        Read Excel file.
        
        Args:
            file_path: Path to Excel file
            sheet_name: Sheet to read
            
        Returns:
            DataFrame
        """
        logger.info("file_reader.excel.reading", 
                   file=str(file_path),
                   sheet=sheet_name)
        
        df = pd.read_excel(file_path, sheet_name=sheet_name)
        
        logger.info("file_reader.excel.loaded",
                   rows=len(df),
                   columns=len(df.columns))
        
        return df
    
    def read_csv(self, file_path: Path) -> pd.DataFrame:
        """
        Read CSV file with automatic encoding detection.
        
        Args:
            file_path: Path to CSV file
            
        Returns:
            DataFrame with properly decoded text
        """
        logger.info("file_reader.csv.reading", file=str(file_path))
        
        # Try different encodings in order of likelihood
        encodings = ['utf-8', 'utf-8-sig', 'latin-1', 'iso-8859-1', 'cp1252', 'windows-1252']
        
        df = None
        successful_encoding = None
        
        for encoding in encodings:
            try:
                df = pd.read_csv(file_path, encoding=encoding, on_bad_lines='skip')
                successful_encoding = encoding
                break
            except (UnicodeDecodeError, UnicodeError):
                continue
        
        if df is None:
            # Last resort: try with error handling
            logger.warning("file_reader.csv.encoding_fallback", file=str(file_path))
            df = pd.read_csv(file_path, encoding='utf-8', errors='replace', on_bad_lines='skip')
            successful_encoding = 'utf-8 (with replacements)'
        
        logger.info("file_reader.csv.loaded",
                   rows=len(df),
                   columns=len(df.columns),
                   encoding=successful_encoding)
        
        # Apply text normalization to handle any remaining encoding issues
        from ..utils.text_normalizer import normalize_dataframe_text
        df = normalize_dataframe_text(df)
        
        return df
    
    def read_parquet(self, file_path: Path) -> pd.DataFrame:
        """
        Read Parquet file.
        
        Args:
            file_path: Path to Parquet file
            
        Returns:
            DataFrame
        """
        logger.info("file_reader.parquet.reading", file=str(file_path))
        
        df = pd.read_parquet(file_path)
        
        logger.info("file_reader.parquet.loaded",
                   rows=len(df),
                   columns=len(df.columns))
        
        return df
    
    def read(self, file_path: Path, use_cache: bool = True) -> pd.DataFrame:
        """
        Read any supported file type with optional caching.
        
        Args:
            file_path: Path to file
            use_cache: Whether to use caching
            
        Returns:
            DataFrame
            
        Raises:
            ValueError: If file type is not supported
        """
        file_path = Path(file_path)
        
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        # Check cache first
        if use_cache and file_path.suffix != '.csv':
            cache_path = self._get_cache_path(file_path)
            if self._is_cache_valid(file_path, cache_path):
                logger.info("file_reader.cache.hit", 
                           file=str(file_path),
                           cache=str(cache_path))
                return pd.read_csv(cache_path)
        
        # Read based on file type
        suffix = file_path.suffix.lower()
        
        if suffix in ['.xlsx', '.xls']:
            df = self.read_excel(file_path)
        elif suffix == '.csv':
            df = self.read_csv(file_path)
        elif suffix == '.parquet':
            df = self.read_parquet(file_path)
        else:
            raise ValueError(f"Unsupported file type: {suffix}")
        
        # Cache as CSV if not already CSV
        if use_cache and suffix != '.csv':
            cache_path = self._get_cache_path(file_path)
            df.to_csv(cache_path, index=False)
            logger.info("file_reader.cache.saved", 
                       file=str(file_path),
                       cache=str(cache_path))
        
        return df
    
    def read_with_duckdb(self, con: duckdb.DuckDBPyConnection, 
                        file_path: Path) -> str:
        """
        Read file directly into DuckDB.
        
        Args:
            con: DuckDB connection
            file_path: Path to file
            
        Returns:
            Table name in DuckDB
        """
        file_path = Path(file_path)
        suffix = file_path.suffix.lower()
        table_name = file_path.stem
        
        logger.info("file_reader.duckdb.loading",
                   file=str(file_path),
                   table=table_name)
        
        if suffix in ['.xlsx', '.xls']:
            # Read via pandas first
            df = self.read(file_path)
            con.register(table_name, df)
        elif suffix == '.csv':
            # Use pandas for better encoding handling, then register
            df = self.read_csv(file_path)
            con.register(table_name, df)
        elif suffix == '.parquet':
            con.execute(f"""
                CREATE TABLE {table_name} AS
                SELECT * FROM '{file_path}'
            """)
        else:
            raise ValueError(f"Unsupported file type: {suffix}")
        
        # Get row count
        row_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        
        logger.info("file_reader.duckdb.loaded",
                   table=table_name,
                   rows=row_count)
        
        return table_name