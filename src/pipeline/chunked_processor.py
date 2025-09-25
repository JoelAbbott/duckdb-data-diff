"""
Chunked data processing for memory efficiency.
Single responsibility: process large datasets without loading all into memory.
"""

from pathlib import Path
from typing import Iterator, Optional, Dict, Any, Callable, Generator
import pandas as pd
import duckdb
import pyarrow.parquet as pq
import pyarrow as pa

from ..utils.logger import get_logger
from ..config.manager import DatasetConfig


logger = get_logger()


class ChunkedProcessor:
    """
    Process large files in chunks to maintain memory efficiency.
    Target: <2GB memory usage for any file size.
    """
    
    # Optimal chunk sizes for different file types
    CHUNK_SIZES = {
        'csv': 50_000,
        'excel': 10_000,
        'parquet': 100_000,
        'default': 25_000
    }
    
    def __init__(self, chunk_size: Optional[int] = None,
                 max_memory_mb: int = 1500):
        """
        Initialize chunked processor.
        
        Args:
            chunk_size: Rows per chunk (auto-determined if None)
            max_memory_mb: Maximum memory usage in MB
        """
        self.chunk_size = chunk_size
        self.max_memory_mb = max_memory_mb
        self.current_memory_usage = 0
        
    def determine_chunk_size(self, file_path: Path,
                            estimated_columns: int = 50) -> int:
        """
        Determine optimal chunk size based on file.
        
        Args:
            file_path: Path to file
            estimated_columns: Estimated number of columns
            
        Returns:
            Optimal chunk size
        """
        if self.chunk_size:
            return self.chunk_size
        
        file_type = file_path.suffix.lower().lstrip('.')
        base_chunk_size = self.CHUNK_SIZES.get(
            file_type, 
            self.CHUNK_SIZES['default']
        )
        
        # Adjust based on file size
        file_size_mb = file_path.stat().st_size / (1024 * 1024)
        
        if file_size_mb > 1000:  # >1GB file
            chunk_size = base_chunk_size // 2
        elif file_size_mb > 5000:  # >5GB file
            chunk_size = base_chunk_size // 4
        else:
            chunk_size = base_chunk_size
        
        # Adjust based on estimated columns
        if estimated_columns > 100:
            chunk_size = chunk_size // 2
        elif estimated_columns > 200:
            chunk_size = chunk_size // 4
        
        logger.info("chunked_processor.chunk_size.determined",
                   file=str(file_path),
                   size_mb=round(file_size_mb, 2),
                   columns=estimated_columns,
                   chunk_size=chunk_size)
        
        return max(1000, chunk_size)  # Minimum 1000 rows
    
    def read_csv_chunked(self, file_path: Path,
                        chunk_size: Optional[int] = None,
                        **kwargs) -> Iterator[pd.DataFrame]:
        """
        Read CSV file in chunks.
        
        Args:
            file_path: Path to CSV file
            chunk_size: Override chunk size
            **kwargs: Additional pandas read_csv arguments
            
        Yields:
            DataFrame chunks
        """
        chunk_size = chunk_size or self.determine_chunk_size(file_path)
        
        logger.info("chunked_processor.csv.start",
                   file=str(file_path),
                   chunk_size=chunk_size)
        
        chunks_processed = 0
        total_rows = 0
        
        for chunk in pd.read_csv(file_path, chunksize=chunk_size, **kwargs):
            chunks_processed += 1
            total_rows += len(chunk)
            
            if chunks_processed % 10 == 0:
                logger.debug("chunked_processor.csv.progress",
                           chunks=chunks_processed,
                           rows=total_rows)
            
            yield chunk
        
        logger.info("chunked_processor.csv.complete",
                   chunks=chunks_processed,
                   rows=total_rows)
    
    def read_excel_chunked(self, file_path: Path,
                          sheet_name: Any = 0,
                          chunk_size: Optional[int] = None) -> Iterator[pd.DataFrame]:
        """
        Read Excel file in chunks.
        
        Args:
            file_path: Path to Excel file
            sheet_name: Sheet to read
            chunk_size: Override chunk size
            
        Yields:
            DataFrame chunks
        """
        chunk_size = chunk_size or self.determine_chunk_size(file_path, 30)
        
        logger.info("chunked_processor.excel.start",
                   file=str(file_path),
                   sheet=sheet_name,
                   chunk_size=chunk_size)
        
        # Read all at once (Excel doesn't support true chunking)
        # But we'll yield it in chunks for consistent interface
        df = pd.read_excel(file_path, sheet_name=sheet_name)
        
        total_rows = len(df)
        chunks_yielded = 0
        
        for start_idx in range(0, total_rows, chunk_size):
            end_idx = min(start_idx + chunk_size, total_rows)
            chunk = df.iloc[start_idx:end_idx]
            chunks_yielded += 1
            
            yield chunk
        
        logger.info("chunked_processor.excel.complete",
                   chunks=chunks_yielded,
                   rows=total_rows)
    
    def read_parquet_chunked(self, file_path: Path,
                            chunk_size: Optional[int] = None) -> Iterator[pd.DataFrame]:
        """
        Read Parquet file in chunks.
        
        Args:
            file_path: Path to Parquet file
            chunk_size: Override chunk size
            
        Yields:
            DataFrame chunks
        """
        chunk_size = chunk_size or self.determine_chunk_size(file_path)
        
        logger.info("chunked_processor.parquet.start",
                   file=str(file_path),
                   chunk_size=chunk_size)
        
        parquet_file = pq.ParquetFile(file_path)
        
        chunks_yielded = 0
        total_rows = 0
        
        for batch in parquet_file.iter_batches(batch_size=chunk_size):
            df = batch.to_pandas()
            chunks_yielded += 1
            total_rows += len(df)
            
            yield df
        
        logger.info("chunked_processor.parquet.complete",
                   chunks=chunks_yielded,
                   rows=total_rows)
    
    def process_file_chunked(self, file_path: Path,
                           processor_func: Callable[[pd.DataFrame], pd.DataFrame],
                           output_path: Optional[Path] = None) -> Path:
        """
        Process file in chunks with a processing function.
        
        Args:
            file_path: Input file path
            processor_func: Function to process each chunk
            output_path: Output file path (auto-generated if None)
            
        Returns:
            Path to processed output file
        """
        file_path = Path(file_path)
        
        if not output_path:
            output_path = file_path.parent / f"{file_path.stem}_processed.parquet"
        
        logger.info("chunked_processor.process.start",
                   input=str(file_path),
                   output=str(output_path))
        
        # Determine file type and get appropriate reader
        suffix = file_path.suffix.lower()
        
        if suffix == '.csv':
            reader = self.read_csv_chunked(file_path)
        elif suffix in ['.xlsx', '.xls']:
            reader = self.read_excel_chunked(file_path)
        elif suffix == '.parquet':
            reader = self.read_parquet_chunked(file_path)
        else:
            raise ValueError(f"Unsupported file type: {suffix}")
        
        # Process chunks and write
        chunks_processed = 0
        writer = None
        
        for chunk in reader:
            # Process chunk
            processed_chunk = processor_func(chunk)
            
            # Write to parquet
            if chunks_processed == 0:
                # First chunk - create file
                processed_chunk.to_parquet(output_path, index=False)
            else:
                # Append to existing
                # Convert to PyArrow table for appending
                table = pa.Table.from_pandas(processed_chunk, preserve_index=False)
                
                # Read existing file
                existing_table = pq.read_table(output_path)
                
                # Concatenate tables
                combined_table = pa.concat_tables([existing_table, table])
                
                # Write back
                pq.write_table(combined_table, output_path)
            
            chunks_processed += 1
            
            if chunks_processed % 10 == 0:
                logger.debug("chunked_processor.process.progress",
                           chunks=chunks_processed)
        
        logger.info("chunked_processor.process.complete",
                   chunks=chunks_processed,
                   output=str(output_path))
        
        return output_path
    
    def stage_to_duckdb_chunked(self, con: duckdb.DuckDBPyConnection,
                               file_path: Path,
                               table_name: str,
                               config: Optional[DatasetConfig] = None) -> str:
        """
        Stage large file to DuckDB in chunks.
        
        Args:
            con: DuckDB connection
            file_path: File to stage
            table_name: Target table name
            config: Optional dataset configuration
            
        Returns:
            Table name in DuckDB
        """
        file_path = Path(file_path)
        suffix = file_path.suffix.lower()
        
        logger.info("chunked_processor.duckdb.start",
                   file=str(file_path),
                   table=table_name)
        
        # For CSV and Parquet, DuckDB can handle directly
        if suffix == '.csv':
            # Use DuckDB's native CSV reader (very efficient)
            con.execute(f"""
                CREATE OR REPLACE TABLE {table_name} AS
                SELECT * FROM read_csv_auto(
                    '{file_path}',
                    sample_size=100000
                )
            """)
            
        elif suffix == '.parquet':
            # Use DuckDB's native Parquet reader
            con.execute(f"""
                CREATE OR REPLACE TABLE {table_name} AS
                SELECT * FROM '{file_path}'
            """)
            
        elif suffix in ['.xlsx', '.xls']:
            # Excel needs chunked reading through pandas
            first_chunk = True
            
            for chunk in self.read_excel_chunked(file_path):
                if first_chunk:
                    # Create table with first chunk
                    con.register('temp_chunk', chunk)
                    con.execute(f"""
                        CREATE OR REPLACE TABLE {table_name} AS
                        SELECT * FROM temp_chunk
                    """)
                    first_chunk = False
                else:
                    # Append subsequent chunks
                    con.register('temp_chunk', chunk)
                    con.execute(f"""
                        INSERT INTO {table_name}
                        SELECT * FROM temp_chunk
                    """)
                
                con.unregister('temp_chunk')
        
        else:
            raise ValueError(f"Unsupported file type: {suffix}")
        
        # Get final row count
        row_count = con.execute(
            f"SELECT COUNT(*) FROM {table_name}"
        ).fetchone()[0]
        
        logger.info("chunked_processor.duckdb.complete",
                   table=table_name,
                   rows=row_count)
        
        return table_name
    
    def compare_chunked(self, con: duckdb.DuckDBPyConnection,
                       left_table: str, right_table: str,
                       key_columns: list,
                       chunk_size: int = 100_000) -> Dict[str, int]:
        """
        Compare large tables in chunks.
        
        Args:
            con: DuckDB connection
            left_table: Left table name
            right_table: Right table name
            key_columns: Key columns for comparison
            chunk_size: Rows per chunk
            
        Returns:
            Comparison statistics
        """
        logger.info("chunked_processor.compare.start",
                   left=left_table,
                   right=right_table,
                   chunk_size=chunk_size)
        
        stats = {
            'total_left': 0,
            'total_right': 0,
            'matched': 0,
            'only_left': 0,
            'only_right': 0,
            'chunks_processed': 0
        }
        
        # Get total counts
        stats['total_left'] = con.execute(
            f"SELECT COUNT(*) FROM {left_table}"
        ).fetchone()[0]
        
        stats['total_right'] = con.execute(
            f"SELECT COUNT(*) FROM {right_table}"
        ).fetchone()[0]
        
        # Process in chunks using OFFSET/LIMIT
        offset = 0
        
        while offset < stats['total_left']:
            # Get chunk of left table
            key_list = ', '.join(key_columns)
            key_join = ' AND '.join([
                f"l.{col} = r.{col}" for col in key_columns
            ])
            
            # Count matches in this chunk
            matches = con.execute(f"""
                SELECT COUNT(*)
                FROM (
                    SELECT {key_list}
                    FROM {left_table}
                    LIMIT {chunk_size} OFFSET {offset}
                ) l
                INNER JOIN {right_table} r ON {key_join}
            """).fetchone()[0]
            
            stats['matched'] += matches
            stats['chunks_processed'] += 1
            
            offset += chunk_size
            
            if stats['chunks_processed'] % 10 == 0:
                logger.debug("chunked_processor.compare.progress",
                           chunks=stats['chunks_processed'],
                           offset=offset)
        
        # Calculate only_left and only_right
        stats['only_left'] = stats['total_left'] - stats['matched']
        stats['only_right'] = stats['total_right'] - stats['matched']
        
        logger.info("chunked_processor.compare.complete",
                   chunks=stats['chunks_processed'],
                   matched=stats['matched'])
        
        return stats