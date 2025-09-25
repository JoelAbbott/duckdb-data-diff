"""
Data lineage tracking.
Single responsibility: track data flow and transformations.
"""

from dataclasses import dataclass, field, asdict
from typing import Dict, List, Any, Optional
from datetime import datetime
from pathlib import Path
import json
import hashlib

from ..utils.logger import get_logger


logger = get_logger()


@dataclass
class DatasetLineage:
    """Lineage information for a single dataset."""
    
    dataset_name: str
    source_file: str
    source_type: str
    source_size_bytes: int
    source_modified: datetime
    source_hash: str
    row_count_original: int
    row_count_final: int
    column_count_original: int
    column_count_final: int
    transformations: List[Dict[str, Any]] = field(default_factory=list)
    staging_path: Optional[str] = None
    processing_time_seconds: float = 0
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class ComparisonLineage:
    """Lineage information for a comparison."""
    
    comparison_id: str
    left_dataset: str
    right_dataset: str
    key_columns: List[str]
    value_columns: List[str]
    matched_rows: int
    only_in_left: int
    only_in_right: int
    value_differences: int
    output_files: List[str] = field(default_factory=list)
    processing_time_seconds: float = 0
    timestamp: datetime = field(default_factory=datetime.now)


class DataLineageTracker:
    """
    Track data lineage throughout pipeline.
    """
    
    def __init__(self):
        """Initialize lineage tracker."""
        self.datasets: Dict[str, DatasetLineage] = {}
        self.comparisons: List[ComparisonLineage] = []
        self.pipeline_metadata: Dict[str, Any] = {
            "start_time": datetime.now(),
            "config_file": None,
            "pipeline_version": "2.0.0"
        }
    
    def track_dataset_source(self, dataset_name: str, 
                            source_path: Path) -> DatasetLineage:
        """
        Track source of a dataset.
        
        Args:
            dataset_name: Dataset identifier
            source_path: Path to source file
            
        Returns:
            Dataset lineage object
        """
        source_path = Path(source_path)
        
        # Get file metadata
        stat = source_path.stat()
        
        # Calculate file hash (first 1MB for performance)
        file_hash = self._calculate_file_hash(source_path)
        
        lineage = DatasetLineage(
            dataset_name=dataset_name,
            source_file=str(source_path),
            source_type=source_path.suffix.lower().lstrip('.'),
            source_size_bytes=stat.st_size,
            source_modified=datetime.fromtimestamp(stat.st_mtime),
            source_hash=file_hash,
            row_count_original=0,
            row_count_final=0,
            column_count_original=0,
            column_count_final=0
        )
        
        self.datasets[dataset_name] = lineage
        
        logger.info("lineage.dataset.source_tracked",
                   dataset=dataset_name,
                   source=str(source_path),
                   size_mb=round(stat.st_size / (1024*1024), 2))
        
        return lineage
    
    def track_transformation(self, dataset_name: str,
                           transformation_type: str,
                           details: Dict[str, Any]):
        """
        Track a transformation applied to dataset.
        
        Args:
            dataset_name: Dataset identifier
            transformation_type: Type of transformation
            details: Transformation details
        """
        if dataset_name not in self.datasets:
            logger.warning("lineage.transformation.dataset_not_found",
                         dataset=dataset_name)
            return
        
        transformation = {
            "type": transformation_type,
            "timestamp": datetime.now().isoformat(),
            "details": details
        }
        
        self.datasets[dataset_name].transformations.append(transformation)
        
        logger.debug("lineage.transformation.tracked",
                    dataset=dataset_name,
                    type=transformation_type)
    
    def update_dataset_stats(self, dataset_name: str,
                           original_rows: Optional[int] = None,
                           final_rows: Optional[int] = None,
                           original_cols: Optional[int] = None,
                           final_cols: Optional[int] = None,
                           staging_path: Optional[str] = None,
                           processing_time: Optional[float] = None):
        """
        Update dataset statistics.
        
        Args:
            dataset_name: Dataset identifier
            original_rows: Original row count
            final_rows: Final row count
            original_cols: Original column count
            final_cols: Final column count
            staging_path: Path to staged data
            processing_time: Processing time in seconds
        """
        if dataset_name not in self.datasets:
            logger.warning("lineage.stats.dataset_not_found",
                         dataset=dataset_name)
            return
        
        lineage = self.datasets[dataset_name]
        
        if original_rows is not None:
            lineage.row_count_original = original_rows
        if final_rows is not None:
            lineage.row_count_final = final_rows
        if original_cols is not None:
            lineage.column_count_original = original_cols
        if final_cols is not None:
            lineage.column_count_final = final_cols
        if staging_path is not None:
            lineage.staging_path = staging_path
        if processing_time is not None:
            lineage.processing_time_seconds = processing_time
        
        logger.debug("lineage.stats.updated",
                    dataset=dataset_name,
                    original_rows=original_rows,
                    final_rows=final_rows)
    
    def track_comparison(self, left: str, right: str,
                       comparison_config: Dict[str, Any],
                       results: Dict[str, Any],
                       output_files: List[str],
                       processing_time: float) -> ComparisonLineage:
        """
        Track comparison lineage.
        
        Args:
            left: Left dataset name
            right: Right dataset name
            comparison_config: Comparison configuration
            results: Comparison results
            output_files: Generated output files
            processing_time: Processing time in seconds
            
        Returns:
            Comparison lineage object
        """
        comparison_id = f"{left}_vs_{right}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        lineage = ComparisonLineage(
            comparison_id=comparison_id,
            left_dataset=left,
            right_dataset=right,
            key_columns=comparison_config.get("key_columns", []),
            value_columns=comparison_config.get("value_columns", []),
            matched_rows=results.get("matched_rows", 0),
            only_in_left=results.get("only_in_left", 0),
            only_in_right=results.get("only_in_right", 0),
            value_differences=results.get("value_differences", 0),
            output_files=output_files,
            processing_time_seconds=processing_time
        )
        
        self.comparisons.append(lineage)
        
        logger.info("lineage.comparison.tracked",
                   comparison_id=comparison_id,
                   matched=lineage.matched_rows,
                   differences=lineage.value_differences)
        
        return lineage
    
    def generate_lineage_report(self) -> Dict[str, Any]:
        """
        Generate complete lineage report.
        
        Returns:
            Lineage report dictionary
        """
        self.pipeline_metadata["end_time"] = datetime.now()
        self.pipeline_metadata["total_duration_seconds"] = (
            self.pipeline_metadata["end_time"] - 
            self.pipeline_metadata["start_time"]
        ).total_seconds()
        
        # Calculate aggregates
        total_source_bytes = sum(
            d.source_size_bytes for d in self.datasets.values()
        )
        total_rows_processed = sum(
            d.row_count_final for d in self.datasets.values()
        )
        total_transformations = sum(
            len(d.transformations) for d in self.datasets.values()
        )
        
        report = {
            "pipeline_metadata": {
                **self.pipeline_metadata,
                "start_time": self.pipeline_metadata["start_time"].isoformat(),
                "end_time": self.pipeline_metadata["end_time"].isoformat()
            },
            "summary": {
                "datasets_processed": len(self.datasets),
                "comparisons_performed": len(self.comparisons),
                "total_source_size_mb": round(total_source_bytes / (1024*1024), 2),
                "total_rows_processed": total_rows_processed,
                "total_transformations": total_transformations
            },
            "datasets": {},
            "comparisons": [],
            "data_flow": self._generate_data_flow()
        }
        
        # Add dataset details
        for name, lineage in self.datasets.items():
            lineage_dict = asdict(lineage)
            # Convert datetimes to strings
            lineage_dict["source_modified"] = lineage.source_modified.isoformat()
            lineage_dict["timestamp"] = lineage.timestamp.isoformat()
            report["datasets"][name] = lineage_dict
        
        # Add comparison details
        for comparison in self.comparisons:
            comp_dict = asdict(comparison)
            comp_dict["timestamp"] = comparison.timestamp.isoformat()
            report["comparisons"].append(comp_dict)
        
        return report
    
    def _generate_data_flow(self) -> Dict[str, Any]:
        """
        Generate data flow visualization data.
        
        Returns:
            Data flow information
        """
        flow = {
            "nodes": [],
            "edges": []
        }
        
        # Add dataset nodes
        for name, lineage in self.datasets.items():
            flow["nodes"].append({
                "id": name,
                "type": "dataset",
                "source": lineage.source_file,
                "rows": lineage.row_count_final
            })
        
        # Add comparison edges
        for comparison in self.comparisons:
            flow["edges"].append({
                "from": comparison.left_dataset,
                "to": comparison.comparison_id,
                "type": "comparison"
            })
            flow["edges"].append({
                "from": comparison.right_dataset,
                "to": comparison.comparison_id,
                "type": "comparison"
            })
            
            # Add comparison node
            flow["nodes"].append({
                "id": comparison.comparison_id,
                "type": "comparison",
                "matched": comparison.matched_rows,
                "differences": comparison.value_differences
            })
        
        return flow
    
    def save_lineage_report(self, output_path: Optional[Path] = None) -> Path:
        """
        Save lineage report to file.
        
        Args:
            output_path: Output path (auto-generated if None)
            
        Returns:
            Path to saved report
        """
        if not output_path:
            output_path = Path("data/reports") / (
                f"lineage_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            )
        
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        report = self.generate_lineage_report()
        
        with open(output_path, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        logger.info("lineage.report.saved", path=str(output_path))
        
        return output_path
    
    def _calculate_file_hash(self, file_path: Path, 
                            max_bytes: int = 1024*1024) -> str:
        """
        Calculate hash of file (first N bytes for performance).
        
        Args:
            file_path: Path to file
            max_bytes: Maximum bytes to hash
            
        Returns:
            SHA256 hash string
        """
        sha256_hash = hashlib.sha256()
        
        with open(file_path, "rb") as f:
            # Read first chunk
            data = f.read(max_bytes)
            sha256_hash.update(data)
        
        return sha256_hash.hexdigest()[:16]