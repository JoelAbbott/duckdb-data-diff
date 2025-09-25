"""
Performance metrics collection.
Single responsibility: track and report performance metrics.
"""

import time
import psutil
import os
from dataclasses import dataclass, field
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from pathlib import Path

from .logger import get_logger


logger = get_logger()


@dataclass
class OperationMetrics:
    """Metrics for a single operation."""
    
    name: str
    start_time: float
    end_time: Optional[float] = None
    duration_seconds: Optional[float] = None
    rows_processed: int = 0
    memory_mb_start: float = 0
    memory_mb_peak: float = 0
    memory_mb_end: float = 0
    success: bool = True
    error: Optional[str] = None


@dataclass
class PipelineMetrics:
    """Overall pipeline metrics."""
    
    start_time: datetime = field(default_factory=datetime.now)
    end_time: Optional[datetime] = None
    total_duration_seconds: float = 0
    operations: List[OperationMetrics] = field(default_factory=list)
    memory_mb_peak: float = 0
    total_rows_processed: int = 0
    datasets_processed: int = 0
    comparisons_completed: int = 0
    errors_encountered: int = 0


class MetricsCollector:
    """
    Collect and track performance metrics.
    """
    
    def __init__(self):
        """Initialize metrics collector."""
        self.pipeline_metrics = PipelineMetrics()
        self.current_operations: Dict[str, OperationMetrics] = {}
        self.process = psutil.Process(os.getpid())
        
    def start_operation(self, name: str) -> None:
        """
        Start tracking an operation.
        
        Args:
            name: Operation name
        """
        memory_mb = self._get_memory_usage()
        
        operation = OperationMetrics(
            name=name,
            start_time=time.time(),
            memory_mb_start=memory_mb
        )
        
        self.current_operations[name] = operation
        
        logger.debug("metrics.operation.start",
                    operation=name,
                    memory_mb=round(memory_mb, 2))
    
    def end_operation(self, name: str, rows_processed: int = 0,
                     success: bool = True, error: Optional[str] = None) -> None:
        """
        End tracking an operation.
        
        Args:
            name: Operation name
            rows_processed: Number of rows processed
            success: Whether operation succeeded
            error: Error message if failed
        """
        if name not in self.current_operations:
            logger.warning("metrics.operation.not_found", operation=name)
            return
        
        operation = self.current_operations[name]
        operation.end_time = time.time()
        operation.duration_seconds = operation.end_time - operation.start_time
        operation.rows_processed = rows_processed
        operation.memory_mb_end = self._get_memory_usage()
        operation.success = success
        operation.error = error
        
        # Update peak memory
        operation.memory_mb_peak = max(
            operation.memory_mb_start,
            operation.memory_mb_end,
            self.pipeline_metrics.memory_mb_peak
        )
        
        # Add to completed operations
        self.pipeline_metrics.operations.append(operation)
        
        # Update pipeline metrics
        self.pipeline_metrics.total_rows_processed += rows_processed
        if not success:
            self.pipeline_metrics.errors_encountered += 1
        
        # Update peak memory
        self.pipeline_metrics.memory_mb_peak = max(
            self.pipeline_metrics.memory_mb_peak,
            operation.memory_mb_peak
        )
        
        # Remove from current
        del self.current_operations[name]
        
        logger.info("metrics.operation.end",
                   operation=name,
                   duration=round(operation.duration_seconds, 2),
                   rows=rows_processed,
                   memory_mb=round(operation.memory_mb_end, 2),
                   success=success)
    
    def record_dataset(self, dataset_name: str, row_count: int) -> None:
        """
        Record dataset processing.
        
        Args:
            dataset_name: Dataset name
            row_count: Number of rows
        """
        self.pipeline_metrics.datasets_processed += 1
        
        logger.debug("metrics.dataset.recorded",
                    dataset=dataset_name,
                    rows=row_count)
    
    def record_comparison(self, left: str, right: str,
                         matched: int, differences: int) -> None:
        """
        Record comparison completion.
        
        Args:
            left: Left dataset name
            right: Right dataset name
            matched: Number of matched rows
            differences: Number of differences
        """
        self.pipeline_metrics.comparisons_completed += 1
        
        logger.debug("metrics.comparison.recorded",
                    left=left,
                    right=right,
                    matched=matched,
                    differences=differences)
    
    def finalize(self) -> PipelineMetrics:
        """
        Finalize metrics collection.
        
        Returns:
            Final pipeline metrics
        """
        self.pipeline_metrics.end_time = datetime.now()
        self.pipeline_metrics.total_duration_seconds = (
            self.pipeline_metrics.end_time - self.pipeline_metrics.start_time
        ).total_seconds()
        
        logger.info("metrics.pipeline.finalized",
                   duration=round(self.pipeline_metrics.total_duration_seconds, 2),
                   datasets=self.pipeline_metrics.datasets_processed,
                   comparisons=self.pipeline_metrics.comparisons_completed,
                   rows=self.pipeline_metrics.total_rows_processed,
                   memory_mb_peak=round(self.pipeline_metrics.memory_mb_peak, 2),
                   errors=self.pipeline_metrics.errors_encountered)
        
        return self.pipeline_metrics
    
    def generate_report(self) -> Dict[str, Any]:
        """
        Generate metrics report.
        
        Returns:
            Metrics report dictionary
        """
        metrics = self.finalize()
        
        # Calculate rates
        if metrics.total_duration_seconds > 0:
            rows_per_second = (
                metrics.total_rows_processed / metrics.total_duration_seconds
            )
        else:
            rows_per_second = 0
        
        # Find slowest operations
        slowest_ops = sorted(
            metrics.operations,
            key=lambda x: x.duration_seconds or 0,
            reverse=True
        )[:5]
        
        # Find most memory intensive operations
        memory_ops = sorted(
            metrics.operations,
            key=lambda x: x.memory_mb_peak,
            reverse=True
        )[:5]
        
        report = {
            "summary": {
                "total_duration_seconds": round(metrics.total_duration_seconds, 2),
                "total_duration_formatted": self._format_duration(
                    metrics.total_duration_seconds
                ),
                "datasets_processed": metrics.datasets_processed,
                "comparisons_completed": metrics.comparisons_completed,
                "total_rows_processed": metrics.total_rows_processed,
                "rows_per_second": round(rows_per_second, 0),
                "memory_mb_peak": round(metrics.memory_mb_peak, 2),
                "errors_encountered": metrics.errors_encountered
            },
            "slowest_operations": [
                {
                    "name": op.name,
                    "duration_seconds": round(op.duration_seconds or 0, 2),
                    "rows": op.rows_processed
                }
                for op in slowest_ops
            ],
            "memory_intensive_operations": [
                {
                    "name": op.name,
                    "memory_mb_peak": round(op.memory_mb_peak, 2),
                    "memory_mb_growth": round(
                        op.memory_mb_end - op.memory_mb_start, 2
                    )
                }
                for op in memory_ops
            ],
            "performance_assessment": self._assess_performance(metrics)
        }
        
        return report
    
    def _get_memory_usage(self) -> float:
        """
        Get current memory usage in MB.
        
        Returns:
            Memory usage in MB
        """
        return self.process.memory_info().rss / (1024 * 1024)
    
    def _format_duration(self, seconds: float) -> str:
        """
        Format duration in human-readable format.
        
        Args:
            seconds: Duration in seconds
            
        Returns:
            Formatted duration string
        """
        if seconds < 60:
            return f"{seconds:.1f} seconds"
        elif seconds < 3600:
            minutes = seconds / 60
            return f"{minutes:.1f} minutes"
        else:
            hours = seconds / 3600
            return f"{hours:.1f} hours"
    
    def _assess_performance(self, metrics: PipelineMetrics) -> Dict[str, Any]:
        """
        Assess pipeline performance against targets.
        
        Args:
            metrics: Pipeline metrics
            
        Returns:
            Performance assessment
        """
        assessment = {
            "meets_targets": True,
            "issues": []
        }
        
        # Target: 300k rows in <5 minutes (300 seconds)
        if metrics.total_rows_processed > 300_000:
            expected_time = (metrics.total_rows_processed / 300_000) * 300
            if metrics.total_duration_seconds > expected_time:
                assessment["meets_targets"] = False
                assessment["issues"].append(
                    f"Processing speed below target: "
                    f"{metrics.total_duration_seconds:.1f}s for "
                    f"{metrics.total_rows_processed:,} rows "
                    f"(expected <{expected_time:.1f}s)"
                )
        
        # Target: <2GB memory
        if metrics.memory_mb_peak > 2048:
            assessment["meets_targets"] = False
            assessment["issues"].append(
                f"Memory usage exceeded 2GB: {metrics.memory_mb_peak:.1f}MB"
            )
        
        # Check for errors
        if metrics.errors_encountered > 0:
            assessment["meets_targets"] = False
            assessment["issues"].append(
                f"Errors encountered: {metrics.errors_encountered}"
            )
        
        if assessment["meets_targets"]:
            assessment["summary"] = "✓ All performance targets met"
        else:
            assessment["summary"] = f"✗ {len(assessment['issues'])} issues found"
        
        return assessment
    
    def save_report(self, output_path: Optional[Path] = None) -> Path:
        """
        Save metrics report to file.
        
        Args:
            output_path: Output path (auto-generated if None)
            
        Returns:
            Path to saved report
        """
        import json
        
        if not output_path:
            output_path = Path("data/reports") / (
                f"metrics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            )
        
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        report = self.generate_report()
        
        with open(output_path, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        logger.info("metrics.report.saved", path=str(output_path))
        
        return output_path