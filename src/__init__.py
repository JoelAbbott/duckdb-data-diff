"""
DuckDB Data Diff - Production-ready data comparison pipeline.
"""

__version__ = "2.0.0"

from .core.comparator import DataComparator, ComparisonResult
from .config.manager import ConfigManager, DatasetConfig, ComparisonConfig
from .pipeline.stager import DataStager
from .pipeline.validators import ValidationPipeline, ValidationReport
from .adapters.file_reader import UniversalFileReader
from .ui.progress import ProgressMonitor, get_progress_monitor
from .utils.logger import get_logger

__all__ = [
    "DataComparator",
    "ComparisonResult",
    "ConfigManager", 
    "DatasetConfig",
    "ComparisonConfig",
    "DataStager",
    "ValidationPipeline",
    "ValidationReport",
    "UniversalFileReader",
    "ProgressMonitor",
    "get_progress_monitor",
    "get_logger",
]