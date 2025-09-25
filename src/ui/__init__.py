"""User interface and progress monitoring."""

from .progress import (
    ProgressMonitor,
    SpinnerProgress,
    get_progress_monitor
)
from .menu import MenuInterface

__all__ = [
    "ProgressMonitor",
    "SpinnerProgress", 
    "get_progress_monitor",
    "MenuInterface",
]

# Export Rich monitor if available
try:
    from .progress import RichProgressMonitor, RICH_AVAILABLE
    if RICH_AVAILABLE:
        __all__.append("RichProgressMonitor")
except ImportError:
    pass