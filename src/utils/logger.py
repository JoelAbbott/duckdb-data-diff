"""
Structured logging utility.
Single responsibility: provide consistent logging across application.
"""

import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional
import json


class StructuredLogger:
    """
    Structured logger for consistent application logging.
    """
    
    def __init__(self, name: str = "duckdb-data-diff", 
                 log_file: Optional[Path] = None):
        """
        Initialize logger.
        
        Args:
            name: Logger name
            log_file: Optional file path for logging
        """
        self.name = name
        self.log_file = log_file
        
    def _format_message(self, level: str, message: str, 
                       **kwargs) -> Dict[str, Any]:
        """
        Format log message with metadata.
        
        Args:
            level: Log level (INFO, DEBUG, ERROR, etc.)
            message: Log message
            **kwargs: Additional context fields
            
        Returns:
            Formatted log entry
        """
        entry = {
            "timestamp": datetime.now().isoformat(),
            "level": level,
            "logger": self.name,
            "message": message
        }
        
        if kwargs:
            entry["context"] = kwargs
            
        return entry
    
    def _output(self, entry: Dict[str, Any]):
        """
        Output log entry to console and optionally file.
        
        Args:
            entry: Log entry dictionary
        """
        # Console output - human readable
        timestamp = entry["timestamp"].split("T")[1][:8]
        level = entry["level"]
        msg = entry["message"]
        
        print(f"[{timestamp}] {level:5} | {msg}", file=sys.stderr)
        
        # Show context if present
        if "context" in entry:
            for key, value in entry["context"].items():
                print(f"  {key}={value}", file=sys.stderr)
        
        # File output - JSON for parsing
        if self.log_file:
            with open(self.log_file, "a") as f:
                f.write(json.dumps(entry) + "\n")
    
    def info(self, message: str, **kwargs):
        """Log info message."""
        entry = self._format_message("INFO", message, **kwargs)
        self._output(entry)
    
    def debug(self, message: str, **kwargs):
        """Log debug message."""
        entry = self._format_message("DEBUG", message, **kwargs)
        self._output(entry)
    
    def warning(self, message: str, **kwargs):
        """Log warning message."""
        entry = self._format_message("WARN", message, **kwargs)
        self._output(entry)
    
    def error(self, message: str, **kwargs):
        """Log error message."""
        entry = self._format_message("ERROR", message, **kwargs)
        self._output(entry)
    
    def critical(self, message: str, **kwargs):
        """Log critical message."""
        entry = self._format_message("CRITICAL", message, **kwargs)
        self._output(entry)


# Global logger instance
_logger: Optional[StructuredLogger] = None


def get_logger(name: str = "duckdb-data-diff") -> StructuredLogger:
    """
    Get or create logger instance.
    
    Args:
        name: Logger name
        
    Returns:
        Logger instance
    """
    global _logger
    if _logger is None:
        _logger = StructuredLogger(name)
    return _logger