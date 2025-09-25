"""
Progress monitoring and user interface.
Single responsibility: provide user feedback during operations.
"""

import sys
import time
from typing import Optional, Any
from contextlib import contextmanager


class ProgressMonitor:
    """
    Simple progress monitoring for console output.
    """
    
    def __init__(self, verbose: bool = True):
        """
        Initialize progress monitor.
        
        Args:
            verbose: Whether to show detailed progress
        """
        self.verbose = verbose
        self.current_task = None
        self.start_time = None
    
    def start_task(self, task_name: str, total: Optional[int] = None):
        """
        Start a new task.
        
        Args:
            task_name: Name of task
            total: Total items to process (optional)
        """
        self.current_task = task_name
        self.total = total
        self.current = 0
        self.start_time = time.time()
        
        if self.verbose:
            print(f"\n[START] {task_name}")
            if total:
                print(f"  Total items: {total:,}")
    
    def update(self, current: Optional[int] = None, 
              message: Optional[str] = None):
        """
        Update progress.
        
        Args:
            current: Current item number
            message: Optional status message
        """
        if current is not None:
            self.current = current
        else:
            self.current += 1
        
        if self.verbose:
            elapsed = time.time() - self.start_time
            
            if self.total:
                percent = (self.current / self.total) * 100
                remaining_items = self.total - self.current
                
                if self.current > 0:
                    rate = self.current / elapsed
                    eta = remaining_items / rate if rate > 0 else 0
                else:
                    eta = 0
                
                status = f"  [{percent:5.1f}%] {self.current:,}/{self.total:,}"
                
                if eta > 0:
                    status += f" - ETA: {self._format_time(eta)}"
            else:
                status = f"  Processing: {self.current:,} items"
            
            status += f" - Elapsed: {self._format_time(elapsed)}"
            
            if message:
                status += f" - {message}"
            
            # Use carriage return to update same line
            print(f"\r{status}", end="", flush=True)
    
    def complete_task(self, message: Optional[str] = None):
        """
        Mark current task as complete.
        
        Args:
            message: Optional completion message
        """
        if self.verbose and self.current_task:
            elapsed = time.time() - self.start_time
            print()  # New line after progress updates
            
            status = f"[DONE] {self.current_task}"
            status += f" - Time: {self._format_time(elapsed)}"
            
            if self.total:
                rate = self.total / elapsed if elapsed > 0 else 0
                status += f" - Rate: {rate:,.0f} items/sec"
            
            if message:
                status += f" - {message}"
            
            print(status)
        
        self.current_task = None
        self.start_time = None
    
    def error(self, message: str):
        """
        Show error message.
        
        Args:
            message: Error message
        """
        print(f"\n[ERROR] {message}", file=sys.stderr)
    
    def warning(self, message: str):
        """
        Show warning message.
        
        Args:
            message: Warning message
        """
        if self.verbose:
            print(f"\n[WARNING] {message}", file=sys.stderr)
    
    def info(self, message: str):
        """
        Show info message.
        
        Args:
            message: Info message
        """
        if self.verbose:
            print(f"[INFO] {message}")
    
    @contextmanager
    def task(self, task_name: str, total: Optional[int] = None):
        """
        Context manager for task progress.
        
        Args:
            task_name: Name of task
            total: Total items to process
            
        Example:
            with progress.task("Processing files", total=100) as task:
                for i in range(100):
                    # Do work
                    task.update()
        """
        self.start_task(task_name, total)
        try:
            yield self
        finally:
            self.complete_task()
    
    def _format_time(self, seconds: float) -> str:
        """
        Format time duration.
        
        Args:
            seconds: Time in seconds
            
        Returns:
            Formatted time string
        """
        if seconds < 60:
            return f"{seconds:.1f}s"
        elif seconds < 3600:
            minutes = seconds / 60
            return f"{minutes:.1f}m"
        else:
            hours = seconds / 3600
            return f"{hours:.1f}h"


class SpinnerProgress:
    """
    Simple spinner for indeterminate progress.
    """
    
    CHARS = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"
    
    def __init__(self, message: str = "Processing"):
        """
        Initialize spinner.
        
        Args:
            message: Message to display
        """
        self.message = message
        self.index = 0
        self.running = False
    
    def start(self):
        """Start spinner."""
        self.running = True
        self.index = 0
    
    def update(self):
        """Update spinner display."""
        if self.running:
            char = self.CHARS[self.index % len(self.CHARS)]
            print(f"\r{char} {self.message}", end="", flush=True)
            self.index += 1
    
    def stop(self, final_message: Optional[str] = None):
        """
        Stop spinner.
        
        Args:
            final_message: Final message to display
        """
        self.running = False
        if final_message:
            print(f"\r✓ {final_message}")
        else:
            print(f"\r✓ {self.message} - Complete")


# Try to import rich for better progress bars
try:
    from rich.progress import (
        Progress, SpinnerColumn, TextColumn,
        BarColumn, TaskProgressColumn, TimeRemainingColumn
    )
    from rich.console import Console
    
    RICH_AVAILABLE = True
    
    class RichProgressMonitor:
        """
        Rich progress monitor with beautiful progress bars.
        """
        
        def __init__(self):
            """Initialize Rich progress monitor."""
            self.console = Console()
            self.progress = None
            self.tasks = {}
        
        def start(self):
            """Start progress monitoring."""
            self.progress = Progress(
                SpinnerColumn(),
                TextColumn("[bold blue]{task.description}"),
                BarColumn(),
                TaskProgressColumn(),
                TimeRemainingColumn(),
                console=self.console
            )
            self.progress.start()
        
        def add_task(self, name: str, total: Optional[int] = None) -> Any:
            """
            Add a new task.
            
            Args:
                name: Task name
                total: Total steps
                
            Returns:
                Task ID
            """
            if self.progress:
                task_id = self.progress.add_task(name, total=total)
                self.tasks[name] = task_id
                return task_id
            return None
        
        def update_task(self, name: str, advance: int = 1,
                       description: Optional[str] = None):
            """
            Update task progress.
            
            Args:
                name: Task name
                advance: Steps to advance
                description: New description
            """
            if self.progress and name in self.tasks:
                task_id = self.tasks[name]
                if description:
                    self.progress.update(task_id, 
                                       description=description)
                self.progress.update(task_id, advance=advance)
        
        def complete_task(self, name: str):
            """
            Mark task as complete.
            
            Args:
                name: Task name
            """
            if self.progress and name in self.tasks:
                task_id = self.tasks[name]
                self.progress.update(task_id, completed=True)
        
        def stop(self):
            """Stop progress monitoring."""
            if self.progress:
                self.progress.stop()
                self.progress = None
                self.tasks = {}
        
        def print(self, message: str, style: Optional[str] = None):
            """
            Print message with optional styling.
            
            Args:
                message: Message to print
                style: Rich style string
            """
            if style:
                self.console.print(message, style=style)
            else:
                self.console.print(message)
    
except ImportError:
    RICH_AVAILABLE = False
    RichProgressMonitor = None


def get_progress_monitor(use_rich: bool = True) -> Any:
    """
    Get appropriate progress monitor.
    
    Args:
        use_rich: Whether to use Rich if available
        
    Returns:
        Progress monitor instance
    """
    if use_rich and RICH_AVAILABLE:
        return RichProgressMonitor()
    else:
        return ProgressMonitor()