"""
Rich progress monitoring with beautiful UI.
Single responsibility: provide rich terminal UI for progress tracking.
"""

from typing import Optional, Dict, Any, List
from contextlib import contextmanager
import time
from datetime import datetime

from rich.console import Console
from rich.progress import (
    Progress,
    SpinnerColumn,
    TextColumn,
    BarColumn,
    TaskProgressColumn,
    TimeRemainingColumn,
    TimeElapsedColumn,
    MofNCompleteColumn
)
from rich.table import Table
from rich.panel import Panel
from rich.layout import Layout
from rich.live import Live
from rich.text import Text
from rich import box

from ..utils.logger import get_logger


logger = get_logger()


class RichProgressMonitor:
    """
    Enhanced progress monitoring using Rich library.
    """
    
    def __init__(self):
        """Initialize Rich progress monitor."""
        self.console = Console()
        self.progress = None
        self.tasks = {}
        self.start_time = None
        self.metrics = {}
        
    def start_pipeline(self, title: str = "DuckDB Data Diff Pipeline"):
        """
        Start pipeline monitoring with header.
        
        Args:
            title: Pipeline title
        """
        self.start_time = datetime.now()
        
        # Clear screen and show header
        self.console.clear()
        
        header = Panel(
            Text(title, justify="center", style="bold cyan"),
            box=box.DOUBLE,
            style="cyan"
        )
        self.console.print(header)
        self.console.print()
        
        # Initialize progress bars
        self.progress = Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TimeRemainingColumn(),
            TimeElapsedColumn(),
            console=self.console,
            refresh_per_second=10
        )
        self.progress.start()
        
    def add_task(self, name: str, total: Optional[int] = None,
                description: Optional[str] = None) -> str:
        """
        Add a new task to track.
        
        Args:
            name: Task identifier
            total: Total steps (None for indeterminate)
            description: Task description
            
        Returns:
            Task ID
        """
        if not self.progress:
            self.start_pipeline()
        
        desc = description or name
        task_id = self.progress.add_task(desc, total=total)
        self.tasks[name] = task_id
        
        logger.debug("rich_progress.task.added",
                    name=name,
                    total=total)
        
        return task_id
    
    def update_task(self, name: str, advance: int = 1,
                   completed: Optional[int] = None,
                   description: Optional[str] = None,
                   total: Optional[int] = None):
        """
        Update task progress.
        
        Args:
            name: Task name
            advance: Steps to advance
            completed: Set completed amount directly
            description: Update description
            total: Update total
        """
        if name not in self.tasks:
            return
        
        task_id = self.tasks[name]
        kwargs = {}
        
        if description:
            kwargs["description"] = description
        if total is not None:
            kwargs["total"] = total
        if completed is not None:
            kwargs["completed"] = completed
        else:
            kwargs["advance"] = advance
        
        self.progress.update(task_id, **kwargs)
    
    def complete_task(self, name: str, message: Optional[str] = None):
        """
        Mark task as complete.
        
        Args:
            name: Task name
            message: Completion message
        """
        if name not in self.tasks:
            return
        
        task_id = self.tasks[name]
        
        if message:
            self.progress.update(task_id, description=f"✓ {message}")
        
        # Ensure task shows as 100% complete
        task_meta = self.progress.tasks[task_id]
        if task_meta.total:
            self.progress.update(task_id, completed=task_meta.total)
        
        logger.info("rich_progress.task.completed", name=name)
    
    def show_comparison_results(self, results: Dict[str, Any]):
        """
        Display comparison results in a formatted table.
        
        Args:
            results: Comparison results dictionary
        """
        table = Table(title="Comparison Results", box=box.ROUNDED)
        
        table.add_column("Metric", style="cyan", no_wrap=True)
        table.add_column("Value", style="magenta")
        table.add_column("Percentage", style="green")
        
        total = results.get("total_left", 0) + results.get("total_right", 0)
        
        metrics = [
            ("Total Left Dataset", results.get("total_left", 0), None),
            ("Total Right Dataset", results.get("total_right", 0), None),
            ("Matched Rows", results.get("matched_rows", 0),
             results.get("match_rate", 0)),
            ("Only in Left", results.get("only_in_left", 0),
             (100 * results.get("only_in_left", 0) / 
              results.get("total_left", 1)) if results.get("total_left") else 0),
            ("Only in Right", results.get("only_in_right", 0),
             (100 * results.get("only_in_right", 0) / 
              results.get("total_right", 1)) if results.get("total_right") else 0),
            ("Value Differences", results.get("value_differences", 0),
             results.get("difference_rate", 0)),
        ]
        
        for metric, value, percentage in metrics:
            if percentage is not None:
                table.add_row(
                    metric,
                    f"{value:,}",
                    f"{percentage:.1f}%"
                )
            else:
                table.add_row(metric, f"{value:,}", "—")
        
        self.console.print()
        self.console.print(table)
        self.console.print()
    
    def show_validation_summary(self, reports: List[Dict[str, Any]]):
        """
        Display validation summary.
        
        Args:
            reports: List of validation reports
        """
        table = Table(title="Data Validation Summary", box=box.SIMPLE)
        
        table.add_column("Dataset", style="cyan")
        table.add_column("Status", style="green")
        table.add_column("Errors", style="red")
        table.add_column("Warnings", style="yellow")
        table.add_column("Rows", style="blue")
        
        for report in reports:
            dataset = report.get("dataset", "Unknown")
            is_valid = report.get("is_valid", False)
            errors = report.get("error_count", 0)
            warnings = report.get("warning_count", 0)
            rows = report.get("row_count", 0)
            
            status = "✓ Valid" if is_valid else "✗ Invalid"
            status_style = "green" if is_valid else "red"
            
            table.add_row(
                dataset,
                Text(status, style=status_style),
                str(errors) if errors > 0 else "—",
                str(warnings) if warnings > 0 else "—",
                f"{rows:,}"
            )
        
        self.console.print(table)
        self.console.print()
    
    def log_error(self, message: str, details: Optional[Dict] = None):
        """
        Display error message.
        
        Args:
            message: Error message
            details: Additional error details
        """
        error_text = Text(f"✗ {message}", style="bold red")
        
        if details:
            panel = Panel(
                error_text,
                title="Error",
                border_style="red",
                expand=False
            )
            self.console.print(panel)
            
            for key, value in details.items():
                self.console.print(f"  {key}: {value}", style="dim")
        else:
            self.console.print(error_text)
    
    def log_warning(self, message: str):
        """
        Display warning message.
        
        Args:
            message: Warning message
        """
        self.console.print(f"⚠ {message}", style="yellow")
    
    def log_success(self, message: str):
        """
        Display success message.
        
        Args:
            message: Success message
        """
        self.console.print(f"✓ {message}", style="green")
    
    def show_metrics(self, metrics: Dict[str, Any]):
        """
        Display performance metrics.
        
        Args:
            metrics: Metrics dictionary
        """
        self.metrics.update(metrics)
        
        table = Table(title="Performance Metrics", box=box.SIMPLE)
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="magenta")
        
        for key, value in self.metrics.items():
            if isinstance(value, float):
                table.add_row(key, f"{value:.2f}")
            elif isinstance(value, int):
                table.add_row(key, f"{value:,}")
            else:
                table.add_row(key, str(value))
        
        self.console.print(table)
    
    def stop(self):
        """Stop progress monitoring and show summary."""
        if self.progress:
            self.progress.stop()
        
        if self.start_time:
            elapsed = datetime.now() - self.start_time
            
            summary = Panel(
                Text(
                    f"Pipeline completed in {elapsed.total_seconds():.1f} seconds",
                    justify="center",
                    style="bold green"
                ),
                box=box.DOUBLE,
                style="green"
            )
            self.console.print()
            self.console.print(summary)
    
    @contextmanager
    def task_context(self, name: str, description: str,
                    total: Optional[int] = None):
        """
        Context manager for task tracking.
        
        Args:
            name: Task name
            description: Task description
            total: Total steps
            
        Example:
            with monitor.task_context("staging", "Staging datasets", 5) as task:
                for dataset in datasets:
                    # Process dataset
                    task.update()
        """
        task_id = self.add_task(name, total, description)
        
        class TaskContext:
            def __init__(self, monitor, name):
                self.monitor = monitor
                self.name = name
            
            def update(self, advance=1, **kwargs):
                self.monitor.update_task(self.name, advance, **kwargs)
        
        try:
            yield TaskContext(self, name)
            self.complete_task(name)
        except Exception as e:
            self.log_error(f"Task {name} failed: {e}")
            raise
    
    def create_live_dashboard(self) -> Live:
        """
        Create a live updating dashboard.
        
        Returns:
            Live dashboard object
        """
        layout = Layout()
        
        layout.split(
            Layout(name="header", size=3),
            Layout(name="body"),
            Layout(name="footer", size=3)
        )
        
        layout["header"].update(
            Panel("DuckDB Data Diff - Live Dashboard", style="cyan")
        )
        
        layout["body"].split_row(
            Layout(name="progress"),
            Layout(name="metrics")
        )
        
        return Live(layout, refresh_per_second=4, console=self.console)