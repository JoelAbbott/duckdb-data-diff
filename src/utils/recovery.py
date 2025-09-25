"""
Recovery and checkpoint management.
Single responsibility: handle failures gracefully and enable resume.
"""

import json
import pickle
from pathlib import Path
from typing import Dict, Any, Optional, List
from datetime import datetime
from dataclasses import dataclass, field, asdict

from .logger import get_logger


logger = get_logger()


@dataclass
class ProcessingState:
    """State of processing for recovery."""
    
    pipeline_id: str
    timestamp: datetime
    config_file: str
    current_step: str
    completed_steps: List[str] = field(default_factory=list)
    staged_datasets: Dict[str, str] = field(default_factory=dict)
    completed_comparisons: List[Dict[str, str]] = field(default_factory=list)
    failed_operations: List[Dict[str, Any]] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


class RecoveryManager:
    """
    Handle failures gracefully and enable pipeline recovery.
    """
    
    def __init__(self, checkpoint_dir: Optional[Path] = None):
        """
        Initialize recovery manager.
        
        Args:
            checkpoint_dir: Directory for checkpoints
        """
        self.checkpoint_dir = Path(checkpoint_dir or "data/.checkpoints")
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self.current_state: Optional[ProcessingState] = None
        self.pipeline_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        
    def initialize_pipeline(self, config_file: str) -> ProcessingState:
        """
        Initialize new pipeline state.
        
        Args:
            config_file: Configuration file path
            
        Returns:
            New processing state
        """
        self.current_state = ProcessingState(
            pipeline_id=self.pipeline_id,
            timestamp=datetime.now(),
            config_file=config_file,
            current_step="initialized"
        )
        
        logger.info("recovery.pipeline.initialized",
                   pipeline_id=self.pipeline_id,
                   config=config_file)
        
        self.save_checkpoint()
        return self.current_state
    
    def save_checkpoint(self, step: Optional[str] = None) -> Path:
        """
        Save current state checkpoint.
        
        Args:
            step: Current step name
            
        Returns:
            Path to checkpoint file
        """
        if not self.current_state:
            logger.warning("recovery.checkpoint.no_state")
            return None
        
        if step:
            self.current_state.current_step = step
        
        checkpoint_file = (
            self.checkpoint_dir / 
            f"checkpoint_{self.pipeline_id}.json"
        )
        
        # Convert to dictionary for JSON serialization
        state_dict = asdict(self.current_state)
        
        # Convert datetime to string
        state_dict['timestamp'] = state_dict['timestamp'].isoformat()
        
        with open(checkpoint_file, 'w') as f:
            json.dump(state_dict, f, indent=2, default=str)
        
        logger.debug("recovery.checkpoint.saved",
                    file=str(checkpoint_file),
                    step=self.current_state.current_step)
        
        return checkpoint_file
    
    def load_checkpoint(self, checkpoint_file: Optional[Path] = None) -> Optional[ProcessingState]:
        """
        Load checkpoint from file.
        
        Args:
            checkpoint_file: Specific checkpoint file (or find latest)
            
        Returns:
            Loaded processing state or None
        """
        if not checkpoint_file:
            # Find most recent checkpoint
            checkpoints = sorted(
                self.checkpoint_dir.glob("checkpoint_*.json"),
                key=lambda x: x.stat().st_mtime,
                reverse=True
            )
            
            if not checkpoints:
                logger.info("recovery.checkpoint.none_found")
                return None
            
            checkpoint_file = checkpoints[0]
        
        logger.info("recovery.checkpoint.loading",
                   file=str(checkpoint_file))
        
        try:
            with open(checkpoint_file, 'r') as f:
                state_dict = json.load(f)
            
            # Convert timestamp back to datetime
            state_dict['timestamp'] = datetime.fromisoformat(
                state_dict['timestamp']
            )
            
            self.current_state = ProcessingState(**state_dict)
            self.pipeline_id = self.current_state.pipeline_id
            
            logger.info("recovery.checkpoint.loaded",
                       pipeline_id=self.pipeline_id,
                       step=self.current_state.current_step,
                       completed_steps=len(self.current_state.completed_steps))
            
            return self.current_state
            
        except Exception as e:
            logger.error("recovery.checkpoint.load_failed",
                        file=str(checkpoint_file),
                        error=str(e))
            return None
    
    def mark_step_complete(self, step: str, metadata: Optional[Dict] = None):
        """
        Mark a step as complete.
        
        Args:
            step: Step name
            metadata: Optional step metadata
        """
        if not self.current_state:
            return
        
        self.current_state.completed_steps.append(step)
        
        if metadata:
            self.current_state.metadata[step] = metadata
        
        logger.debug("recovery.step.completed",
                    step=step,
                    total_completed=len(self.current_state.completed_steps))
        
        self.save_checkpoint(step)
    
    def record_staged_dataset(self, dataset_name: str, 
                             staging_path: str):
        """
        Record that a dataset has been staged.
        
        Args:
            dataset_name: Dataset name
            staging_path: Path to staged file
        """
        if not self.current_state:
            return
        
        self.current_state.staged_datasets[dataset_name] = staging_path
        
        logger.debug("recovery.dataset.staged",
                    dataset=dataset_name,
                    path=staging_path)
        
        self.save_checkpoint()
    
    def record_comparison_complete(self, left: str, right: str,
                                  output_path: str):
        """
        Record that a comparison has completed.
        
        Args:
            left: Left dataset name
            right: Right dataset name
            output_path: Path to comparison output
        """
        if not self.current_state:
            return
        
        comparison = {
            "left": left,
            "right": right,
            "output": output_path,
            "timestamp": datetime.now().isoformat()
        }
        
        self.current_state.completed_comparisons.append(comparison)
        
        logger.debug("recovery.comparison.completed",
                    left=left,
                    right=right)
        
        self.save_checkpoint()
    
    def record_failure(self, operation: str, error: str,
                      context: Optional[Dict] = None):
        """
        Record a failure for recovery analysis.
        
        Args:
            operation: Operation that failed
            error: Error message
            context: Additional context
        """
        if not self.current_state:
            return
        
        failure = {
            "operation": operation,
            "error": error,
            "timestamp": datetime.now().isoformat(),
            "context": context or {}
        }
        
        self.current_state.failed_operations.append(failure)
        
        logger.error("recovery.failure.recorded",
                    operation=operation,
                    error=error)
        
        self.save_checkpoint()
    
    def can_resume(self) -> bool:
        """
        Check if pipeline can be resumed.
        
        Returns:
            True if resumable
        """
        if not self.current_state:
            return False
        
        # Check if we have critical failures
        critical_failures = [
            f for f in self.current_state.failed_operations
            if "critical" in f.get("context", {})
        ]
        
        if critical_failures:
            logger.warning("recovery.resume.critical_failures",
                         count=len(critical_failures))
            return False
        
        return True
    
    def get_resume_point(self) -> Dict[str, Any]:
        """
        Get information about where to resume.
        
        Returns:
            Resume point information
        """
        if not self.current_state:
            return {}
        
        resume_info = {
            "pipeline_id": self.current_state.pipeline_id,
            "last_step": self.current_state.current_step,
            "completed_steps": self.current_state.completed_steps,
            "staged_datasets": list(self.current_state.staged_datasets.keys()),
            "completed_comparisons": len(self.current_state.completed_comparisons),
            "can_resume": self.can_resume()
        }
        
        # Determine what needs to be done
        resume_info["next_steps"] = self._determine_next_steps()
        
        return resume_info
    
    def _determine_next_steps(self) -> List[str]:
        """
        Determine what steps need to be completed.
        
        Returns:
            List of next steps
        """
        if not self.current_state:
            return []
        
        next_steps = []
        
        # Check common pipeline steps
        pipeline_steps = [
            "load_config",
            "validate_config",
            "stage_datasets",
            "normalize_columns",
            "run_comparisons",
            "generate_reports"
        ]
        
        for step in pipeline_steps:
            if step not in self.current_state.completed_steps:
                next_steps.append(step)
        
        return next_steps
    
    def cleanup_failed_run(self, remove_staged: bool = False):
        """
        Clean up after a failed run.
        
        Args:
            remove_staged: Whether to remove staged files
        """
        if not self.current_state:
            return
        
        logger.info("recovery.cleanup.starting",
                   pipeline_id=self.current_state.pipeline_id)
        
        # Remove staged files if requested
        if remove_staged:
            for dataset, path in self.current_state.staged_datasets.items():
                file_path = Path(path)
                if file_path.exists():
                    file_path.unlink()
                    logger.debug("recovery.cleanup.removed_staged",
                               dataset=dataset,
                               path=path)
        
        # Archive checkpoint
        checkpoint_file = (
            self.checkpoint_dir /
            f"checkpoint_{self.pipeline_id}.json"
        )
        
        if checkpoint_file.exists():
            archive_file = (
                self.checkpoint_dir /
                "archive" /
                f"checkpoint_{self.pipeline_id}_failed.json"
            )
            archive_file.parent.mkdir(exist_ok=True)
            checkpoint_file.rename(archive_file)
            
            logger.info("recovery.cleanup.checkpoint_archived",
                       file=str(archive_file))
    
    def cleanup_old_checkpoints(self, days_to_keep: int = 7):
        """
        Remove old checkpoint files.
        
        Args:
            days_to_keep: Days of checkpoints to keep
        """
        import time
        
        cutoff_time = time.time() - (days_to_keep * 24 * 60 * 60)
        
        removed = 0
        for checkpoint in self.checkpoint_dir.glob("checkpoint_*.json"):
            if checkpoint.stat().st_mtime < cutoff_time:
                checkpoint.unlink()
                removed += 1
        
        if removed > 0:
            logger.info("recovery.cleanup.old_checkpoints",
                       removed=removed,
                       days=days_to_keep)