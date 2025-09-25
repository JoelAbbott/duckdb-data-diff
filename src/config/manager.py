"""
Configuration management.
Single responsibility: load, validate, and manage configuration.
"""

import yaml
from pathlib import Path
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field

from ..utils.logger import get_logger


logger = get_logger()


@dataclass
class DatasetConfig:
    """Configuration for a single dataset."""
    
    path: str
    name: str
    type: str = "csv"
    key_columns: List[str] = field(default_factory=list)
    exclude_columns: List[str] = field(default_factory=list)
    normalizers: Dict[str, str] = field(default_factory=dict)
    converters: Dict[str, str] = field(default_factory=dict)
    column_map: Dict[str, str] = field(default_factory=dict)  # BUG 3 fix: support column mappings
    custom_sql: Optional[str] = None
    chunk_size: int = 10000
    
    def __post_init__(self):
        """Validate configuration after initialization."""
        if not self.path:
            raise ValueError("Dataset path is required")
        if not self.name:
            raise ValueError("Dataset name is required")


@dataclass
class ComparisonConfig:
    """Configuration for dataset comparison."""
    
    left_dataset: str
    right_dataset: str
    comparison_keys: List[str] = field(default_factory=list)
    value_columns: List[str] = field(default_factory=list)
    tolerance: float = 0.01
    ignore_case: bool = True
    ignore_spaces: bool = True
    output_format: str = "excel"
    max_differences: int = 1000


class ConfigManager:
    """
    Manage application configuration.
    """
    
    def __init__(self, config_path: Optional[Path] = None):
        """
        Initialize config manager.
        
        Args:
            config_path: Path to configuration file
        """
        self.config_path = config_path or Path("datasets.yaml")
        self.config: Dict[str, Any] = {}
        self.datasets: Dict[str, DatasetConfig] = {}
        self.comparisons: List[ComparisonConfig] = []
    
    def load(self) -> Dict[str, Any]:
        """
        Load configuration from file.
        
        Returns:
            Configuration dictionary
            
        Raises:
            FileNotFoundError: If config file doesn't exist
            yaml.YAMLError: If config is invalid YAML
        """
        if not self.config_path.exists():
            raise FileNotFoundError(f"Config not found: {self.config_path}")
        
        logger.info("config.loading", file=str(self.config_path))
        
        with open(self.config_path) as f:
            self.config = yaml.safe_load(f)
        
        self._parse_datasets()
        self._parse_comparisons()
        
        logger.info("config.loaded", 
                   datasets=len(self.datasets),
                   comparisons=len(self.comparisons))
        
        return self.config
    
    def _parse_datasets(self):
        """Parse dataset configurations."""
        if "datasets" not in self.config:
            return
        
        for name, cfg in self.config["datasets"].items():
            try:
                dataset_cfg = DatasetConfig(
                    name=name,
                    path=cfg.get("path", ""),
                    type=cfg.get("type", "csv"),
                    key_columns=cfg.get("key_columns", []),
                    exclude_columns=cfg.get("exclude_columns", []),
                    normalizers=cfg.get("normalizers", {}),
                    converters=cfg.get("converters", {}),
                    column_map=cfg.get("column_map", {}),  # BUG 3 fix: parse column mappings
                    custom_sql=cfg.get("custom_sql"),
                    chunk_size=cfg.get("chunk_size", 10000)
                )
                self.datasets[name] = dataset_cfg
            except Exception as e:
                logger.error("config.dataset.invalid", 
                           dataset=name, 
                           error=str(e))
                raise
    
    def _parse_comparisons(self):
        """Parse comparison configurations."""
        if "comparisons" not in self.config:
            return
        
        for cmp in self.config["comparisons"]:
            try:
                comparison_cfg = ComparisonConfig(
                    left_dataset=cmp.get("left"),
                    right_dataset=cmp.get("right"),
                    comparison_keys=cmp.get("keys", []),
                    value_columns=cmp.get("columns", []),
                    tolerance=cmp.get("tolerance", 0.01),
                    ignore_case=cmp.get("ignore_case", True),
                    ignore_spaces=cmp.get("ignore_spaces", True),
                    output_format=cmp.get("output_format", "excel"),
                    max_differences=cmp.get("max_differences", 1000)
                )
                self.comparisons.append(comparison_cfg)
            except Exception as e:
                logger.error("config.comparison.invalid",
                           comparison=cmp,
                           error=str(e))
                raise
    
    def get_dataset(self, name: str) -> DatasetConfig:
        """
        Get dataset configuration by name.
        
        Args:
            name: Dataset name
            
        Returns:
            Dataset configuration
            
        Raises:
            KeyError: If dataset not found
        """
        if name not in self.datasets:
            raise KeyError(f"Dataset not found: {name}")
        return self.datasets[name]
    
    def save(self, path: Optional[Path] = None):
        """
        Save configuration to file.
        
        Args:
            path: Output path (uses original path if not specified)
        """
        output_path = path or self.config_path
        
        logger.info("config.saving", file=str(output_path))
        
        # Convert dataclasses back to dictionaries
        config_dict = {
            "datasets": {},
            "comparisons": []
        }
        
        for name, dataset in self.datasets.items():
            config_dict["datasets"][name] = {
                "path": dataset.path,
                "type": dataset.type,
                "key_columns": dataset.key_columns,
                "exclude_columns": dataset.exclude_columns,
                "normalizers": dataset.normalizers,
                "converters": dataset.converters,
                "column_map": dataset.column_map,  # BUG 3 fix: save column mappings
                "chunk_size": dataset.chunk_size
            }
            if dataset.custom_sql:
                config_dict["datasets"][name]["custom_sql"] = dataset.custom_sql
        
        for comparison in self.comparisons:
            config_dict["comparisons"].append({
                "left": comparison.left_dataset,
                "right": comparison.right_dataset,
                "keys": comparison.comparison_keys,
                "columns": comparison.value_columns,
                "tolerance": comparison.tolerance,
                "ignore_case": comparison.ignore_case,
                "ignore_spaces": comparison.ignore_spaces,
                "output_format": comparison.output_format,
                "max_differences": comparison.max_differences
            })
        
        with open(output_path, 'w') as f:
            yaml.dump(config_dict, f, default_flow_style=False)
        
        logger.info("config.saved", file=str(output_path))