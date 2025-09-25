"""
Data validation pipeline.
Single responsibility: validate data quality and structure.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional
import pandas as pd

from ..utils.logger import get_logger


logger = get_logger()


@dataclass
class ValidationIssue:
    """Single validation issue."""
    
    severity: str  # ERROR, WARNING, INFO
    category: str
    message: str
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ValidationReport:
    """Complete validation report."""
    
    is_valid: bool
    issues: List[ValidationIssue] = field(default_factory=list)
    stats: Dict[str, Any] = field(default_factory=dict)
    
    def add_issue(self, severity: str, category: str, 
                  message: str, **details):
        """Add an issue to the report."""
        issue = ValidationIssue(severity, category, message, details)
        self.issues.append(issue)
        
        if severity == "ERROR":
            self.is_valid = False
    
    def get_errors(self) -> List[ValidationIssue]:
        """Get only error-level issues."""
        return [i for i in self.issues if i.severity == "ERROR"]
    
    def get_warnings(self) -> List[ValidationIssue]:
        """Get only warning-level issues."""
        return [i for i in self.issues if i.severity == "WARNING"]


class Validator(ABC):
    """Base validator class."""
    
    @abstractmethod
    def validate(self, df: pd.DataFrame, 
                config: Optional[Dict[str, Any]] = None) -> ValidationReport:
        """
        Validate dataframe.
        
        Args:
            df: DataFrame to validate
            config: Optional validation configuration
            
        Returns:
            Validation report
        """
        pass


class SchemaValidator(Validator):
    """Validate data schema and structure."""
    
    def validate(self, df: pd.DataFrame, 
                config: Optional[Dict[str, Any]] = None) -> ValidationReport:
        """Validate schema."""
        report = ValidationReport(is_valid=True)
        
        logger.debug("validator.schema.checking", 
                    rows=len(df), 
                    columns=len(df.columns))
        
        # Check for empty dataframe
        if df.empty:
            report.add_issue(
                "ERROR", "schema", "DataFrame is empty",
                rows=0, columns=len(df.columns)
            )
            return report
        
        # Check for duplicate columns
        duplicate_cols = df.columns[df.columns.duplicated()].tolist()
        if duplicate_cols:
            report.add_issue(
                "ERROR", "schema", "Duplicate column names found",
                duplicates=duplicate_cols
            )
        
        # Check column types
        for col in df.columns:
            if df[col].dtype == 'object':
                # Check for mixed types
                types = df[col].dropna().apply(type).unique()
                if len(types) > 1:
                    report.add_issue(
                        "WARNING", "schema", f"Mixed types in column {col}",
                        column=col, types=[t.__name__ for t in types]
                    )
        
        report.stats["row_count"] = len(df)
        report.stats["column_count"] = len(df.columns)
        report.stats["dtypes"] = df.dtypes.value_counts().to_dict()
        
        return report


class DataTypeValidator(Validator):
    """Validate data types."""
    
    def validate(self, df: pd.DataFrame,
                config: Optional[Dict[str, Any]] = None) -> ValidationReport:
        """Validate data types."""
        report = ValidationReport(is_valid=True)
        
        logger.debug("validator.dtype.checking")
        
        # Check for expected types from config
        if config and "expected_types" in config:
            for col, expected_type in config["expected_types"].items():
                if col in df.columns:
                    actual_type = str(df[col].dtype)
                    if actual_type != expected_type:
                        report.add_issue(
                            "WARNING", "dtype", 
                            f"Type mismatch for column {col}",
                            column=col,
                            expected=expected_type,
                            actual=actual_type
                        )
        
        # Check for numeric columns that might be stored as strings
        for col in df.select_dtypes(include=['object']).columns:
            sample = df[col].dropna().head(100)
            if sample.empty:
                continue
                
            # Try to convert to numeric
            try:
                pd.to_numeric(sample)
                report.add_issue(
                    "INFO", "dtype",
                    f"Column {col} appears numeric but stored as string",
                    column=col
                )
            except:
                pass
        
        return report


class KeyValidator(Validator):
    """Validate key columns."""
    
    def validate(self, df: pd.DataFrame,
                config: Optional[Dict[str, Any]] = None) -> ValidationReport:
        """Validate key columns."""
        report = ValidationReport(is_valid=True)
        
        if not config or "key_columns" not in config:
            return report
        
        key_columns = config["key_columns"]
        
        logger.debug("validator.keys.checking", keys=key_columns)
        
        # BUG 4 fix: Check if key columns exist, considering column mapping
        column_map = config.get("column_map", {})
        
        # For each key column, check if it exists directly or through mapping
        missing_keys = []
        actual_keys = []  # The actual column names to use
        
        for key_col in key_columns:
            # Check if the column exists directly
            if key_col in df.columns:
                actual_keys.append(key_col)
            # Check if there's a mapping for this key (reverse lookup)
            elif column_map:
                # Find if any mapped column corresponds to this key
                mapped_col = None
                for orig_col, mapped_col in column_map.items():
                    if mapped_col == key_col and orig_col in df.columns:
                        actual_keys.append(orig_col)
                        mapped_col = orig_col
                        break
                if mapped_col is None:
                    missing_keys.append(key_col)
            else:
                missing_keys.append(key_col)
        
        if missing_keys:
            report.add_issue(
                "ERROR", "keys", "Key columns not found",
                missing=missing_keys,
                available_columns=list(df.columns),
                column_map=column_map
            )
            return report
        
        # Use actual_keys for subsequent validation
        key_columns = actual_keys
        
        # Check for null values in keys
        for col in key_columns:
            null_count = df[col].isnull().sum()
            if null_count > 0:
                report.add_issue(
                    "ERROR", "keys", f"Null values in key column {col}",
                    column=col,
                    null_count=int(null_count),
                    percent=round(100 * null_count / len(df), 2)
                )
        
        # Check for uniqueness
        if key_columns:
            dup_count = df.duplicated(subset=key_columns).sum()
            if dup_count > 0:
                report.add_issue(
                    "WARNING", "keys", "Duplicate key values found",
                    duplicate_count=int(dup_count),
                    percent=round(100 * dup_count / len(df), 2)
                )
                
                # Find examples of duplicates
                duplicates = df[df.duplicated(subset=key_columns, keep=False)]
                examples = duplicates.head(10)[key_columns].values.tolist()
                report.stats["duplicate_examples"] = examples
        
        report.stats["key_columns"] = key_columns
        report.stats["unique_keys"] = len(df[key_columns].drop_duplicates())
        
        return report


class DuplicateValidator(Validator):
    """Check for duplicate rows."""
    
    def validate(self, df: pd.DataFrame,
                config: Optional[Dict[str, Any]] = None) -> ValidationReport:
        """Check for duplicates."""
        report = ValidationReport(is_valid=True)
        
        logger.debug("validator.duplicates.checking")
        
        # Full row duplicates
        full_dup_count = df.duplicated().sum()
        if full_dup_count > 0:
            report.add_issue(
                "WARNING", "duplicates", "Full duplicate rows found",
                count=int(full_dup_count),
                percent=round(100 * full_dup_count / len(df), 2)
            )
        
        report.stats["full_duplicates"] = int(full_dup_count)
        
        return report


class ValidationPipeline:
    """
    Run validation checks in sequence.
    """
    
    def __init__(self, validators: Optional[List[Validator]] = None):
        """
        Initialize validation pipeline.
        
        Args:
            validators: List of validators to run
        """
        if validators is None:
            self.validators = [
                SchemaValidator(),
                DataTypeValidator(),
                KeyValidator(),
                DuplicateValidator()
            ]
        else:
            self.validators = validators
    
    def validate(self, df: pd.DataFrame, 
                config: Optional[Dict[str, Any]] = None) -> ValidationReport:
        """
        Run all validators.
        
        Args:
            df: DataFrame to validate
            config: Validation configuration
            
        Returns:
            Combined validation report
        """
        logger.info("validation.pipeline.starting",
                   validators=len(self.validators))
        
        combined_report = ValidationReport(is_valid=True)
        
        for validator in self.validators:
            validator_name = validator.__class__.__name__
            
            logger.debug("validation.pipeline.running", 
                        validator=validator_name)
            
            report = validator.validate(df, config)
            
            # Merge reports
            combined_report.issues.extend(report.issues)
            combined_report.stats[validator_name] = report.stats
            
            if not report.is_valid:
                combined_report.is_valid = False
                
                # Fail fast on errors if configured
                if config and config.get("fail_fast", False):
                    logger.warning("validation.pipeline.failed_fast",
                                 validator=validator_name)
                    break
        
        logger.info("validation.pipeline.completed",
                   is_valid=combined_report.is_valid,
                   errors=len(combined_report.get_errors()),
                   warnings=len(combined_report.get_warnings()))
        
        return combined_report