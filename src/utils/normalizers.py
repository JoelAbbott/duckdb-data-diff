"""
Data normalization utilities.
Single responsibility: normalize and clean data values.
"""

import re
import unicodedata
from typing import Any, Optional, Union


def strip_hierarchy(val: str) -> str:
    """
    Strip hierarchical path from a string based on a colon delimiter.
    
    Args:
        val: Input string possibly containing hierarchical path
        
    Returns:
        Last part of hierarchy or original value
    """
    if not isinstance(val, str):
        return val
    parts = val.split(":")
    return parts[-1].strip()


def unicode_clean(val: str) -> str:
    """
    Normalize text by removing accents, unicode chars, and collapsing spaces.
    
    Args:
        val: Input string to clean
        
    Returns:
        Cleaned string
    """
    if not isinstance(val, str):
        return val
    
    # Decompose characters into base + combining marks
    nfkd_form = unicodedata.normalize('NFKD', val)
    val = "".join([c for c in nfkd_form if not unicodedata.combining(c)])
    
    # Remove zero-width characters
    val = re.sub(r"[\u200B-\u200D\u2060\ufeff]", "", val)
    
    # Normalize quotes and dashes
    val = val.replace(""", '"').replace(""", '"')
    val = val.replace("'", "'").replace("'", "'")
    val = val.replace("–", "-").replace("—", "-")
    
    # Collapse spaces
    val = re.sub(r"\s+", " ", val).strip()
    return val


def collapse_spaces(val: str) -> str:
    """
    Collapse multiple whitespace characters into a single space.
    
    Args:
        val: Input string
        
    Returns:
        String with collapsed spaces
    """
    if not isinstance(val, str):
        return val
    return re.sub(r"\s+", " ", val).strip()


def normalize_column_name(col: str) -> str:
    """
    Normalize column names for comparison.
    
    Args:
        col: Column name to normalize
        
    Returns:
        Normalized column name
    """
    # Convert to lowercase
    normalized = col.lower()
    
    # Replace spaces and special characters with underscores
    normalized = re.sub(r'[^\w\s]', '_', normalized)
    normalized = re.sub(r'\s+', '_', normalized)
    
    # Remove leading/trailing underscores
    normalized = normalized.strip('_')
    
    # Collapse multiple underscores
    normalized = re.sub(r'_+', '_', normalized)
    
    return normalized