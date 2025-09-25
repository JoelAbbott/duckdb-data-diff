"""
Data type conversion utilities.
Single responsibility: convert between data types safely.
"""

from typing import Any, Optional, Union


def currency_to_float(val: Any) -> Optional[float]:
    """
    Convert currency string to float.
    Handles $, commas, and parentheses for negatives.
    
    Args:
        val: Value to convert (string, int, float, or None)
        
    Returns:
        Float value or None if conversion fails
        
    Examples:
        >>> currency_to_float("$1,234.56")
        1234.56
        >>> currency_to_float("(100)")
        -100.0
    """
    if val is None:
        return None
        
    if isinstance(val, (int, float)):
        return float(val)
        
    if not isinstance(val, str):
        return None
    
    val = val.strip()
    
    # Check for negative format with parentheses
    is_negative = val.startswith("(") and val.endswith(")")
    val = val.strip("() ")
    
    # Remove currency symbols and commas
    val = val.replace("$", "").replace(",", "")
    
    try:
        num = float(val)
        return -num if is_negative else num
    except (ValueError, TypeError):
        return None


def normalize_boolean(val: Any) -> Optional[str]:
    """
    Normalize boolean values to 't' or 'f'.
    
    Args:
        val: Value to normalize
        
    Returns:
        't' for true, 'f' for false, None for null/invalid
        
    Examples:
        >>> normalize_boolean("True")
        't'
        >>> normalize_boolean(0)
        'f'
    """
    if val is None:
        return None
        
    val = str(val).lower().strip()
    
    if val in ('t', 'true', '1', 'yes', 'y'):
        return 't'
    elif val in ('f', 'false', '0', 'no', 'n'):
        return 'f'
    else:
        return None


def safe_cast(val: Any, target_type: type) -> Optional[Any]:
    """
    Safely cast a value to target type.
    
    Args:
        val: Value to cast
        target_type: Target type (int, float, str, bool)
        
    Returns:
        Cast value or None if casting fails
    """
    if val is None:
        return None
    
    try:
        if target_type == bool:
            return normalize_boolean(val) == 't'
        else:
            return target_type(val)
    except (ValueError, TypeError):
        return None