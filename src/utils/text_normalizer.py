"""
Text normalization utilities for handling encoding and character issues.
Ensures consistent text comparison regardless of encoding variations.
"""

import unicodedata
import re
from typing import Optional


def normalize_text_for_comparison(text: Optional[str]) -> Optional[str]:
    """
    Normalize text for comparison to handle encoding issues.
    
    This function handles:
    - Non-breaking spaces (\\xa0, &nbsp;, etc.)
    - Unicode normalization (é vs e + ́)
    - Multiple spaces
    - Special quotes and dashes
    - Invisible characters
    - Different line endings
    
    Args:
        text: Input text that may have encoding issues
        
    Returns:
        Normalized text for comparison, or None if input is None
    """
    if text is None or pd.isna(text):
        return None
    
    # Convert to string if not already
    text = str(text)
    
    # 1. Normalize Unicode (NFC = Canonical Decomposition, followed by Canonical Composition)
    # This handles cases like é (single char) vs e + ́ (two chars)
    text = unicodedata.normalize('NFC', text)
    
    # 2. Replace non-breaking spaces and other space variants with regular spaces
    # \xa0 = non-breaking space, \u00a0 = unicode non-breaking space
    text = text.replace('\xa0', ' ').replace('\u00a0', ' ').replace('\u202f', ' ')
    text = text.replace('\u2009', ' ')  # Thin space
    text = text.replace('\u200a', ' ')  # Hair space
    
    # 3. Replace special quotes with standard quotes
    # Smart quotes, curly quotes, etc.
    text = text.replace('"', '"').replace('"', '"')  # Double quotes
    text = text.replace(''', "'").replace(''', "'")  # Single quotes
    text = text.replace('`', "'").replace('´', "'")  # Backticks and acute accents
    
    # 4. Replace special dashes with standard dashes
    text = text.replace('–', '-')  # En dash
    text = text.replace('—', '-')  # Em dash
    text = text.replace('‐', '-')  # Hyphen
    text = text.replace('­', '')   # Soft hyphen (invisible)
    
    # 5. Remove zero-width characters (invisible but affect comparison)
    text = text.replace('\u200b', '')  # Zero-width space
    text = text.replace('\u200c', '')  # Zero-width non-joiner
    text = text.replace('\u200d', '')  # Zero-width joiner
    text = text.replace('\ufeff', '')  # Zero-width no-break space (BOM)
    
    # 6. Normalize line endings
    text = text.replace('\r\n', '\n').replace('\r', '\n')
    
    # 7. Collapse multiple spaces into single space
    text = re.sub(r'\s+', ' ', text)
    
    # 8. Strip leading/trailing whitespace
    text = text.strip()
    
    # 9. Handle empty strings
    if not text:
        return None
        
    return text


def normalize_for_display(text: Optional[str]) -> str:
    """
    Normalize text for display purposes (less aggressive than comparison).
    Preserves more formatting while fixing obvious encoding issues.
    
    Args:
        text: Input text
        
    Returns:
        Cleaned text for display
    """
    if text is None or pd.isna(text):
        return ""
    
    text = str(text)
    
    # Fix common encoding issues but preserve intentional formatting
    text = text.replace('\xa0', ' ')  # Non-breaking space to regular space
    text = unicodedata.normalize('NFC', text)  # Unicode normalization
    
    # Fix mojibake (UTF-8 decoded as Latin-1)
    # Common patterns:
    replacements = {
        'Ã¢': 'â',
        'Ã©': 'é', 
        'Ã¨': 'è',
        'Ã´': 'ô',
        'Ã ': 'à',
        'Ã§': 'ç',
        'Ã±': 'ñ',
        'â€™': "'",
        'â€"': '–',
        'â€"': '—',
        'â€œ': '"',
        'â€\x9d': '"',
    }
    
    for bad, good in replacements.items():
        text = text.replace(bad, good)
    
    return text


def create_normalized_comparison_sql(left_col: str, right_col: str) -> str:
    """
    Create SQL for normalized text comparison in DuckDB.
    
    Args:
        left_col: Left column reference (e.g., 'l.name')
        right_col: Right column reference (e.g., 'r.name')
        
    Returns:
        SQL expression for normalized comparison
    """
    # DuckDB SQL for text normalization
    # This handles the most common cases directly in SQL
    normalize_sql = """
    TRIM(
        REGEXP_REPLACE(
            REGEXP_REPLACE(
                REPLACE(
                    REPLACE(
                        REPLACE(
                            REPLACE({col}, CHR(160), ' '),  -- Non-breaking space
                            CHR(8217), ''''),  -- Smart quote
                        CHR(8220), '"'),  -- Left double quote
                    CHR(8221), '"'),  -- Right double quote
                '\\s+', ' ', 'g'),  -- Multiple spaces to single
            '^\\s+|\\s+$', '', 'g')  -- Trim
    )
    """
    
    left_normalized = normalize_sql.format(col=left_col)
    right_normalized = normalize_sql.format(col=right_col)
    
    return f"{left_normalized} = {right_normalized}"


# Import pandas here to avoid circular imports
import pandas as pd


def normalize_dataframe_text(df: pd.DataFrame, columns: list = None) -> pd.DataFrame:
    """
    Normalize text in specified columns of a DataFrame.
    
    Args:
        df: Input DataFrame
        columns: List of columns to normalize (None = all object columns)
        
    Returns:
        DataFrame with normalized text
    """
    df = df.copy()
    
    if columns is None:
        # Normalize all text columns
        columns = df.select_dtypes(include=['object']).columns.tolist()
    
    for col in columns:
        if col in df.columns:
            df[col] = df[col].apply(normalize_text_for_comparison)
    
    return df