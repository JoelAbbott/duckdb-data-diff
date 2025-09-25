#!/usr/bin/env python3
"""
Schema Inspector - Examine raw data files to find matching columns
"""
import pandas as pd
import duckdb
import os

def inspect_file_schema(file_path, file_type, sample_rows=1000):
    """Inspect a file and return its schema information"""
    print(f"\n=== INSPECTING: {os.path.basename(file_path)} ===")
    
    if not os.path.exists(file_path):
        print(f"❌ File not found: {file_path}")
        return None
    
    try:
        if file_type == 'csv':
            # Try different encodings
            encodings = ['utf-8', 'utf-8-sig', 'latin-1', 'iso-8859-1', 'cp1252']
            df = None
            for encoding in encodings:
                try:
                    df = pd.read_csv(file_path, encoding=encoding, nrows=sample_rows)
                    print(f"✅ Successfully read with encoding: {encoding}")
                    break
                except UnicodeDecodeError:
                    continue
            if df is None:
                raise Exception("Could not read file with any encoding")
                
        elif file_type == 'excel':
            df = pd.read_excel(file_path, nrows=sample_rows)
        else:
            raise Exception(f"Unsupported file type: {file_type}")
        
        print(f"📊 Shape: {df.shape[0]} rows, {df.shape[1]} columns")
        print(f"📝 Columns: {list(df.columns)}")
        
        # Show data types
        print("\n🔍 Column Types:")
        for col in df.columns:
            print(f"  {col}: {df[col].dtype} (sample: {df[col].iloc[0] if len(df) > 0 else 'N/A'})")
        
        return df.columns.tolist()
        
    except Exception as e:
        print(f"❌ Error reading file: {str(e)}")
        return None

def find_common_columns(left_cols, right_cols):
    """Find columns that exist in both datasets"""
    if not left_cols or not right_cols:
        return []
    
    # Normalize column names for comparison (snake_case)
    def normalize_name(name):
        return name.lower().replace(' ', '_').replace('-', '_').replace('/', '_')
    
    left_normalized = {normalize_name(col): col for col in left_cols}
    right_normalized = {normalize_name(col): col for col in right_cols}
    
    common_normalized = set(left_normalized.keys()) & set(right_normalized.keys())
    
    print(f"\n🔗 COMMON COLUMNS (Normalized Names): {len(common_normalized)}")
    common_pairs = []
    for norm_name in sorted(common_normalized):
        left_orig = left_normalized[norm_name]
        right_orig = right_normalized[norm_name]
        print(f"  {norm_name}: '{left_orig}' <-> '{right_orig}'")
        common_pairs.append((left_orig, right_orig, norm_name))
    
    return common_pairs

def main():
    print("🔍 INVENTORY BALANCE SCHEMA INVESTIGATION")
    print("="*60)
    
    # File paths from test_key_validation.yaml
    left_file = "data/raw/netsuite_inventory_balance (1).csv"
    right_file = "data/raw/qa2_netsuite_inventory_balance.xlsx"
    
    # Inspect schemas
    left_columns = inspect_file_schema(left_file, 'csv', sample_rows=5000)
    right_columns = inspect_file_schema(right_file, 'excel', sample_rows=5000)
    
    if left_columns and right_columns:
        # Find common columns
        common_columns = find_common_columns(left_columns, right_columns)
        
        print(f"\n📋 ANALYSIS SUMMARY:")
        print(f"  Left dataset: {len(left_columns)} columns")
        print(f"  Right dataset: {len(right_columns)} columns") 
        print(f"  Common columns: {len(common_columns)} potential keys")
        
        if common_columns:
            print(f"\n🎯 COMPOSITE KEY CANDIDATES:")
            print(f"Consider combinations like:")
            candidates = [pair[2] for pair in common_columns[:5]]  # First 5 normalized names
            print(f"  - Single: {candidates}")
            if len(candidates) >= 2:
                print(f"  - Composite: [{candidates[0]}, {candidates[1]}]")
            if len(candidates) >= 3:
                print(f"  - Triple: [{candidates[0]}, {candidates[1]}, {candidates[2]}]")
    
    print(f"\n✅ Schema inspection complete")

if __name__ == "__main__":
    main()