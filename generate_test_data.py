#!/usr/bin/env python3
"""
Generate test datasets for performance testing.
Creates CSV files with 100k, 300k, and 1M rows.
"""

import pandas as pd
import numpy as np
from pathlib import Path
import random
import string
from datetime import datetime, timedelta


def generate_large_dataset(num_rows: int, num_cols: int = 50, 
                         output_path: Path = None,
                         seed: int = 42) -> Path:
    """
    Generate large test dataset.
    
    Args:
        num_rows: Number of rows to generate
        num_cols: Number of columns
        output_path: Output file path
        seed: Random seed for reproducibility
        
    Returns:
        Path to generated file
    """
    np.random.seed(seed)
    random.seed(seed)
    
    if not output_path:
        output_path = Path(f"data/raw/test_{num_rows//1000}k.csv")
    
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    print(f"Generating {num_rows:,} rows with {num_cols} columns...")
    
    # Generate data
    data = {}
    
    # ID columns
    data['id'] = range(1, num_rows + 1)
    data['customer_id'] = [f"CUST{i:08d}" for i in range(1, num_rows + 1)]
    
    # String columns with some variations
    first_names = ['John', 'Jane', 'Bob', 'Alice', 'Charlie', 'Emma', 'Oliver', 'Sophia']
    last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis']
    data['first_name'] = np.random.choice(first_names, num_rows)
    data['last_name'] = np.random.choice(last_names, num_rows)
    
    # Add variations to test normalization
    data['full_name'] = [
        f"{fn}  {ln}" if i % 10 == 0 else f"{fn} {ln}"  # Some with extra spaces
        for i, (fn, ln) in enumerate(zip(data['first_name'], data['last_name']))
    ]
    
    # Email with unicode characters occasionally
    data['email'] = [
        f"{fn.lower()}.{ln.lower()}@example.com" if i % 20 != 0
        else f"{fn.lower()}.{ln.lower()}@éxample.com"
        for i, (fn, ln) in enumerate(zip(data['first_name'], data['last_name']))
    ]
    
    # Numeric columns
    data['age'] = np.random.randint(18, 80, num_rows)
    data['account_balance'] = np.random.uniform(0, 100000, num_rows).round(2)
    
    # Currency columns (test conversion)
    data['transaction_amount'] = [
        f"${amt:,.2f}" if i % 3 == 0 else str(amt)
        for i, amt in enumerate(np.random.uniform(10, 5000, num_rows).round(2))
    ]
    
    # Boolean columns
    data['is_active'] = np.random.choice(['T', 'F', 'true', 'false', '1', '0'], num_rows)
    
    # Date columns
    start_date = datetime(2020, 1, 1)
    data['created_date'] = [
        (start_date + timedelta(days=random.randint(0, 1000))).strftime('%Y-%m-%d')
        for _ in range(num_rows)
    ]
    
    # Hierarchical data (test strip_hierarchy)
    departments = ['Sales', 'Marketing', 'Engineering', 'Support', 'Finance']
    data['department'] = [
        f"Company:Division:{dept}" if i % 5 == 0 else dept
        for i, dept in enumerate(np.random.choice(departments, num_rows))
    ]
    
    # Add more columns to reach target
    for i in range(num_cols - len(data)):
        col_name = f"metric_{i+1}"
        data[col_name] = np.random.uniform(0, 1000, num_rows).round(3)
    
    # Create DataFrame
    df = pd.DataFrame(data)
    
    # Save to CSV
    print(f"Saving to {output_path}...")
    df.to_csv(output_path, index=False)
    
    file_size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"✓ Generated {output_path.name}: {num_rows:,} rows, {file_size_mb:.1f} MB")
    
    return output_path


def generate_matching_dataset(base_path: Path, output_path: Path = None,
                            modification_rate: float = 0.1,
                            deletion_rate: float = 0.05,
                            addition_rate: float = 0.05) -> Path:
    """
    Generate a matching dataset with controlled differences.
    
    Args:
        base_path: Path to base dataset
        output_path: Output path for matching dataset
        modification_rate: Fraction of rows to modify
        deletion_rate: Fraction of rows to delete
        addition_rate: Fraction of rows to add
        
    Returns:
        Path to generated file
    """
    if not output_path:
        output_path = base_path.parent / f"{base_path.stem}_match.csv"
    
    print(f"Generating matching dataset for {base_path.name}...")
    
    # Read base dataset
    df = pd.read_csv(base_path)
    original_rows = len(df)
    
    # Delete some rows
    num_deletions = int(original_rows * deletion_rate)
    if num_deletions > 0:
        delete_indices = np.random.choice(df.index, num_deletions, replace=False)
        df = df.drop(delete_indices)
        print(f"  Deleted {num_deletions:,} rows")
    
    # Modify some rows
    num_modifications = int(len(df) * modification_rate)
    if num_modifications > 0:
        modify_indices = np.random.choice(df.index, num_modifications, replace=False)
        
        # Modify numeric columns
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        if len(numeric_cols) > 0:
            for idx in modify_indices[:num_modifications//2]:
                col = np.random.choice(numeric_cols)
                df.loc[idx, col] = float(df.loc[idx, col]) * 1.1  # 10% change
        
        # Modify string columns
        string_cols = df.select_dtypes(include=['object']).columns
        if len(string_cols) > 0:
            for idx in modify_indices[num_modifications//2:]:
                col = np.random.choice(string_cols)
                if pd.notna(df.at[idx, col]):
                    df.at[idx, col] = str(df.at[idx, col]) + "_modified"
        
        print(f"  Modified {num_modifications:,} rows")
    
    # Add new rows
    num_additions = int(original_rows * addition_rate)
    if num_additions > 0:
        # Get max ID
        if 'id' in df.columns:
            max_id = int(df['id'].max())
            new_rows = []
            
            for i in range(num_additions):
                new_row = df.sample(1).iloc[0].copy()
                new_row['id'] = max_id + i + 1
                if 'customer_id' in new_row:
                    new_row['customer_id'] = f"CUST{max_id + i + 1:08d}"
                new_rows.append(new_row)
            
            df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
        
        print(f"  Added {num_additions:,} new rows")
    
    # Save
    df.to_csv(output_path, index=False)
    
    file_size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"✓ Generated {output_path.name}: {len(df):,} rows, {file_size_mb:.1f} MB")
    
    return output_path


def main():
    """Generate test datasets."""
    
    # Generate test datasets
    datasets = [
        (100_000, 30),   # 100k rows, 30 columns
        (300_000, 50),   # 300k rows, 50 columns
        (1_000_000, 75), # 1M rows, 75 columns
    ]
    
    for num_rows, num_cols in datasets:
        # Generate base dataset
        base_path = generate_large_dataset(num_rows, num_cols)
        
        # Generate matching dataset with differences
        generate_matching_dataset(base_path)
    
    print("\n✅ Test data generation complete!")
    print("\nGenerated files in data/raw/:")
    for path in Path("data/raw").glob("test_*.csv"):
        size_mb = path.stat().st_size / (1024 * 1024)
        print(f"  - {path.name}: {size_mb:.1f} MB")


if __name__ == "__main__":
    main()