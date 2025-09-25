#!/usr/bin/env python3
"""
Key Uniqueness Validator - Test potential keys using DuckDB
"""
import pandas as pd
import duckdb
import os

def validate_key_uniqueness(file_path, file_type, key_columns, dataset_name):
    """Validate if key columns provide unique identification"""
    print(f"\n🔍 VALIDATING KEY UNIQUENESS: {dataset_name}")
    print(f"   File: {os.path.basename(file_path)}")
    print(f"   Key: {key_columns}")
    
    try:
        # Read data
        if file_type == 'csv':
            encodings = ['utf-8', 'utf-8-sig', 'latin-1', 'iso-8859-1', 'cp1252']
            df = None
            for encoding in encodings:
                try:
                    df = pd.read_csv(file_path, encoding=encoding)
                    break
                except UnicodeDecodeError:
                    continue
        elif file_type == 'excel':
            df = pd.read_excel(file_path)
        
        if df is None:
            raise Exception("Could not read file")
        
        print(f"   Total rows: {len(df):,}")
        
        # Check if key columns exist
        missing_cols = [col for col in key_columns if col not in df.columns]
        if missing_cols:
            print(f"   ❌ Missing columns: {missing_cols}")
            return False
        
        # Check uniqueness
        key_subset = df[key_columns].dropna()
        unique_combinations = key_subset.drop_duplicates()
        
        print(f"   Non-null key combinations: {len(key_subset):,}")
        print(f"   Unique key combinations: {len(unique_combinations):,}")
        print(f"   Duplicates: {len(key_subset) - len(unique_combinations):,}")
        
        if len(key_subset) == len(unique_combinations):
            print(f"   ✅ KEY IS UNIQUE - Perfect candidate!")
            return True
        else:
            duplicate_rate = ((len(key_subset) - len(unique_combinations)) / len(key_subset)) * 100
            print(f"   ❌ KEY HAS DUPLICATES - {duplicate_rate:.2f}% duplicate rate")
            
            # Show some duplicate examples
            duplicates = key_subset[key_subset.duplicated(keep=False)].sort_values(key_columns)
            if len(duplicates) > 0:
                print(f"   📋 Sample duplicates (first 5):")
                for i, (_, row) in enumerate(duplicates.head(10).iterrows()):
                    if i % 2 == 0:  # Show pairs
                        print(f"      {dict(row)}")
            return False
            
    except Exception as e:
        print(f"   ❌ Error validating key: {str(e)}")
        return False

def test_composite_keys():
    """Test various key combinations for uniqueness"""
    print("🔑 KEY UNIQUENESS VALIDATION - INVENTORY BALANCE")
    print("="*60)
    
    left_file = "data/raw/netsuite_inventory_balance (1).csv"
    right_file = "data/raw/qa2_netsuite_inventory_balance.xlsx"
    
    # Test key candidates
    key_candidates = [
        ['serial_number_id'],
        ['item_id', 'location_id'],
        ['item_id', 'location_id', 'status'],
        ['serial_number_id', 'item_id'],
        ['serial_number_id', 'location_id']
    ]
    
    print("\n📊 TESTING KEY CANDIDATES:")
    
    results = []
    for keys in key_candidates:
        print(f"\n{'='*50}")
        print(f"🧪 TESTING KEY: {keys}")
        
        left_valid = validate_key_uniqueness(left_file, 'csv', keys, 'LEFT')
        right_valid = validate_key_uniqueness(right_file, 'excel', keys, 'RIGHT')
        
        if left_valid and right_valid:
            results.append((keys, 'PERFECT'))
            print(f"   🎯 RESULT: ✅ PERFECT KEY - Unique in both datasets!")
        elif left_valid or right_valid:
            results.append((keys, 'PARTIAL'))
            print(f"   🔶 RESULT: ⚠️ PARTIAL - Unique in one dataset only")
        else:
            results.append((keys, 'FAILED'))
            print(f"   ❌ RESULT: ❌ FAILED - Duplicates in both datasets")
    
    print(f"\n🏆 FINAL RECOMMENDATIONS:")
    print(f"="*50)
    
    perfect_keys = [k for k, status in results if status == 'PERFECT']
    partial_keys = [k for k, status in results if status == 'PARTIAL']
    
    if perfect_keys:
        print(f"✅ PERFECT KEYS (use these):")
        for key in perfect_keys:
            print(f"   - {key}")
    
    if partial_keys:
        print(f"⚠️ PARTIAL KEYS (investigate further):")
        for key in partial_keys:
            print(f"   - {key}")
    
    if not perfect_keys and not partial_keys:
        print(f"❌ NO VIABLE KEYS FOUND - Need alternative approach")

if __name__ == "__main__":
    test_composite_keys()