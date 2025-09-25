#!/usr/bin/env python3
"""Test script to debug NetSuite comparison issue."""

import duckdb
from pathlib import Path
from src.pipeline.stager import DataStager
from src.core.comparator import DataComparator

# Create connection
con = duckdb.connect()
stager = DataStager()

# Create configs for NetSuite files
class SimpleConfig:
    def __init__(self, name, path):
        self.name = name
        self.path = path
        self.custom_sql = None
        self.normalizers = None
        self.converters = None
        self.column_map = {}  # No column mapping initially

# Stage both datasets
print("Staging netsuite_messages...")
left_config = SimpleConfig('netsuite_messages', 'data/raw/netsuite_messages (1).csv')
left_table = stager.stage_dataset(con, left_config, force_restage=True)

print("Staging qa2_netsuite_messages...")
right_config = SimpleConfig('qa2_netsuite_messages', 'data/raw/qa2_netsuite_messages.xlsx')
right_table = stager.stage_dataset(con, right_config, force_restage=True)

# Check columns in both tables
print("\nColumns in staged tables:")
for table in [left_table, right_table]:
    cols = con.execute(f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = '{table}'
        ORDER BY column_name
    """).fetchall()
    
    print(f"\n{table}:")
    print(f"  First 5 columns: {[c[0] for c in cols[:5]]}")
    print(f"  Total columns: {len(cols)}")
    
    # Check if internal_id exists
    col_names = [c[0] for c in cols]
    has_internal_id = 'internal_id' in col_names
    has_message_id = 'message_id' in col_names
    print(f"  Has internal_id: {has_internal_id}")
    print(f"  Has message_id: {has_message_id}")

print("\nDone!")