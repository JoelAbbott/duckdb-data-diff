#!/usr/bin/env python3
"""
Test column mapping with actual staged table column names.
This simulates what happens in the real pipeline.
"""

import duckdb
from src.core.comparator import DataComparator
from src.config.manager import DatasetConfig, ComparisonConfig

def test_with_staged_column_names():
    """Test column mapping with snake_case column names like in staged tables."""
    
    # Create connection
    con = duckdb.connect(':memory:')
    
    # Create tables with snake_case column names (like after staging)
    con.execute("""
        CREATE TABLE netsuite_messages_1 AS SELECT 
            1 as internal_id,
            'John' as from,
            'john@example.com' as from_email_address,
            'Hello' as subject,
            true as emailed,
            'Customer1' as entity,
            'msg' as type,
            'john@example.com' as email_address,
            'Yes' as bcc,
            true as has_attachments,
            false as is_incoming,
            '2024-01-01' as modification_date,
            '2024-01-01' as date_created,
            2 as internal_id_1,
            'Jane' as recipient,
            'admin@example.com' as cc
    """)
    
    con.execute("""
        CREATE TABLE qa2_netsuite_messages AS SELECT
            1 as message_id,
            'John' as author,
            'john@example.com' as author_email,
            'Hello' as email_subject,
            'true' as is_emailed,
            'Customer1' as vendor,
            'msg' as message_type,
            'john@example.com' as recipient_email,
            'Yes' as email_bcc,
            'true' as is_attachment_included,
            'false' as is_incoming,
            '2024-01-01' as last_modified_date,
            '2024-01-01' as message_date,
            2 as transaction_id,
            'Jane' as recipient,
            'admin@example.com' as email_cc,
            false as deleted,
            '2024-01-01' as last_seen,
            '2024-01-01' as created_at,
            '2024-01-01' as updated_at
    """)
    
    # Create dataset configs with column mapping (right dataset maps to left)
    left_config = DatasetConfig(
        name="netsuite_messages_1",
        path="test",
        column_map={}  # No mapping on left
    )
    
    # This is the critical mapping - right columns map to left columns
    right_config = DatasetConfig(
        name="qa2_netsuite_messages", 
        path="test",
        column_map={
            'message_id': 'internal_id',
            'author': 'from',
            'author_email': 'from_email_address',
            'email_subject': 'subject',
            'is_emailed': 'emailed',
            'vendor': 'entity',
            'message_type': 'type',
            'recipient_email': 'email_address',
            'email_bcc': 'bcc',
            'is_attachment_included': 'has_attachments',
            'is_incoming': 'is_incoming',
            'last_modified_date': 'modification_date',
            'message_date': 'date_created',
            'transaction_id': 'internal_id_1',
            'recipient': 'recipient',
            'email_cc': 'cc'
        }
    )
    
    # Create comparison config
    comp_config = ComparisonConfig(
        left_dataset="netsuite_messages_1",
        right_dataset="qa2_netsuite_messages",
        comparison_keys=['internal_id']
    )
    
    # Create comparator and run comparison
    comparator = DataComparator(con)
    comparator.left_dataset_config = left_config
    comparator.right_dataset_config = right_config
    
    # First, let's trace what columns exist
    left_cols = con.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'netsuite_messages_1' ORDER BY ordinal_position").fetchall()
    right_cols = con.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'qa2_netsuite_messages' ORDER BY ordinal_position").fetchall()
    
    print("Left table columns:")
    for col in left_cols:
        print(f"  {col[0]}")
    
    print("\nRight table columns:")
    for col in right_cols:
        print(f"  {col[0]}")
    
    print("\nColumn mapping (right -> left):")
    for right_col, left_col in right_config.column_map.items():
        print(f"  {right_col} -> {left_col}")
    
    # Now run the comparison
    result = comparator.compare(
        "netsuite_messages_1", 
        "qa2_netsuite_messages",
        comp_config,
        left_config,
        right_config
    )
    
    print("\n=== COMPARISON RESULTS ===")
    print(f"Key columns: {result.key_columns}")
    print(f"Value columns compared: {len(result.columns_compared)}")
    print(f"Columns: {sorted(result.columns_compared)}")
    print(f"Matched rows: {result.matched_rows}")
    print(f"Value differences: {result.value_differences}")
    
    # Expected columns (all left columns except the key)
    expected_value_columns = [
        'from', 'from_email_address', 'subject', 'emailed', 'entity',
        'type', 'email_address', 'bcc', 'has_attachments', 'is_incoming',
        'modification_date', 'date_created', 'internal_id_1', 'recipient', 'cc'
    ]
    
    actual_value_columns = sorted(result.columns_compared)
    expected_value_columns_sorted = sorted(expected_value_columns)
    
    print(f"\nExpected {len(expected_value_columns_sorted)} columns to be compared")
    print(f"Actually compared {len(actual_value_columns)} columns")
    
    if actual_value_columns == expected_value_columns_sorted:
        print("\n✅ SUCCESS: All mapped columns are being compared!")
        return True
    else:
        print(f"\n❌ FAILURE: Column mismatch!")
        print(f"\nExpected columns not being compared:")
        for col in expected_value_columns_sorted:
            if col not in actual_value_columns:
                print(f"  - {col}")
        print(f"\nUnexpected columns being compared:")
        for col in actual_value_columns:
            if col not in expected_value_columns_sorted:
                print(f"  + {col}")
        return False

if __name__ == "__main__":
    success = test_with_staged_column_names()
    exit(0 if success else 1)