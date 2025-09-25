#!/usr/bin/env python3
"""
Test with actual data files to see column mapping in action.
"""

import yaml
from pathlib import Path
from src.config.manager import ConfigManager

# Create a test config with column mapping
config = {
    'datasets': {
        'netsuite_messages_1': {
            'path': 'data/raw/netsuite_messages (1).csv',
            'type': 'csv',
            'column_map': {}
        },
        'qa2_netsuite_messages': {
            'path': 'data/raw/qa2_netsuite_messages.xlsx',
            'type': 'excel',
            'column_map': {
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
        }
    },
    'comparisons': [
        {
            'left': 'netsuite_messages_1',
            'right': 'qa2_netsuite_messages',
            'keys': ['internal_id']
        }
    ]
}

# Save config
config_path = Path('test_config_with_mapping.yaml')
with open(config_path, 'w') as f:
    yaml.dump(config, f, default_flow_style=False)

print(f"Created test config: {config_path}")

# Load and verify
manager = ConfigManager(config_path)
manager.load()

# Check the loaded config
right_dataset = manager.get_dataset('qa2_netsuite_messages')
print(f"\nLoaded column_map has {len(right_dataset.column_map)} mappings:")
for i, (right_col, left_col) in enumerate(right_dataset.column_map.items()):
    if i < 5:
        print(f"  {right_col} -> {left_col}")
if len(right_dataset.column_map) > 5:
    print(f"  ... and {len(right_dataset.column_map) - 5} more")

print("\nNow run: python main.py test_config_with_mapping.yaml")
print("This will use the column mappings for the comparison")