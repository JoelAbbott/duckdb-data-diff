#!/usr/bin/env python3
"""
Interactive menu-driven data comparison tool.
Provides user-friendly interface for comparing datasets.
"""

import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

from src.ui.menu import MenuInterface


def main():
    """Main entry point for menu-driven interface."""
    try:
        print("🚀 DuckDB Data Comparison System")
        print("=" * 60)
        
        # Initialize menu interface
        menu = MenuInterface()
        
        # Check if files are available
        if not menu.available_files:
            print(f"\n❌ No data files found in {menu.data_dir}")
            print("Please add CSV, Excel, or Parquet files to the data/raw directory")
            return 1
        
        print(f"✅ Found {len(menu.available_files)} data files")
        
        # Run interactive menu
        success = menu.run_interactive_mode()
        
        return 0 if success else 1
        
    except KeyboardInterrupt:
        print("\n\n👋 Goodbye!")
        return 0
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())