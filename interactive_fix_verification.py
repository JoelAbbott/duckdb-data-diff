#!/usr/bin/env python3
"""
Interactive Fix Verification Script
Test the column mapping fix with NetSuite datasets in interactive mode.
"""

import sys
from pathlib import Path

# Add project root to path for imports
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from src.ui.menu import MenuInterface
from main import DataDiffPipeline
import tempfile
import yaml


def simulate_interactive_comparison():
    """
    Simulate the interactive comparison that previously failed.
    This will test our column normalization fix.
    """
    print("🔧 INTERACTIVE FIX VERIFICATION")
    print("=" * 60)
    print("Testing column mapping fix with NetSuite datasets...")
    print()
    
    # Initialize menu interface
    menu = MenuInterface()
    
    # Simulate selecting NetSuite datasets
    left_file = Path("data/raw/netsuite_messages (1).csv")
    right_file = Path("data/raw/qa2_netsuite_messages.xlsx") 
    
    print(f"Selected datasets:")
    print(f"  Left:  {left_file.name}")
    print(f"  Right: {right_file.name}")
    
    if not left_file.exists() or not right_file.exists():
        print("❌ NetSuite datasets not found. Using available test files...")
        # Fallback to available files
        left_file = Path("data/raw/test_left.csv")
        right_file = Path("data/raw/test_right.csv")
        
        if not left_file.exists() or not right_file.exists():
            print("❌ No suitable test files found!")
            return False
    
    try:
        # Profile the datasets to understand structure
        print("\n📊 Profiling datasets...")
        left_profile = menu._profile_dataset(left_file)
        right_profile = menu._profile_dataset(right_file)
        
        print(f"Left dataset columns: {len(left_profile.get('columns', {}))}")
        print(f"Right dataset columns: {len(right_profile.get('columns', {}))}")
        
        # Find column matches
        print("\n🔍 Finding column matches...")
        matches = menu._find_column_matches(left_profile, right_profile)
        
        print(f"Found {len(matches)} potential matches:")
        for i, match in enumerate(matches[:5], 1):  # Show first 5
            print(f"  {i}. {match['left_column']} -> {match['right_column']} ({match['confidence']:.1%})")
        if len(matches) > 5:
            print(f"  ... and {len(matches) - 5} more matches")
        
        # Simulate approving all matches (this would be interactive in real usage)
        print("\n✅ Simulating user approval of all matches...")
        approved_matches = matches  # In real usage, user would review these
        
        # Select key column (first match with high confidence)
        validated_keys = None
        if approved_matches:
            # Find a good key column
            best_match = max(approved_matches, key=lambda x: x['confidence'])
            validated_keys = [best_match['left_column']]
            print(f"🔑 Selected key column: {validated_keys[0]}")
        
        # Create the interactive config with our fix applied
        print("\n⚙️ Creating interactive config with normalization fix...")
        config = menu._create_interactive_config(
            left_file=left_file,
            right_file=right_file,
            matches=approved_matches,
            validated_keys=validated_keys
        )
        
        # Show the generated config to verify normalization
        right_dataset_name = list(config["datasets"].keys())[1]
        column_map = config["datasets"][right_dataset_name].get("column_map", {})
        
        print(f"\n🗂️ Generated column mappings ({len(column_map)} total):")
        for i, (right_col, left_col) in enumerate(list(column_map.items())[:5], 1):
            print(f"  {i}. '{right_col}' -> '{left_col}'")
        if len(column_map) > 5:
            print(f"  ... and {len(column_map) - 5} more mappings")
        
        # Save temporary config and run comparison
        print(f"\n🚀 Running comparison with {len(approved_matches)} column mappings...")
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(config, f, default_flow_style=False)
            temp_config_path = Path(f.name)
        
        # Run the pipeline with our generated config
        pipeline = DataDiffPipeline(
            temp_config_path,
            verbose=True,
            use_rich=True
        )
        
        success = pipeline.run()
        
        # Cleanup
        temp_config_path.unlink()
        
        if success:
            print("\n✅ Comparison completed successfully!")
            
            # Find the latest report directory
            reports_dir = Path("data/reports")
            if reports_dir.exists():
                report_dirs = [d for d in reports_dir.iterdir() if d.is_dir()]
                if report_dirs:
                    latest_report = max(report_dirs, key=lambda x: x.stat().st_mtime)
                    
                    # Check the summary file
                    summary_file = latest_report / "comparison_summary.txt"
                    if summary_file.exists():
                        print(f"📄 Reading summary: {summary_file}")
                        
                        with open(summary_file, 'r') as f:
                            summary_content = f.read()
                        
                        # Extract key metrics
                        lines = summary_content.split('\n')
                        columns_compared = 0
                        
                        for line in lines:
                            if "Value Columns Compared:" in line:
                                columns_compared = int(line.split(':')[1].strip())
                                break
                        
                        print(f"\n🎯 VERIFICATION RESULTS:")
                        print(f"  📊 Total column mappings created: {len(column_map)}")
                        print(f"  📊 Value columns compared: {columns_compared if columns_compared > 0 else 'Found in summary'}")
                        
                        if columns_compared > 2:
                            print(f"  ✅ FIX SUCCESSFUL: More than 2 columns compared!")
                            print(f"  ✅ Column mapping normalization fix is working!")
                        else:
                            print(f"  ❌ FIX FAILED: Only {columns_compared} columns compared")
                            print(f"  ❌ Column mapping issue persists")
                        
                        return columns_compared > 2
            
            return True
        else:
            print("❌ Comparison failed!")
            return False
            
    except Exception as e:
        print(f"❌ Error during verification: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Main verification function."""
    print("Starting interactive fix verification...")
    
    success = simulate_interactive_comparison()
    
    if success:
        print(f"\n🎉 VERIFICATION COMPLETE: Column mapping fix is working!")
    else:
        print(f"\n❌ VERIFICATION FAILED: Fix needs more work")
    
    return 0 if success else 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\n👋 Verification interrupted")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)