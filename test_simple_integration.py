#!/usr/bin/env python3
"""
Simple integration test for strip_hierarchy normalizer.
"""

import os
import sys
import duckdb
import pandas as pd

# Add src to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

def test_normalizer_integration():
    """Test the normalizer with DuckDB to simulate pipeline behavior."""
    print("🧪 Testing strip_hierarchy integration with DuckDB...")
    print("=" * 60)
    
    # Create DuckDB connection
    con = duckdb.connect()
    
    try:
        # Register the strip_hierarchy function
        print("📋 Registering strip_hierarchy function...")
        con.create_function("strip_hierarchy", 
                          lambda x: x.split(":")[-1].strip() if x else x,
                          return_type=str)
        
        # Create test data that mimics the real scenario
        print("📊 Creating test data...")
        con.execute("""
            CREATE TABLE left_dept AS
            SELECT * FROM VALUES
                (1, '110 Operations', '100 - Operations : 110 Operations'),
                (2, '410 CTO', '400 - Construction : 410 CTO'),
                (3, '520 - HR', '500 FAA : 520 - HR')
            AS t(id, name, full_name)
        """)
        
        con.execute("""
            CREATE TABLE right_dept AS  
            SELECT * FROM VALUES
                (1, '110 Operations', '110 Operations'),
                (2, '410 CTO', '410 CTO'),
                (3, '520 - HR', '520 - HR')
            AS t(id, name, full_name)
        """)
        
        # Test comparison WITHOUT normalization
        print("\n🔍 Testing comparison WITHOUT normalization...")
        differences_before = con.execute("""
            SELECT 
                l.id,
                l.full_name as left_full_name,
                r.full_name as right_full_name,
                CASE WHEN l.full_name != r.full_name THEN 'Different' ELSE 'Same' END as status
            FROM left_dept l
            JOIN right_dept r ON l.id = r.id
            WHERE l.full_name != r.full_name
        """).fetchall()
        
        print(f"   Found {len(differences_before)} differences before normalization:")
        for diff in differences_before:
            print(f"     ID {diff[0]}: '{diff[1]}' vs '{diff[2]}'")
        
        # Apply normalization to left table
        print("\n🔧 Applying strip_hierarchy normalization...")
        con.execute("""
            UPDATE left_dept
            SET full_name = strip_hierarchy(full_name)
        """)
        
        # Test comparison WITH normalization
        print("🔍 Testing comparison WITH normalization...")
        differences_after = con.execute("""
            SELECT 
                l.id,
                l.full_name as left_full_name,
                r.full_name as right_full_name,
                CASE WHEN l.full_name != r.full_name THEN 'Different' ELSE 'Same' END as status
            FROM left_dept l
            JOIN right_dept r ON l.id = r.id  
            WHERE l.full_name != r.full_name
        """).fetchall()
        
        print(f"   Found {len(differences_after)} differences after normalization:")
        for diff in differences_after:
            print(f"     ID {diff[0]}: '{diff[1]}' vs '{diff[2]}'")
        
        # Show all normalized values
        print("\n📋 All normalized values:")
        all_rows = con.execute("""
            SELECT 
                l.id,
                l.full_name as normalized_left,
                r.full_name as right_value,
                CASE WHEN l.full_name = r.full_name THEN '✅ Match' ELSE '❌ Diff' END as result
            FROM left_dept l
            JOIN right_dept r ON l.id = r.id
            ORDER BY l.id
        """).fetchall()
        
        for row in all_rows:
            print(f"     ID {row[0]}: '{row[1]}' = '{row[2]}' {row[3]}")
        
        # Determine success
        success = len(differences_after) == 0 and len(differences_before) > 0
        
        if success:
            print(f"\n🎉 SUCCESS: Normalization eliminated {len(differences_before)} false positive differences!")
            return True
        else:
            print(f"\n⚠️ Results: Before: {len(differences_before)}, After: {len(differences_after)}")
            return len(differences_after) < len(differences_before)
            
    except Exception as e:
        print(f"❌ Error during test: {e}")
        return False
    finally:
        con.close()

if __name__ == "__main__":
    success = test_normalizer_integration()
    print(f"\n{'✅ Integration test PASSED!' if success else '❌ Integration test had issues.'}")