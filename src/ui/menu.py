"""
Interactive menu interface for data comparison.
Single responsibility: Handle user interaction and file selection.
"""

import os
import sys
from pathlib import Path
from typing import List, Tuple, Optional, Dict, Any
import duckdb
# Imports removed - will import dynamically when needed
from ..utils.normalizers import normalize_column_name


class MenuInterface:
    """
    Interactive terminal menu for data comparison.
    """
    
    def __init__(self, data_dir: Path = None):
        """
        Initialize menu interface.
        
        Args:
            data_dir: Directory containing data files (default: data/raw)
        """
        if data_dir is None:
            data_dir = Path("data/raw")
        self.data_dir = Path(data_dir)
        self.available_files = self._scan_data_files()
    
    def _scan_data_files(self) -> List[Path]:
        """
        Scan data directory for supported files.
        
        Returns:
            List of available data files
        """
        if not self.data_dir.exists():
            return []
        
        supported_extensions = {'.csv', '.xlsx', '.xls', '.parquet'}
        files = []
        
        for file_path in self.data_dir.iterdir():
            if file_path.is_file() and file_path.suffix.lower() in supported_extensions:
                files.append(file_path)
        
        return sorted(files, key=lambda x: x.name.lower())
    
    def show_main_menu(self) -> str:
        """
        Show main menu and get user choice.
        
        Returns:
            User's menu choice
        """
        print("\n" + "="*60)
        print("       DUCKDB DATA COMPARISON SYSTEM")
        print("="*60)
        print("1. Quick Comparison (Auto-match columns)")
        print("2. Interactive Comparison (Review matches)")
        print("3. View Available Files")
        print("4. Exit")
        print("="*60)
        
        while True:
            try:
                choice = input("\nSelect option (1-4): ").strip()
                if choice in ['1', '2', '3', '4']:
                    return choice
                else:
                    print("Please enter 1, 2, 3, or 4")
            except (KeyboardInterrupt, EOFError):
                print("\nExiting...")
                return '4'
    
    def show_file_list(self) -> None:
        """Display available files in data directory."""
        if not self.available_files:
            print(f"\nNo data files found in {self.data_dir}")
            print("Supported formats: CSV, Excel (.xlsx/.xls), Parquet")
            return
        
        print(f"\nAvailable files in {self.data_dir}:")
        print("-" * 50)
        for i, file_path in enumerate(self.available_files, 1):
            file_size = self._get_file_size(file_path)
            print(f"{i:2d}. {file_path.name:<35} ({file_size})")
    
    def select_files(self) -> Optional[Tuple[Path, Path]]:
        """
        Let user select two files for comparison.
        
        Returns:
            Tuple of (left_file, right_file) or None if cancelled
        """
        if len(self.available_files) < 2:
            print("\nNeed at least 2 files for comparison.")
            print(f"Found {len(self.available_files)} files in {self.data_dir}")
            return None
        
        print("\n" + "="*60)
        print("SELECT FILES FOR COMPARISON")
        print("="*60)
        
        self.show_file_list()
        
        # Select left dataset
        left_file = self._select_single_file("left dataset")
        if not left_file:
            return None
        
        # Select right dataset
        print(f"\nSelected left dataset: {left_file.name}")
        print("\nSelect right dataset (excluding the one already chosen):")
        right_file = self._select_single_file("right dataset", exclude=left_file)
        if not right_file:
            return None
        
        print(f"\nComparison selected:")
        print(f"  Left:  {left_file.name}")
        print(f"  Right: {right_file.name}")
        
        # Confirm selection
        confirm = input("\nProceed with these files? (y/n): ").strip().lower()
        if confirm in ['y', 'yes']:
            return left_file, right_file
        
        return None
    
    def _select_single_file(self, purpose: str, exclude: Path = None) -> Optional[Path]:
        """
        Select a single file from the list.
        
        Args:
            purpose: Description of what file is for
            exclude: File to exclude from selection
            
        Returns:
            Selected file path or None
        """
        if not self.available_files:
            print(f"No files available for {purpose}")
            return None
        
        # Only show files if we have exclusions (avoid double display)
        if exclude:
            print("-" * 50)
            for i, file_path in enumerate(self.available_files, 1):
                file_size = self._get_file_size(file_path)
                if file_path == exclude:
                    print(f"{i:2d}. {file_path.name:<35} ({file_size}) [EXCLUDED]")
                else:
                    print(f"{i:2d}. {file_path.name:<35} ({file_size})")
        
        while True:
            try:
                choice = input(f"\nSelect {purpose}: ").strip()
                
                if choice == '0':
                    return None
                
                choice_num = int(choice)
                if 1 <= choice_num <= len(self.available_files):
                    selected_file = self.available_files[choice_num - 1]
                    # Check if this file is excluded
                    if exclude and selected_file == exclude:
                        print(f"File {choice_num} is excluded. Please choose a different file.")
                        continue
                    return selected_file
                else:
                    print(f"Please enter a number between 1 and {len(self.available_files)}")
                    
            except ValueError:
                print("Please enter a valid number")
            except (KeyboardInterrupt, EOFError):
                print("\nCancelled")
                return None
    
    def _sanitize_table_name(self, name: str) -> str:
        """
        Sanitize filename for use as SQL table name.
        
        Args:
            name: Original filename (without extension)
            
        Returns:
            Valid SQL identifier
        """
        # Replace problematic characters (CLAUDE.md: fail fast with clear rules)
        sanitized = name.replace(' ', '_')
        sanitized = sanitized.replace('-', '_')
        sanitized = sanitized.replace('(', '_')
        sanitized = sanitized.replace(')', '_')
        sanitized = sanitized.replace('.', '_')
        sanitized = sanitized.replace('/', '_')
        sanitized = sanitized.replace('\\', '_')
        
        # Remove multiple underscores
        while '__' in sanitized:
            sanitized = sanitized.replace('__', '_')
        
        # Remove leading/trailing underscores
        sanitized = sanitized.strip('_')
        
        # Ensure it starts with letter (SQL requirement)
        if sanitized and not sanitized[0].isalpha():
            sanitized = 't_' + sanitized
        
        # Handle empty result
        if not sanitized:
            sanitized = 'table_unnamed'
        
        return sanitized
    
    def _profile_dataset(self, file_path: Path) -> Dict[str, Any]:
        """
        Profile a dataset to understand its structure.
        
        Args:
            file_path: Path to dataset file
            
        Returns:
            Profile dictionary with column info
        """
        try:
            # Read more rows to ensure sparse columns are detected (CLAUDE.md: BUG 2 fix)
            if file_path.suffix.lower() == '.csv':
                import pandas as pd
                df = pd.read_csv(file_path, nrows=5000)
            else:
                import pandas as pd
                df = pd.read_excel(file_path, nrows=5000)
            
            profile = {
                'file_name': file_path.name,
                'row_count': len(df),
                'columns': {}
            }
            
            for col in df.columns:
                profile['columns'][col] = {
                    'dtype': str(df[col].dtype),
                    'non_null_count': df[col].count(),
                    'unique_count': df[col].nunique(),
                    'sample_values': df[col].dropna().head(3).tolist()
                }
            
            return profile
            
        except Exception as e:
            print(f"⚠️ Warning: Could not profile {file_path.name}: {e}")
            return {'file_name': file_path.name, 'columns': {}}
    
    def _find_column_matches(self, left_profile: Dict, right_profile: Dict) -> List[Dict]:
        """
        Find potential column matches between datasets.
        
        Args:
            left_profile: Left dataset profile
            right_profile: Right dataset profile
            
        Returns:
            List of match dictionaries
        """
        matches = []
        left_cols = left_profile.get('columns', {})
        right_cols = right_profile.get('columns', {})
        
        for left_col, left_info in left_cols.items():
            best_match = None
            best_confidence = 0.0
            
            for right_col, right_info in right_cols.items():
                confidence = self._calculate_match_confidence(
                    left_col, left_info, right_col, right_info
                )
                
                if confidence > best_confidence:
                    best_confidence = confidence
                    best_match = right_col
            
            if best_match and best_confidence > 0.3:  # Minimum confidence threshold
                matches.append({
                    'left_column': left_col,
                    'right_column': best_match,
                    'confidence': best_confidence,
                    'match_reason': self._get_match_reason(best_confidence)
                })
        
        return sorted(matches, key=lambda x: x['confidence'], reverse=True)
    
    def create_comparison_config(self, left_file: Path, right_file: Path) -> Dict[str, Any]:
        """
        Create comparison configuration from selected files.
        
        Args:
            left_file: Left dataset file
            right_file: Right dataset file
            
        Returns:
            Configuration dictionary
        """
        # Generate dataset names (CLAUDE.md: single responsibility function)
        left_name = self._sanitize_table_name(left_file.stem)
        right_name = self._sanitize_table_name(right_file.stem)
        
        # Create configuration
        config = {
            "datasets": {
                left_name: {
                    "path": str(left_file),
                    "type": self._get_file_type(left_file),
                    "key_columns": [],  # Will be auto-detected
                    "exclude_columns": []
                },
                right_name: {
                    "path": str(right_file), 
                    "type": self._get_file_type(right_file),
                    "key_columns": [],  # Will be auto-detected
                    "exclude_columns": []
                }
            },
            "comparisons": [{
                "left": left_name,
                "right": right_name,
                "keys": [],  # Will be auto-detected
                "columns": [],  # Empty means compare all
                "tolerance": 0.01,
                "ignore_case": True,
                "ignore_spaces": True,
                "output_format": "excel",
                "max_differences": 1000
            }]
        }
        
        return config
    
    def _get_file_type(self, file_path: Path) -> str:
        """
        Determine file type from extension.
        
        Args:
            file_path: Path to file
            
        Returns:
            File type string
        """
        suffix = file_path.suffix.lower()
        if suffix == '.csv':
            return 'csv'
        elif suffix in ['.xlsx', '.xls']:
            return 'excel'
        elif suffix == '.parquet':
            return 'parquet'
        else:
            return 'csv'  # Default fallback
    
    def _get_file_size(self, file_path: Path) -> str:
        """
        Get human-readable file size.
        
        Args:
            file_path: Path to file
            
        Returns:
            Formatted file size
        """
        try:
            size_bytes = file_path.stat().st_size
            
            if size_bytes < 1024:
                return f"{size_bytes} B"
            elif size_bytes < 1024**2:
                return f"{size_bytes/1024:.1f} KB"
            elif size_bytes < 1024**3:
                return f"{size_bytes/(1024**2):.1f} MB"
            else:
                return f"{size_bytes/(1024**3):.1f} GB"
        except:
            return "Unknown"
    
    def run_interactive_mode(self) -> bool:
        """
        Run the interactive menu system.
        
        Returns:
            True if comparison was run, False if cancelled
        """
        while True:
            choice = self.show_main_menu()
            
            if choice == '1':  # Quick comparison
                return self._run_quick_comparison()
            elif choice == '2':  # Interactive comparison
                return self._run_interactive_comparison()
            elif choice == '3':  # View files
                self.show_file_list()
                input("\nPress Enter to continue...")
            elif choice == '4':  # Exit
                print("Goodbye!")
                return False
    
    def _run_quick_comparison(self) -> bool:
        """Run quick comparison with auto-matching."""
        print("\n" + "="*60)
        print("QUICK COMPARISON MODE")
        print("="*60)
        print("This mode will:")
        print("• Auto-detect key columns")
        print("• Auto-match column names")
        print("• Use default comparison settings")
        
        files = self.select_files()
        if not files:
            return False
        
        config = self.create_comparison_config(*files)
        return self._execute_comparison(config)
    
    def _run_interactive_comparison(self) -> bool:
        """Run interactive comparison with user review."""
        print("\n" + "="*60)
        print("INTERACTIVE COMPARISON MODE")
        print("="*60)
        print("This mode will:")
        print("• Let you review column mappings")
        print("• Configure comparison options") 
        print("• Preview data before comparing")
        
        files = self.select_files()
        if not files:
            return False
        
        left_file, right_file = files
        
        try:
            # Profile datasets (CLAUDE.md: single responsibility)
            print(f"\n📊 Profiling datasets...")
            left_profile = self._profile_dataset(left_file)
            right_profile = self._profile_dataset(right_file)
            
            # Generate column matches
            print("🔍 Finding column matches...")
            matches = self._find_column_matches(left_profile, right_profile)
            
            # Interactive review
            print(f"\n📋 Found {len(matches)} potential column matches")
            reviewed_matches = self._review_matches_interactively(matches, right_profile, left_profile)
            
            # Check if user cancelled the review
            if reviewed_matches is None:
                print("Comparison cancelled.")
                return False
            
            # CLAUDE.md Step 3: Interactive key selection and validation
            print("\n🔑 Validating key selection...")
            validated_keys = self._select_and_validate_keys(
                left_file, right_file, reviewed_matches
            )
            
            # Create enhanced config with user choices and validated keys
            config = self._create_interactive_config(left_file, right_file, reviewed_matches, validated_keys)
            
            return self._execute_comparison(config)
            
        except Exception as e:
            print(f"\n❌ Error during interactive comparison: {e}")
            return False
    
    def _calculate_match_confidence(self, left_col: str, left_info: Dict,
                                  right_col: str, right_info: Dict) -> float:
        """Calculate confidence score for column match (CLAUDE.md: comprehensive matching)."""
        confidence = 0.0
        left_lower = left_col.lower().replace(' ', '_').replace('_', '')
        right_lower = right_col.lower().replace(' ', '_').replace('_', '')
        
        # Exact normalized match (highest confidence)
        if left_lower == right_lower:
            confidence += 0.95
        elif left_col.lower() == right_col.lower():
            confidence += 0.9
        # Partial matches with better patterns
        elif left_lower in right_lower or right_lower in left_lower:
            confidence += 0.8
        elif left_col.lower() in right_col.lower() or right_col.lower() in left_col.lower():
            confidence += 0.7
        
        # Semantic matching patterns
        semantic_matches = {
            'from': ['author'],
            'fromemail': ['authoremail'],
            'emailaddress': ['authoremail', 'recipientemail'],
            'hasattachments': ['isattachmentincluded'],
            'internalid': ['messageid', 'transactionid'],
            'isincoming': ['incoming'],
            'modificationdate': ['lastmodifieddate'],
            'datecreated': ['messagedate', 'createdat'],
            'subject': ['emailsubject'],
            'recipient': ['recipientemail']
        }
        
        for pattern, targets in semantic_matches.items():
            if pattern in left_lower:
                for target in targets:
                    if target in right_lower:
                        confidence += 0.85
                        break
            elif pattern in right_lower:
                for target in targets:
                    if target in left_lower:
                        confidence += 0.85
                        break
        
        # Data type similarity
        if left_info.get('dtype') == right_info.get('dtype'):
            confidence += 0.2
        
        return min(confidence, 1.0)
    
    def _get_match_reason(self, confidence: float) -> str:
        """Get human-readable match reason."""
        if confidence >= 0.9:
            return "exact_name_match"
        elif confidence >= 0.7:
            return "partial_name_match + data_type"
        else:
            return "data_type_similarity"
    
    def _review_matches_interactively(self, matches: List[Dict], right_profile: Dict, left_profile: Dict) -> List[Dict]:
        """Let user review and modify column matches (CLAUDE.md: clear user interaction)."""
        if not matches:
            print("❌ No column matches found automatically.")
            matches = []  # Start with empty list
        
        print("\n" + "="*60)
        print("COLUMN MATCH REVIEW")
        print("="*60)
        
        approved_matches = []
        
        # Review automatic matches first
        for i, match in enumerate(matches, 1):
            print(f"\nMatch {i}/{len(matches)}:")
            print(f"  Left column:  {match['left_column']}")
            print(f"  Right column: {match['right_column']}")
            print(f"  Confidence:   {match['confidence']:.1%}")
            print(f"  Reason:       {match['match_reason']}")
            
            while True:
                try:
                    choice = input("\n  [Enter] Accept  [s] Skip  [m] Manual  [q] Quit: ").strip().lower()
                    
                    if choice in ['', 'enter']:
                        # Check for existing mapping to this right column and resolve conflicts
                        should_add_mapping = self._handle_conflicting_mapping(approved_matches, match['right_column'], 
                                                       match['left_column'], match['confidence'])
                        if should_add_mapping:
                            approved_matches.append(match)
                            print("  ✅ Accepted")
                        else:
                            print("  ❌ Rejected (conflict with existing mapping)")
                        break
                    elif choice == 's':
                        print("  ⏭️ Skipped")
                        break
                    elif choice == 'm':
                        # Manual column selection  
                        right_cols = list(right_profile.get('columns', {}).keys())
                        manual_match = self._manual_column_selection(match['left_column'], right_cols)
                        if manual_match:
                            # Fix: Check for existing mapping to this right column and remove it
                            # Manual selections always override existing mappings
                            self._handle_conflicting_mapping(approved_matches, manual_match, match['left_column'], 1.0)
                            
                            approved_matches.append({
                                'left_column': match['left_column'],
                                'right_column': manual_match,
                                'confidence': 1.0,  # User confirmed
                                'match_reason': 'manual_selection'
                            })
                            print(f"  ✅ Manually mapped to: {manual_match}")
                        else:
                            print("  ⏭️ Manual selection cancelled")
                        break
                    elif choice == 'q':
                        print(f"\n📋 Review cancelled by user.")
                        return None  # Signal that user wants to quit
                    else:
                        print("  Please press Enter, 's', 'm', or 'q'")
                        
                except (KeyboardInterrupt, EOFError):
                    print(f"\n📋 Review cancelled by user.")
                    return None  # Signal that user wants to quit
        
        # Now offer to manually map unmatched left columns
        matched_left_columns = {m['left_column'] for m in approved_matches}
        all_left_columns = set(left_profile.get('columns', {}).keys())
        unmatched_left = all_left_columns - matched_left_columns
        
        if unmatched_left:
            print(f"\n" + "="*60)
            print("UNMATCHED COLUMNS")
            print("="*60)
            print(f"Found {len(unmatched_left)} unmatched left columns.")
            print("Would you like to manually map any of them?")
            
            for left_col in sorted(unmatched_left):
                print(f"\nUnmatched left column: {left_col}")
                
                while True:
                    try:
                        choice = input("  [Enter] Skip  [m] Manual map  [q] Quit: ").strip().lower()
                        
                        if choice in ['', 'enter']:
                            print("  ⏭️ Skipped")
                            break
                        elif choice == 'm':
                            right_cols = list(right_profile.get('columns', {}).keys())
                            manual_match = self._manual_column_selection(left_col, right_cols)
                            if manual_match:
                                # Fix: Check for existing mapping to this right column and remove it
                                # Manual selections always override existing mappings
                                self._handle_conflicting_mapping(approved_matches, manual_match, left_col, 1.0)
                                
                                approved_matches.append({
                                    'left_column': left_col,
                                    'right_column': manual_match,
                                    'confidence': 1.0,  # User confirmed
                                    'match_reason': 'manual_selection'
                                })
                                print(f"  ✅ Manually mapped to: {manual_match}")
                            else:
                                print("  ⏭️ Manual selection cancelled")
                            break
                        elif choice == 'q':
                            print(f"\n📋 Review cancelled by user.")
                            return None  # Signal that user wants to quit
                        else:
                            print("  Please press Enter, 'm', or 'q'")
                            
                    except (KeyboardInterrupt, EOFError):
                        print(f"\n📋 Review cancelled by user.")
                        return None  # Signal that user wants to quit
        
        print(f"\n📋 Review complete. {len(approved_matches)} matches approved.")
        return approved_matches
    
    def _handle_conflicting_mapping(self, approved_matches: List[Dict], 
                                   right_column: str, new_left_column: str, 
                                   new_confidence: float = 1.0, 
                                   validated_keys: List[str] = None) -> bool:
        """
        Handle conflicts when multiple left columns map to the same right column.
        
        ARCHITECTURAL PATTERN: Key columns have absolute priority in conflict resolution.
        Any column present in validated_keys will always win conflicts regardless of confidence.
        
        Args:
            approved_matches: List of approved matches to check/modify
            right_column: Right column being mapped to
            new_left_column: Left column requesting this mapping
            new_confidence: Confidence score of the new mapping (default 1.0 for manual selections)
            validated_keys: List of validated key column names (original names, not normalized)
            
        Returns:
            True if the new mapping should be added, False if it should be rejected
        """
        # Find any existing mapping to this right column
        conflicting_match = None
        conflicting_index = None
        
        for i, match in enumerate(approved_matches):
            if match['right_column'] == right_column:
                conflicting_match = match
                conflicting_index = i
                break
        
        if conflicting_match:
            existing_confidence = conflicting_match.get('confidence', 0.0)
            existing_left_column = conflicting_match['left_column']
            
            print(f"  ⚠️  Conflict: '{existing_left_column}' is already mapped to '{right_column}'")
            print(f"      Existing confidence: {existing_confidence:.1f}, New confidence: {new_confidence:.1f}")
            
            # CRITICAL FIX: Key column priority logic
            # Check if either column is a validated key column
            new_is_key = validated_keys and new_left_column in validated_keys
            existing_is_key = validated_keys and existing_left_column in validated_keys
            
            if new_is_key and not existing_is_key:
                # New column is key, existing is non-key: KEY WINS (absolute priority)
                print(f"      🔑 KEY COLUMN PRIORITY: '{new_left_column}' is a key column")
                print(f"      Replacing non-key mapping '{existing_left_column}' -> '{right_column}' with key mapping")
                
                # Debug logging
                print(f"  DEBUG: Key column override - removing non-key match: {conflicting_match}")
                print(f"  DEBUG: Total approved matches before key override: {len(approved_matches)}")
                
                # Remove the conflicting non-key mapping
                approved_matches.pop(conflicting_index)
                
                print(f"  DEBUG: Total approved matches after key override: {len(approved_matches)}")
                return True  # Add the key mapping
                
            elif existing_is_key and not new_is_key:
                # Existing is key, new is non-key: PRESERVE KEY (reject non-key)
                print(f"      🔑 KEY COLUMN PROTECTION: '{existing_left_column}' is a key column")
                print(f"      Rejecting non-key mapping '{new_left_column}' -> '{right_column}' to protect key mapping")
                return False  # Reject the non-key mapping
                
            elif new_is_key and existing_is_key:
                # Both are keys: This shouldn't happen in normal use, but use confidence as tiebreaker
                print(f"      🔑 DUAL KEY CONFLICT: Both columns are keys, using confidence tiebreaker")
                if new_confidence >= existing_confidence:
                    approved_matches.pop(conflicting_index)
                    return True
                else:
                    return False
            else:
                # Neither is key: Use existing confidence-based logic (preserve existing functionality)
                if new_confidence > existing_confidence:
                    print(f"      Replacing with '{new_left_column}' -> '{right_column}' (higher confidence)")
                    
                    # Debug logging
                    print(f"  DEBUG: Confidence override - removing match: {conflicting_match}")
                    print(f"  DEBUG: Total approved matches before confidence override: {len(approved_matches)}")
                    
                    # Remove the conflicting mapping
                    approved_matches.pop(conflicting_index)
                    
                    print(f"  DEBUG: Total approved matches after confidence override: {len(approved_matches)}")
                    return True  # Add the new mapping
                else:
                    # Preserve existing mapping when confidence is equal or lower
                    print(f"      Keeping existing mapping '{existing_left_column}' -> '{right_column}' (equal/higher confidence)")
                    print(f"      Rejecting new mapping '{new_left_column}' -> '{right_column}'")
                    return False  # Reject the new mapping
        else:
            print(f"  DEBUG: No conflict found for '{new_left_column}' -> '{right_column}'")
            return True  # Add the new mapping
    
    def _manual_column_selection(self, left_column: str, right_columns: List[str]) -> Optional[str]:
        """
        Let user manually select matching column (CLAUDE.md: single responsibility).
        
        Args:
            left_column: Left column name
            right_columns: Available right columns
            
        Returns:
            Selected right column or None
        """
        print(f"\n🎯 Manual mapping for '{left_column}'")
        print("Available right columns:")
        print("-" * 40)
        
        for i, col in enumerate(right_columns, 1):
            print(f"{i:2d}. {col}")
        
        while True:
            try:
                choice = input(f"\nSelect column (1-{len(right_columns)}, 0 to cancel): ").strip()
                
                if choice == '0':
                    return None
                
                col_index = int(choice) - 1
                if 0 <= col_index < len(right_columns):
                    return right_columns[col_index]
                else:
                    print(f"Please enter a number between 1 and {len(right_columns)}")
                    
            except ValueError:
                print("Please enter a valid number")
            except (KeyboardInterrupt, EOFError):
                print("\nCancelled")
                return None
    
    def _select_and_validate_keys(self, left_file: Path, right_file: Path,
                                 reviewed_matches: List[Dict]) -> List[str]:
        """
        Select and validate comparison keys using KeySelector.
        
        Args:
            left_file: Left dataset file path
            right_file: Right dataset file path
            reviewed_matches: User-approved column matches
            
        Returns:
            List of validated key column names (original, not normalized)
        """
        try:
            # Import KeySelector dynamically to avoid circular imports
            from ..core.key_selector import KeySelector
            from ..core.key_validator import KeyValidator
            from ..utils.normalizers import normalize_column_name
            
            # Create in-memory DuckDB connection for key validation
            con = duckdb.connect(":memory:")
            
            # Initialize KeyValidator and KeySelector
            validator = KeyValidator(con)
            selector = KeySelector(con, validator)
            
            # Stage sample data for key validation with normalization
            left_table = self._stage_sample_data_for_validation(con, left_file, "left_table")
            right_table = self._stage_sample_data_for_validation(con, right_file, "right_table")
            
            # Create dataset configs with normalized column mappings
            # Convert original column names to normalized for the configs
            normalized_matches = []
            for match in reviewed_matches:
                normalized_matches.append({
                    'left_column': normalize_column_name(match['left_column']),
                    'right_column': normalize_column_name(match['right_column']),
                    'confidence': match.get('confidence', 1.0),
                    'original_left': match['left_column'],  # Keep original for display
                    'original_right': match['right_column']
                })
            
            left_config = self._create_mock_dataset_config(None)  # Left has no mapping
            right_config = self._create_mock_dataset_config(normalized_matches)
            
            # ARCHITECTURAL FIX: Present normalized column names that match staged tables
            # This prevents user from selecting column names that don't exist in staged data
            
            # CRITICAL REGRESSION FIX: Handle empty reviewed_matches to prevent infinite loop
            if not reviewed_matches:
                print("❌ No approved column matches found for key selection.")
                print("Suggestion: Review column mapping and ensure at least one column is approved.")
                # Return empty list to indicate no key could be selected
                return []
            
            # Build mapping from normalized names to original names for display purposes
            key_mapping = {}  # normalized_name -> original_name
            normalized_keys = []  # List of normalized names to present to user
            
            for match in reviewed_matches:
                original_name = match['left_column']
                normalized_name = normalize_column_name(original_name)
                key_mapping[normalized_name] = original_name
                normalized_keys.append(normalized_name)
            
            # ADDITIONAL SAFETY CHECK: Ensure we have at least one normalized key
            if not normalized_keys:
                print("❌ No valid key columns found after normalization.")
                print("Suggestion: Check column name normalization and ensure approved columns are valid.")
                return []
            
            # Present key selection with NORMALIZED column names (matching staged tables)
            print("\n" + "="*60)
            print("KEY COLUMN SELECTION")
            print("="*60)
            print("Select a key column for comparison:")
            print("(Showing normalized names that match staged data)")
            print("")
            
            for i, normalized_column in enumerate(normalized_keys, 1):
                original_column = key_mapping[normalized_column]
                # Show both normalized (for staging) and original (for reference)
                print(f"  {i:2}. {normalized_column} (from '{original_column}')")
            
            print("")
            
            # Get user selection
            while True:
                try:
                    user_input = input(f"Selection [1-{len(normalized_keys)}]: ").strip()
                    
                    if not user_input:
                        print("Please enter a number.")
                        continue
                    
                    choice_index = int(user_input) - 1
                    
                    if 0 <= choice_index < len(normalized_keys):
                        # User selected a normalized key name
                        selected_key_normalized = normalized_keys[choice_index]
                        selected_key_original = key_mapping[selected_key_normalized]
                        
                        print(f"\n✅ Selected key column: '{selected_key_normalized}' (original: '{selected_key_original}')")
                        
                        # Validate the normalized key (which matches staged table schema)
                        validation_result = validator.validate_key(
                            table_name=left_table,
                            key_columns=[selected_key_normalized],
                            dataset_config=left_config
                        )
                        
                        if validation_result.is_valid:
                            print(f"✅ Key validation successful")
                            # Return the ORIGINAL column name for display purposes and backward compatibility
                            return [selected_key_original]
                        else:
                            print(f"❌ Key validation failed: {validation_result.error_message}")
                            print("Please select a different key.")
                            continue
                    else:
                        print(f"Invalid selection. Please enter a number between 1 and {len(normalized_keys)}.")
                        continue
                        
                except ValueError:
                    print("Invalid input. Please enter a number.")
                    continue
                except (KeyboardInterrupt, EOFError):
                    print("\n❌ Key selection cancelled.")
                    # Fallback to first matched column as key
                    if reviewed_matches:
                        fallback_key = reviewed_matches[0]['left_column']
                        print(f"Using fallback key: {fallback_key}")
                        return [fallback_key]
                    return []
                
        except Exception as e:
            print(f"⚠️ Key validation error: {e}")
            # Fallback to first matched column as key
            if reviewed_matches:
                fallback_key = reviewed_matches[0]['left_column'] 
                print(f"Using fallback key: {fallback_key}")
                return [fallback_key]
            return []
    
    def _create_mock_dataset_config(self, matches: List[Dict]):
        """Create mock dataset config for key validation."""
        from types import SimpleNamespace
        
        if not matches:
            return SimpleNamespace(column_map=None)
        
        # Build column mapping from matches (right -> left mapping)
        column_map = {}
        for match in matches:
            column_map[match['right_column']] = match['left_column']
        
        return SimpleNamespace(column_map=column_map)
    
    def _stage_sample_data_for_validation(self, con: duckdb.DuckDBPyConnection,
                                        file_path: Path, table_name: str) -> str:
        """Stage sample data for key validation."""
        try:
            # Import normalizer to apply same normalization as staging
            from ..utils.normalizers import normalize_column_name
            
            # Read sample data (first 1000 rows for validation)
            if file_path.suffix.lower() == '.csv':
                # First create table with raw data
                sql = f"""
                    CREATE TABLE {table_name}_raw AS
                    SELECT * FROM read_csv_auto('{file_path}') LIMIT 1000
                """
                con.execute(sql)
                
                # Get columns and normalize them
                columns = con.execute(f"""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = '{table_name}_raw'
                """).fetchall()
                
                # Build rename statement to normalize columns
                renames = []
                for (col,) in columns:
                    normalized = normalize_column_name(col)
                    if normalized != col:
                        renames.append(f'"{col}" AS {normalized}')
                    else:
                        renames.append(f'"{col}"')
                
                # Create normalized table
                rename_sql = f"""
                    CREATE TABLE {table_name} AS
                    SELECT {', '.join(renames)}
                    FROM {table_name}_raw
                """
                con.execute(rename_sql)
                
                # Drop raw table
                con.execute(f"DROP TABLE {table_name}_raw")
                
            else:
                # For Excel files, read via pandas and register
                import pandas as pd
                df = pd.read_excel(file_path, nrows=1000)
                
                # Normalize column names
                df.columns = [normalize_column_name(col) for col in df.columns]
                
                con.register(table_name, df)
            
            return table_name
            
        except Exception as e:
            print(f"⚠️ Warning: Could not stage sample data for validation: {e}")
            # Create empty table as fallback
            con.execute(f"CREATE TABLE {table_name} (dummy_col VARCHAR)")
            return table_name
    
    def _create_interactive_config(self, left_file: Path, right_file: Path, 
                                  matches: List[Dict], validated_keys: List[str] = None) -> Dict[str, Any]:
        """Create enhanced config with user-approved matches."""
        config = self.create_comparison_config(left_file, right_file)
        
        if matches:
            # Add column mappings with normalized names for staging consistency
            # ARCHITECTURAL PATTERN: When building dataset configurations from interactive matches,
            # always normalize both sides of the column map to ensure consistency with staged data
            left_name = list(config["datasets"].keys())[0]
            right_name = list(config["datasets"].keys())[1]
            
            column_map = {}
            print(f"DEBUG: Creating config with {len(matches)} matches:")
            print(f"DEBUG: Validated keys for key priority: {validated_keys}")
            
            # Apply key column priority during mapping creation
            # First pass: Add all mappings, but track conflicts
            conflict_resolution_needed = {}
            
            for match in matches:
                # Normalize both column names to match staged table structure
                normalized_right = normalize_column_name(match['right_column'])
                normalized_left = normalize_column_name(match['left_column'])
                
                print(f"  DEBUG: Match - {match['left_column']} -> {match['right_column']} (confidence: {match['confidence']:.3f})")
                print(f"  DEBUG: Normalized mapping: {normalized_left} -> {normalized_right}")
                
                # Check if this right column already has a mapping (conflict detection)
                if normalized_right in column_map:
                    print(f"  DEBUG: CONFLICT DETECTED: {normalized_right} already mapped to {column_map[normalized_right]}")
                    
                    # Store conflict for resolution
                    if normalized_right not in conflict_resolution_needed:
                        conflict_resolution_needed[normalized_right] = []
                    
                    # Add existing mapping to conflict list if not already there
                    existing_left = column_map[normalized_right]
                    existing_original = None
                    for prev_match in matches:
                        if normalize_column_name(prev_match['left_column']) == existing_left:
                            existing_original = prev_match['left_column']
                            break
                    
                    if existing_original and not any(c['left_column'] == existing_original for c in conflict_resolution_needed[normalized_right]):
                        conflict_resolution_needed[normalized_right].append({
                            'left_column': existing_original,
                            'right_column': match['right_column'],  # Use original name
                            'normalized_left': existing_left,
                            'confidence': next((m['confidence'] for m in matches if normalize_column_name(m['left_column']) == existing_left), 1.0)
                        })
                    
                    # Add current match to conflict list
                    conflict_resolution_needed[normalized_right].append({
                        'left_column': match['left_column'],
                        'right_column': match['right_column'],
                        'normalized_left': normalized_left,
                        'confidence': match['confidence']
                    })
                else:
                    # No conflict, add directly
                    column_map[normalized_right] = normalized_left
            
            # Second pass: Resolve conflicts using key column priority
            for right_col, conflicting_matches in conflict_resolution_needed.items():
                print(f"  DEBUG: RESOLVING CONFLICT for right column '{right_col}':")
                
                # Find key columns among conflicting matches
                key_matches = []
                non_key_matches = []
                
                for conflict_match in conflicting_matches:
                    if validated_keys and conflict_match['left_column'] in validated_keys:
                        key_matches.append(conflict_match)
                        print(f"    🔑 KEY COLUMN: {conflict_match['left_column']}")
                    else:
                        non_key_matches.append(conflict_match)
                        print(f"    📄 NON-KEY: {conflict_match['left_column']}")
                
                # Apply key column priority resolution
                if key_matches:
                    # Key columns have absolute priority
                    if len(key_matches) == 1:
                        # Single key column wins
                        winning_match = key_matches[0]
                        print(f"    ✅ KEY PRIORITY: '{winning_match['left_column']}' wins conflict")
                    else:
                        # Multiple key columns: use confidence (shouldn't happen normally)
                        winning_match = max(key_matches, key=lambda x: x['confidence'])
                        print(f"    ✅ KEY TIEBREAKER: '{winning_match['left_column']}' wins with confidence {winning_match['confidence']:.3f}")
                    
                    # Set the key mapping
                    column_map[right_col] = winning_match['normalized_left']
                else:
                    # No key columns: use confidence-based resolution (existing logic)
                    winning_match = max(conflicting_matches, key=lambda x: x['confidence'])
                    print(f"    ✅ CONFIDENCE: '{winning_match['left_column']}' wins with confidence {winning_match['confidence']:.3f}")
                    column_map[right_col] = winning_match['normalized_left']
            
            print(f"DEBUG: Final normalized column_map being stored: {column_map}")
            config["datasets"][right_name]["column_map"] = column_map
            
            # Set comparison keys - use validated keys if available, fallback to first match
            if validated_keys:
                # Preserve original names in main comparison config for display/error messages
                config["comparisons"][0]["keys"] = validated_keys
                
                # Normalize key column names for dataset configs (staging consistency)
                normalized_left_keys = [normalize_column_name(key) for key in validated_keys]
                config["datasets"][left_name]["key_columns"] = normalized_left_keys
                
                # For right table, map validated keys through column mappings and normalize
                right_key_cols = []
                for key in validated_keys:
                    # Find corresponding right column for this left key
                    right_col = key  # Default to same name
                    for match in matches:
                        if match['left_column'] == key:
                            right_col = match['right_column']
                            break
                    # Normalize the right key column for staging consistency
                    normalized_right_key = normalize_column_name(right_col)
                    right_key_cols.append(normalized_right_key)
                    
                config["datasets"][right_name]["key_columns"] = right_key_cols
                print(f"DEBUG: Using validated keys (original): {validated_keys}")
                print(f"DEBUG: Left dataset key_columns (normalized): {normalized_left_keys}")
                print(f"DEBUG: Right dataset key_columns (normalized): {right_key_cols}")
            elif matches:
                # Fallback to first match - preserve original name in comparison config
                key_col = matches[0]['left_column']
                config["comparisons"][0]["keys"] = [key_col]
                
                # Normalize key columns for dataset configs (staging consistency)
                normalized_left_key = normalize_column_name(key_col)
                normalized_right_key = normalize_column_name(matches[0]['right_column'])
                
                config["datasets"][left_name]["key_columns"] = [normalized_left_key]
                config["datasets"][right_name]["key_columns"] = [normalized_right_key]
                
                print(f"DEBUG: Fallback key (original): {key_col}")
                print(f"DEBUG: Left dataset key_columns (normalized): [{normalized_left_key}]")
                print(f"DEBUG: Right dataset key_columns (normalized): [{normalized_right_key}]")
        
        return config
    
    def _execute_comparison(self, config: Dict[str, Any]) -> bool:
        """
        Execute the comparison with given configuration.
        
        Args:
            config: Comparison configuration
            
        Returns:
            True if successful
        """
        # Import main components dynamically to avoid relative import issues
        import sys
        from pathlib import Path
        
        # Add project root to path
        project_root = Path(__file__).parent.parent.parent
        if str(project_root) not in sys.path:
            sys.path.insert(0, str(project_root))
        
        from main import DataDiffPipeline
        
        try:
            # Save temporary config
            import tempfile
            import yaml
            
            with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', 
                                           delete=False) as f:
                yaml.dump(config, f, default_flow_style=False)
                temp_config_path = Path(f.name)
            
            print(f"\n🚀 Starting comparison...")
            
            # Run pipeline
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
                print("📁 Check data/reports/ for results")
            else:
                print("\n❌ Comparison failed. Check logs for details.")
            
            return success
            
        except Exception as e:
            print(f"\n❌ Error running comparison: {e}")
            return False