import pandas as pd
import os
from pathlib import Path
from typing import List, Dict, Tuple
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TCFinder:
    """
    Handles TC detection, filtering, and file path management for DINEOF processing.
    Updated: Removed dependency on 'Archive File' for validation.
    """
    
    def __init__(self, csv_path: str, output_base_dir: str):
        """
        Initialize TC Finder
        
        Args:
            csv_path: Path to TC_Summary_normal_independent.csv
            output_base_dir: Base directory for output (e.g., "C:\\Users\\...\\Output")
        """
        self.csv_path = csv_path
        self.output_base_dir = Path(output_base_dir)
        self.df = None
        self._load_data()
    
    def _load_data(self):
        """Load the TC summary CSV file and standardize columns"""
        try:
            # 1. Load CSV
            self.df = pd.read_csv(self.csv_path)
            
            # 2. Clean Column Names (Strip whitespace and convert to Uppercase)
            self.df.columns = self.df.columns.str.strip().str.upper()
            
            # 3. Handle 'YEAR' vs 'SEASON' alias
            if 'YEAR' not in self.df.columns:
                if 'SEASON' in self.df.columns:
                    logger.info("Mapping 'SEASON' column to 'YEAR'...")
                    self.df['YEAR'] = self.df['SEASON']
                else:
                    logger.error(f"Available columns: {list(self.df.columns)}")
                    raise KeyError("CSV must contain a 'YEAR' or 'SEASON' column")
            
            # 4. Handle 'STORM NAME' vs 'NAME' alias
            if 'STORM NAME' not in self.df.columns:
                if 'NAME' in self.df.columns:
                    self.df['STORM NAME'] = self.df['NAME']

            logger.info(f"Loaded {len(self.df)} TCs from {self.csv_path}")
            
        except Exception as e:
            logger.error(f"Failed to load CSV: {e}")
            raise
    
    def get_tc_list(self, start_year: int, end_year: int) -> List[Dict]:
        """
        Get list of TCs within specified year range
        """
        # Filter by year range (Now guaranteed to have 'YEAR' column)
        filtered_df = self.df[
            (self.df['YEAR'] >= start_year) & 
            (self.df['YEAR'] <= end_year)
        ].copy()
        
        tc_list = []
        for idx, row in filtered_df.iterrows():
            # Check Pre Process File (for DINEOF input)
            cols = self.df.columns
            # Flexible search for the Pre Process column
            pre_col = next((c for c in cols if 'PRE' in c and 'PROCESS' in c and 'FILE' in c), None)

            if not pre_col:
                logger.warning(f"Skipping row {idx}: Could not find 'Pre Process File' column")
                continue

            pre_process_file = row.get(pre_col)
            
            # Skip if NaN or not a string
            if pd.isna(pre_process_file) or not isinstance(pre_process_file, str) or not pre_process_file.strip():
                # Optional: Log warning only if it's inside the year range we care about
                # logger.warning(f"Skipping row {idx}: Empty Pre Process File")
                continue
            
            # Get storm name
            storm_name = row.get('STORM NAME', 'UNKNOWN')
            
            tc_info = {
                'year': int(row['YEAR']), 
                'storm_name': str(storm_name),
                'pre_process_file': str(pre_process_file),
                'windows_path': str(pre_process_file),  
                'wsl_path': self.convert_to_wsl_path(str(pre_process_file)),
                'output_folder': self.create_output_folder_name(
                    int(row['YEAR']), 
                    str(storm_name)
                )
            }
            
            tc_list.append(tc_info)
        
        logger.info(f"Found {len(tc_list)} valid TCs between {start_year} and {end_year}")
        return tc_list
    
    @staticmethod
    def convert_to_wsl_path(windows_path: str) -> str:
        """Convert Windows path to WSL path"""
        if not isinstance(windows_path, str):
            raise ValueError(f"Expected string path, got {type(windows_path)}")
        
        # Remove quotes
        windows_path = windows_path.strip('"').strip("'")
        
        # Replace backslashes
        path = windows_path.replace('\\', '/')
        
        # Convert drive letter
        if ':' in path:
            drive_letter = path[0].lower()
            path = f"/mnt/{drive_letter}" + path[2:]
        
        return path
    
    @staticmethod
    def create_output_folder_name(year: int, storm_name: str) -> str:
        """Create standardized output folder name"""
        clean_name = str(storm_name).upper().replace(' ', '_')
        return f"Output_{year}_{clean_name}"
    
    def create_tc_folder(self, tc_info: Dict) -> Tuple[Path, str]:
        """Create output folder for specific TC"""
        folder_name = tc_info['output_folder']
        windows_folder = self.output_base_dir / folder_name
        
        windows_folder.mkdir(parents=True, exist_ok=True)
        wsl_folder = self.convert_to_wsl_path(str(windows_folder))
        
        return windows_folder, wsl_folder
    
    def verify_input_file_exists(self, tc_info: Dict) -> bool:
        """Verify that the input .nc file exists"""
        try:
            file_path = Path(tc_info['windows_path'])
            exists = file_path.exists()
            if not exists:
                logger.warning(f"Input file not found: {file_path}")
            return exists
        except Exception as e:
            logger.error(f"Error checking file existence: {e}")
            return False
    
    def get_available_tcs(self, start_year: int, end_year: int) -> List[Dict]:
        """Get list of TCs where input files actually exist"""
        all_tcs = self.get_tc_list(start_year, end_year)
        available_tcs = [tc for tc in all_tcs if self.verify_input_file_exists(tc)]
        
        logger.info(f"{len(available_tcs)} out of {len(all_tcs)} TCs have valid input files")
        return available_tcs
    
    def print_tc_summary(self, tc_list: List[Dict]):
        """Print summary of TCs to be processed"""
        print("\n" + "="*70)
        print(f"{'Year':<8} {'Storm Name':<25} {'Status':<15}")
        print("="*70)
        for tc in tc_list:
            status = "✓ Ready" if self.verify_input_file_exists(tc) else "✗ Missing"
            print(f"{tc['year']:<8} {tc['storm_name']:<25} {status:<15}")
        print("="*70 + "\n")