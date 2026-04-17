"""
Logging and summary tracking with CSV file path updating.
"""
import pandas as pd
from pathlib import Path
from datetime import datetime

class ProcessingLogger:
    def __init__(self, output_dir, tc_summary_csv=None):
        """
        Initialize logger with optional TC summary CSV for updating file paths.
        
        Parameters
        ----------
        output_dir : Path
            Output directory for logs
        tc_summary_csv : str or Path, optional
            Path to TC_Summary CSV file to update with file paths
        """
        self.output_dir = Path(output_dir)
        self.summary_file = self.output_dir / "summary_results.csv"
        self.records = []
        
        # CSV integration
        self.tc_summary_csv = Path(tc_summary_csv) if tc_summary_csv else None
        self.tc_summary_df = None
        
        if self.tc_summary_csv and self.tc_summary_csv.exists():
            self.tc_summary_df = pd.read_csv(self.tc_summary_csv)
            print(f"Loaded TC summary CSV: {self.tc_summary_csv}")
            print(f"  Total storms in CSV: {len(self.tc_summary_df)}")
            
            # Initialize columns if they don't exist
            if 'Pre Process File' not in self.tc_summary_df.columns:
                self.tc_summary_df['Pre Process File'] = ''
            if 'Archive File' not in self.tc_summary_df.columns:
                self.tc_summary_df['Archive File'] = ''
        
    def log_tc_result(self, tc_info, status, message="", files_found=0, 
                     valid_pixels=0, coverage_pct=0.0, 
                     archive_file=None, dineof_file=None):
        """
        Log result for a single TC and update CSV if provided.
        
        Parameters
        ----------
        tc_info : dict
            TC information
        status : str
            'SUCCESS', 'FAILED', or 'SKIPPED'
        message : str
            Status message
        files_found : int
            Number of input files found
        valid_pixels : int
            Number of valid pixels
        coverage_pct : float
            Coverage percentage
        archive_file : Path, optional
            Path to archive file (if created)
        dineof_file : Path, optional
            Path to DINEOF file (if created)
        """
        # Log to summary results
        record = {
            'timestamp': datetime.now().isoformat(),
            'tc_name': tc_info['tc_name'],
            'year': tc_info['year'],
            'par_start': tc_info['par_start'],
            'par_end': tc_info['par_end'],
            'par_days': tc_info['par_days'],
            'total_days': tc_info['total_days'],
            'files_found': files_found,
            'status': status,
            'message': message,
            'valid_pixels': valid_pixels,
            'coverage_pct': coverage_pct,
            'archive_file': str(archive_file) if archive_file else '',
            'dineof_file': str(dineof_file) if dineof_file else ''
        }
        
        self.records.append(record)
        self._save_summary()
        
        # Update TC summary CSV if available and status is SUCCESS
        if self.tc_summary_df is not None and status == 'SUCCESS':
            self._update_tc_summary_csv(
                tc_info['tc_name'], 
                tc_info['year'],
                archive_file,
                dineof_file
            )
    
    def _update_tc_summary_csv(self, tc_name, year, archive_file, dineof_file):
        try:
            mask = (
                (self.tc_summary_df['NAME'].str.strip().str.upper() == tc_name.strip().upper()) &
                (self.tc_summary_df['YEAR'] == year)
            )
            matching_indices = self.tc_summary_df[mask].index
            
            if len(matching_indices) > 0:
                for idx in matching_indices:
                    # UPDATE: If we have a DINEOF file, put it in BOTH columns.
                    if dineof_file:
                        path_str = str(dineof_file)
                        self.tc_summary_df.at[idx, 'Pre Process File'] = path_str
                        # FIX: Force 'Archive File' to point to the same file
                        self.tc_summary_df.at[idx, 'Archive File'] = path_str
                    
                    # (Keeps compatibility if you ever do generate a real archive file later)
                    if archive_file:
                        self.tc_summary_df.at[idx, 'Archive File'] = str(archive_file)
                
                self.tc_summary_df.to_csv(self.tc_summary_csv, index=False)
                print(f"  ✓ CSV Updated: {tc_name} ({year}) path set for Validation.")
            else:
                print(f"  Warning: No matching row found in CSV for {tc_name} ({year})")
                
        except Exception as e:
            print(f"  Warning: Could not update CSV: {e}")
    
    def _save_summary(self):
        """Save current records to summary CSV."""
        df = pd.DataFrame(self.records)
        df.to_csv(self.summary_file, index=False)
    
    def get_final_stats(self):
        """
        Get final processing statistics from TC summary CSV.
        
        Returns
        -------
        dict
            Statistics about matched/unmatched files
        """
        if self.tc_summary_df is None:
            return None
        
        total = len(self.tc_summary_df)
        matched_pre = (self.tc_summary_df['Pre Process File'] != '').sum()
        matched_archive = (self.tc_summary_df['Archive File'] != '').sum()
        matched_both = ((self.tc_summary_df['Pre Process File'] != '') & 
                       (self.tc_summary_df['Archive File'] != '')).sum()
        unmatched = ((self.tc_summary_df['Pre Process File'] == '') & 
                    (self.tc_summary_df['Archive File'] == '')).sum()
        
        return {
            'total_storms': total,
            'matched_pre_process': matched_pre,
            'matched_archive': matched_archive,
            'matched_both': matched_both,
            'unmatched': unmatched
        }