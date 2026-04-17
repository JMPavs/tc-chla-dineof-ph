"""
Module 2: File Discovery using CSV Index
"""
import pandas as pd
from datetime import datetime
from pathlib import Path

class FileFinder:
    def __init__(self, csv_index_path):
        """
        Initialize with CSV index file.
        
        Parameters
        ----------
        csv_index_path : str or Path
            Path to CSV file with columns: date, filepath, status
        """
        self.df = pd.read_csv(csv_index_path)
        
        # Parse date column - handle multiple formats
        if 'date' in self.df.columns:
            # Try ISO format first (YYYY-MM-DD), then fall back to other formats
            try:
                self.df['date'] = pd.to_datetime(self.df['date'], format='ISO8601')
            except:
                self.df['date'] = pd.to_datetime(self.df['date'], format='%d/%m/%Y')
        else:
            raise ValueError(f"No 'date' column found. Available columns: {self.df.columns.tolist()}")
        
        # Only keep files marked as 'OK'
        if 'status' in self.df.columns:
            self.df = self.df[self.df['status'] == 'OK'].copy()
        
        # Ensure filepath is Path object
        self.df['filepath'] = self.df['filepath'].apply(lambda x: Path(x))
        
        print(f"Loaded {len(self.df)} valid files from index")
    
    def find_files(self, start_date, end_date):
        """
        Find all NetCDF files within date range using index.
        
        Parameters
        ----------
        start_date : datetime
            Start date
        end_date : datetime
            End date
        
        Returns
        -------
        list of Path
            Sorted list of file paths
        """
        mask = (self.df['date'] >= start_date) & (self.df['date'] <= end_date)
        files = self.df.loc[mask, 'filepath'].tolist()
        
        # Verify files exist
        existing_files = [f for f in files if f.exists()]
        
        if len(existing_files) < len(files):
            missing = len(files) - len(existing_files)
            print(f"  Warning: {missing} files in index not found on disk")
        
        return sorted(existing_files)
    
    @staticmethod
    def parse_date_from_file(filepath):
        """Extract date from MODIS filename (kept for compatibility)."""
        try:
            parts = Path(filepath).stem.split(".")
            if len(parts) > 1:
                return datetime.strptime(parts[1], "%Y%m%d")
        except (ValueError, IndexError):
            pass
        return None