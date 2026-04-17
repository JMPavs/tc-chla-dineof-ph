"""
Main orchestration script for TC chlorophyll-a processing (Multivariate Edition).
Updated to support 'Range' and 'Specific TC' modes.
"""
import sys
import numpy as np
from pathlib import Path
import gc
import warnings

# Ensure project root is in path
try:
    project_root = Path(__file__).resolve().parent
except NameError:
    project_root = Path.cwd()

if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# Import Custom Modules
from modules.tc_selector import TCSelector
from modules.file_finder import FileFinder
from modules.preprocessor import Preprocessor
from modules.logger import ProcessingLogger
import config

def cleanup_memory(ds=None):
    """Safely clean up memory."""
    try:
        if ds: ds.close()
        gc.collect()
    except: pass

def process_single_tc(tc_selector, file_finder, preprocessor, logger, name, year, tc_num, total_tcs, output_dir):
    """Process a single TC."""
    print(f"\n{'='*80}")
    print(f"Processing TC {tc_num}/{total_tcs}: {name} ({year})")
    print(f"{'='*80}")
    
    ds = None
    tc_info = None
    
    try:
        # 1. Get temporal window
        tc_info = tc_selector.get_tc_window(name, year)
        if not tc_info:
            print(f"⊘ TC did not enter PAR or data missing - SKIPPING")
            return 'SKIPPED'
        
        # 2. Find Chl-a files
        files = file_finder.find_files(tc_info['window_start'], tc_info['window_end'])
        if not files:
            print(f"⊘ No data files found - SKIPPING")
            logger.log_tc_result(tc_info, 'SKIPPED', 'No files', 0)
            return 'SKIPPED'
        
        print(f"Found {len(files)} Chl-a files")
        
        # 3. Load & Preprocess
        print("Loading dataset...")
        ds = preprocessor.load_dataset(files)
        
        print("Applying quality control (Masking <30m)...")
        ds = preprocessor.apply_quality_control(ds)
        
        print("Computing biological anomalies (vs Climatology)...")
        ds = preprocessor.compute_log_and_anomalies(ds)
        
        # 4. MULTIVARIATE STEP
        print("Assembling Multivariate Matrix (SST + Wind)...")
        ds = preprocessor.compute_multivariate_matrix(
            ds, 
            tc_info['window_start'], 
            tc_info['window_end']
        )
        
        # 5. Define Paths
        dineof_filename = f"{year}_{name}_DINEOF_Input_Multivariate.nc"
        dineof_file = output_dir / dineof_filename
        
        # 6. Save
        preprocessor.save_dineof(ds, dineof_file, tc_info) 
        
        # 7. Log
        logger.log_tc_result(tc_info, 'SUCCESS', f'Saved to {output_dir.name}', 
                             files_found=len(files), dineof_file=dineof_file)
        
        print(f"\n✓ Successfully processed {name} ({year})")
        return 'SUCCESS'
        
    except Exception as e:
        import traceback
        print(f"\n✗ ERROR: {e}")
        print(traceback.format_exc())
        return 'FAILED'
        
    finally:
        cleanup_memory(ds)

# === UPDATED MAIN FUNCTION ===
def main(mode='range', year_start=2005, year_end=2024, 
         target_name=None, target_year=None,
         output_dir=None, ibtracs_csv=None, tc_summary_csv=None):
    """
    Main execution loop with two modes:
    1. mode='range': Runs all TCs between year_start and year_end.
    2. mode='specific': Runs only the TC defined by target_name and target_year.
    """
    
    # 1. Resolve Paths
    final_output_dir = Path(output_dir) if output_dir else config.OUTPUT_DIR
    final_ibtracs = Path(ibtracs_csv) if ibtracs_csv else config.IBTRACS_CSV
    final_summary = Path(tc_summary_csv) if tc_summary_csv else config.TC_SUMMARY_CSV

    final_output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"Output Directory: {final_output_dir}")
    print(f"Mode:             {mode.upper()}")
    
    # 2. Initialize Modules
    tc_selector = TCSelector(final_ibtracs, config.PAR_SHAPEFILE, config.BUFFER_DAYS)
    file_finder = FileFinder(config.CHL_INDEX_CSV) 
    logger = ProcessingLogger(final_output_dir, final_summary)
    
    try:
        preprocessor = Preprocessor(
            chl_min=config.CHL_MIN, chl_max=config.CHL_MAX,
            log_offset=config.LOG_OFFSET
        )
    except FileNotFoundError as e:
        print(f"CRITICAL ERROR: {e}")
        return

    # 3. Generate TC List based on Mode
    tc_list = []
    
    if mode == 'specific':
        if target_name is None or target_year is None:
            print("❌ ERROR: Mode 'specific' requires 'target_name' and 'target_year'.")
            return
        # Create a single-item list manually
        tc_list = [(target_name.upper(), int(target_year))]
        print(f"Targeting Single TC: {target_name} ({target_year})")
        
    else: # mode == 'range'
        print(f"Scanning Years:   {year_start}-{year_end}")
        tc_list = tc_selector.get_tc_list(year_start, year_end)
        tc_list.sort(key=lambda x: (x[1], x[0]))
        print(f"Found {len(tc_list)} storms in PAR.")

    # 4. Loop
    if not tc_list:
        print("No TCs found to process.")
        return

    for i, (name, year) in enumerate(tc_list, 1):
        process_single_tc(
            tc_selector, file_finder, preprocessor, logger,
            name, year, i, len(tc_list), final_output_dir
        )
        
    print("\nAll processing complete.")

if __name__ == "__main__":
    # ================= CONFIGURE RUN HERE =================
    
    # OPTION 1: Run a specific TC
    # main(mode='specific', target_name='YOLANDA', target_year=2013)
    
    # OPTION 2: Run a whole range of years
    # main(mode='range', year_start=2005, year_end=2024)

    # OPTION 3: Run a Custom List (Use this for fixes)
    # List format: ('NAME', Year)
    target_list = [
        ('MAYSAK', 2020),
        ('CHOI-WAN', 2021),
        ('SURIGAE', 2021),
        ('MEGI', 2022)  # Added based on your other error
    ]

    # This loop runs them one by one
    for name, year in target_list:
        print(f"\n--- Processing List Item: {name} {year} ---")
        main(mode='specific', target_name=name, target_year=year)
    
    # ======================================================