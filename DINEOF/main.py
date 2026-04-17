import subprocess
import logging
import time
import os
import shutil
import concurrent.futures
from pathlib import Path
from typing import Dict, List

# --- IMPORTS ---
from modules.tc_finder import TCFinder
from modules.dineof_init_manager import DINEOFInitManager
from modules.post_processor import PostProcessor
from modules.validator import Validator
from modules.tc_anomaly_analyzer import TCAnomalyAnalyzer
from modules.drive_mount_checker import WSLDriveMounter, check_wsl_available
import config 

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- 1. WORKER FUNCTION ---
def process_single_tc_workflow(tc: Dict, config_overrides: Dict, flags: Dict):
    """
    Executes the pipeline based on the flags provided (True/False).
    """
    os.environ['OMP_NUM_THREADS'] = '4'
    
    # Initialize Modules
    init_manager = DINEOFInitManager()
    
    # Load params from config (Dynamic)
    class TempConfig: pass
    cfg = TempConfig()
    for k, v in config.DINEOF_PARAMS.items(): setattr(cfg, k, v)
    for k, v in config_overrides.items(): setattr(cfg, k, v)
    
    dineof_processor = DINEOFProcessor(cfg) 
    post_processor = PostProcessor()
    validator = Validator()
    
    # Anomaly Analyzer Setup
    climatology_paths = getattr(config, 'SEASONAL_CLIMATOLOGY_PATHS', {})
    anomaly_analyzer = TCAnomalyAnalyzer(
        config.PAR_SHAPEFILE, 
        config.IBTRACS_CSV, 
        climatology_paths
    )
    
    tc_finder = TCFinder(config_overrides['CSV_PATH'], config_overrides['OUTPUT_BASE_DIR'])
    
    res = {'id': f"{tc['year']}_{tc['storm_name']}", 'success': True, 'error': None}
    tc_success = True
    temp_dir = None
    fast_input_path = None

    try:
        # Create output folders
        windows_folder, wsl_folder = tc_finder.create_tc_folder(tc)
        
        # --- A. SETUP TEMP & FAST I/O ---
        # Only needed if running DINEOF, otherwise we can skip temp file creation
        if flags['run_dineof']:
            temp_dir = Path("C:/tmp") / f"dineof_{tc['storm_name']}"
            temp_dir.mkdir(parents=True, exist_ok=True)
            
            orig_win_path = Path(tc['windows_path'])
            fast_input_path = temp_dir / orig_win_path.name
            
            if not fast_input_path.exists():
                shutil.copy(orig_win_path, fast_input_path)

            tc['wsl_path'] = TCFinder.convert_to_wsl_path(str(fast_input_path))

        # --- B. DINEOF ---
        if tc_success and flags['run_dineof']:
            
            # 1. FORCE CLEANUP: Delete old init file
            init_file_path = windows_folder / "dineof.init"
            if init_file_path.exists():
                try: os.remove(init_file_path)
                except: pass
            
            # 2. DYNAMIC CONFIG
            init_path = init_manager.create_init_file(
                tc, 
                wsl_folder, 
                params=config.DINEOF_PARAMS 
            )
            
            if init_manager.validate_init_file(init_path):
                # Run with Live Streaming
                d_success = dineof_processor.run_dineof(tc, wsl_folder, windows_folder)
                if not d_success:
                    tc_success = False
                    res['error'] = "DINEOF Execution Failed (Check dineof.log)"
            else:
                tc_success = False
                res['error'] = "Init File Generation Failed"

        # --- C. POST PROCESS ---
        if tc_success and flags['run_postprocess']:
            # If we skipped DINEOF, we assume the file already exists in the folder
            post_processor.process_tc(tc, windows_folder)

        # --- D. VALIDATION ---
        if tc_success and flags['run_validation']:
            validator.validate_tc(tc, windows_folder)
            
        # --- E. ANOMALY ---
        if tc_success and flags['run_anomaly']:
            post_file = windows_folder / f"chlorophyll_a_final_{tc['year']}_{tc['storm_name']}.nc"
            if post_file.exists():
                bloom_thresh = getattr(config, 'BLOOM_THRESHOLD', 0.5)
                anomaly_analyzer.analyze_tc(
                    tc, post_file, windows_folder,
                    pre_tc_days=config.PRE_TC_BASELINE_DAYS,
                    analysis_days=config.ANALYSIS_DAYS,
                    bloom_threshold=bloom_thresh
                )
            else:
                logger.warning(f"[{tc['storm_name']}] Cannot run Anomaly: Final .nc file missing.")

        res['success'] = tc_success
        return res

    except Exception as e:
        import traceback
        logger.error(traceback.format_exc())
        return {'id': f"{tc['year']}_{tc['storm_name']}", 'success': False, 'error': str(e)}
    
    finally:
        # Cleanup
        try:
            if temp_dir and temp_dir.exists(): shutil.rmtree(temp_dir)
        except: pass

# --- 2. DINEOF CLASS (LIVE STREAMING) ---
class DINEOFProcessor:
    def __init__(self, config_obj):
        self.timeout = getattr(config_obj, 'DINEOF_TIMEOUT', 7200)
        self.dineof_path = getattr(config, 'DINEOF_PATH', '/home/jmpavs/DINEOF')
    
    def run_dineof(self, tc_info: Dict, wsl_output_dir: str, windows_output_dir: Path) -> bool:
        if not wsl_output_dir.endswith('/'): wsl_output_dir += '/'
        init_file = f"{wsl_output_dir}dineof.init"
        
        cmd = ['wsl', 'bash', '-c', f'cd "{self.dineof_path}" && ./dineof "{init_file}"']
        log_path = windows_output_dir / "dineof.log"
        logger.info(f"🚀 Running DINEOF for {tc_info['storm_name']} (Live Output)...")

        try:
            with subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, 
                                  text=True, bufsize=1, encoding='utf-8', errors='replace') as p:
                with open(log_path, "w", encoding='utf-8') as f:
                    f.write(f"Command: {' '.join(cmd)}\n\n")
                    for line in p.stdout:
                        print(f"[{tc_info['storm_name']}] {line.rstrip()}") # Live Print
                        f.write(line)
            
            if p.returncode != 0:
                logger.error(f"DINEOF Failed. See log: {log_path}")
                return False
            return True

        except subprocess.TimeoutExpired:
            logger.error(f"DINEOF timed out for {tc_info['storm_name']}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return False

# --- 3. MAIN ORCHESTRATOR ---
def main(mode='range', 
         year_start=2005, year_end=2024, 
         target_name=None, target_year=None,
         csv_path: str = None, 
         output_dir: str = None,
         # STEPS TOGGLES
         run_dineof=True,
         run_postprocess=True,
         run_validation=True,
         run_anomaly=True):
    
    final_csv = csv_path if csv_path else config.CSV_PATH
    final_out = output_dir if output_dir else config.OUTPUT_BASE_DIR
    
    conf_overrides = {'CSV_PATH': final_csv, 'OUTPUT_BASE_DIR': final_out}
    
    # Map arguments to flags dictionary
    flags = {
        'run_dineof': run_dineof,
        'run_postprocess': run_postprocess,
        'run_validation': run_validation,
        'run_anomaly': run_anomaly
    }

    if not check_wsl_available(): return
    tc_finder = TCFinder(final_csv, final_out)
    
    # === MODE SELECTION ===
    tc_list = []
    if mode == 'specific':
        if not target_name or not target_year:
            logger.error("❌ 'specific' mode requires target_name and target_year")
            return
        # Get specific
        all_tcs = tc_finder.get_available_tcs(target_year, target_year)
        tc_list = [tc for tc in all_tcs if tc['storm_name'].upper() == target_name.upper()]
        logger.info(f"🎯 MODE: SPECIFIC | Target: {target_name} ({target_year})")
    else:
        # Get range
        tc_list = tc_finder.get_available_tcs(year_start, year_end)
        logger.info(f"📅 MODE: RANGE | Years: {year_start}-{year_end}")

    if not tc_list:
        logger.warning("No TCs found matching criteria.")
        return
        
    # =========================================================
    # ⏯️ SKIP LOGIC: Filter out already processed TCs
    # =========================================================
    tracking_file = Path(final_out) / "processed_tcs.txt"
    
    if mode == 'range' and tracking_file.exists():
        try:
            with open(tracking_file, 'r') as f:
                # Set of IDs that are already done
                done_tcs = {line.strip() for line in f if line.strip()}
            
            original_count = len(tc_list)
            
            # Filter the list: Keep TC if its ID is NOT in the done_tcs set
            tc_list = [tc for tc in tc_list if f"{tc['year']}_{tc['storm_name']}" not in done_tcs]
            
            skipped_count = original_count - len(tc_list)
            
            if skipped_count > 0:
                logger.info(f"⏭️  SKIPPING {skipped_count} TCs found in processed_tcs.txt")
                logger.info(f"📋 Remaining to process: {len(tc_list)}")
                
        except Exception as e:
            logger.warning(f"Could not read tracking file: {e}")

    if not tc_list:
        logger.info("🎉 All TCs in this range are already processed!")
        return

    # === EXECUTION ===
    MAX_WORKERS = 2 
    logger.info(f"Starting {len(tc_list)} TCs with {MAX_WORKERS} workers...")
    logger.info(f"Steps: DINEOF={run_dineof}, POST={run_postprocess}, VAL={run_validation}, ANOMALY={run_anomaly}")
    
    with concurrent.futures.ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_tc = {executor.submit(process_single_tc_workflow, tc, conf_overrides, flags): tc for tc in tc_list}
        
        for future in concurrent.futures.as_completed(future_to_tc):
            tc = future_to_tc[future]
            try:
                result = future.result()
                if result['success']:
                    logger.info(f"✓ COMPLETED: {result['id']}")
                    
                    # =========================================================
                    # 💾 SAVE LOGIC: Append successful TC to tracking file
                    # =========================================================
                    try:
                        with open(tracking_file, "a") as f:
                            f.write(f"{result['id']}\n")
                    except Exception as file_e:
                        logger.error(f"Could not write to tracking file: {file_e}")
                    # =========================================================
                    
                else:
                    logger.warning(f"✗ FAILED: {result['id']} - {result.get('error')}")
            except Exception as e:
                logger.error(f"Critical Worker Exception: {e}")

if __name__ == "__main__":
    # ==========================================
    # 🎛️ CONTROL PANEL
    # ==========================================
    #"C:\Users\Jan Mcknere\Downloads\TC_Independent_Filtered\TC_independent_mingap30_first_ANOMALOUS_summary.csv"
    #"D:\Thesis_2\Output_Normal_3\TC_independent_mingap30_first_ANOMALOUS_summary.csv"

    # 1. FILE PATHS
    CSV_PATH = r"C:\Users\Jan Mcknere\Downloads\TC_Independent_Filtered\TC_independent_mingap30_first_ANOMALOUS_summary.csv"
    OUTPUT_BASE_DIR = r"D:\Thesis_2\Output_Anomalous_3"

    # 2. SELECTION MODE
    # Options: 'specific' OR 'range'
    MODE = 'range' 
    
    # IF SPECIFIC:
    TARGET_NAME = 'YOLANDA'
    TARGET_YEAR = 2013
    
    # IF RANGE:
    START_YEAR = 2005
    END_YEAR = 2024

    # 3. PIPELINE STEPS (True/False)
    RUN_DINEOF      = False   # Reconstruct missing data
    RUN_POSTPROCESS = False   # Convert to Chlorophyll-a
    RUN_VALIDATION  = False   # Check coverage %
    RUN_ANOMALY     = True   # Calculate Anomalies

    # ==========================================
    # 🚀 EXECUTE
    # ==========================================
    main(
        mode=MODE,
        # Specific params
        target_name=TARGET_NAME,
        target_year=TARGET_YEAR,
        # Range params
        year_start=START_YEAR,
        year_end=END_YEAR,
        # Paths
        csv_path=CSV_PATH,
        output_dir=OUTPUT_BASE_DIR,
        # Toggles
        run_dineof=RUN_DINEOF,
        run_postprocess=RUN_POSTPROCESS,
        run_validation=RUN_VALIDATION,
        run_anomaly=RUN_ANOMALY
    )