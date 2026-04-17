import sys
import logging
from pathlib import Path

# --- CONFIGURATION ---
TARGET_FILE = r"D:\Thesis_2\Output_Validation_Test\Output_2013_HAIYAN\chlorophyll_a_final_2013_HAIYAN.nc"
STORM_NAME = "HAIYAN"
YEAR = 2013

# Setup simple logging to see progress in the terminal
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# Import the Validator class from your existing file
try:
    # Assuming validator.py is in the same directory
    from validator import Validator
except ImportError:
    # If validator is inside a 'modules' folder, try this:
    try:
        sys.path.append(str(Path(__file__).parent / 'modules'))
        from validator import Validator
    except Exception as e:
        logger.error("Could not import 'Validator'. Make sure 'validator.py' is in the same folder.")
        sys.exit(1)

def run_standalone_validation():
    target_path = Path(TARGET_FILE)
    
    if not target_path.exists():
        logger.error(f"Target file not found: {target_path}")
        return

    logger.info(f"=== Starting Sensitivity Analysis Validation ===")
    logger.info(f"Target: {STORM_NAME} ({YEAR})")
    logger.info(f"File:   {target_path.name}")

    # 1. Initialize Validator
    val = Validator()

    # 2. Mock the 'tc_info' dictionary required by the class
    # The validator relies on this to name outputs and find the file
    tc_info = {
        'year': YEAR,
        'storm_name': STORM_NAME,
        # These are usually calculated, but for validation plots, only names matter
        'tc_name': f"{YEAR} {STORM_NAME}" 
    }

    # 3. Define the Output Directory
    # The validator expects the file to be INSIDE this directory
    output_dir = target_path.parent

    # 4. Run Validation
    # note: validate_tc expects to find the file named "chlorophyll_a_final_{year}_{storm_name}.nc"
    # inside the output_dir. Since your file matches this naming convention, it will work.
    success, metrics = val.validate_tc(tc_info, output_dir)

    if success:
        print("\n" + "="*40)
        print(f"✓ VALIDATION SUCCESS")
        print("="*40)
        print(f"RMSE:        {metrics.get('rmse_linear', 0):.4f}")
        print(f"Correlation: {metrics.get('correlation', 0):.4f}")
        print(f"Bias:        {metrics.get('bias', 0):.4f}")
        print(f"Gaps Filled: {metrics.get('gaps_filled_pct', 0):.2f}%")
        print("-" * 40)
        print(f"Outputs saved to: {output_dir}")
        print(f"1. Validation Plot (.png)")
        print(f"2. Metrics Summary (.csv)")
    else:
        print("\n❌ VALIDATION FAILED. Check logs above.")

if __name__ == "__main__":
    run_standalone_validation()