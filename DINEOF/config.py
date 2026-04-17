"""
Configuration file for DINEOF TC Processing

HOW TO SET YOUR PATHS:
  Create a .env file in this folder (never commit it) or set environment
  variables in your shell before running. Example .env:

      TC_CSV_PATH=D:/Thesis_2/TC_Summary_filtered_PAR_normal.csv
      OUTPUT_BASE_DIR=D:/Thesis_2/Output_Normal
      DINEOF_PATH=/home/youruser/DINEOF
      PAR_SHAPEFILE=D:/Thesis_2/PAR/Philippine_Area_of_Responsibility.shp
      IBTRACS_CSV=D:/Thesis_2/ibtracs_filtered_PAR.csv
      CLIMATOLOGY_DIR=D:/Thesis_2/TC_Analysis_Output
      RAW_MODIS_DIR=D:/Thesis_2/Chl-a/Chl-a L3 Mapped Custom
"""

import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

def _require(var: str) -> str:
    """Get an env var, raising a clear error if it's not set."""
    val = os.environ.get(var)
    if not val:
        raise EnvironmentError(
            f"Required environment variable '{var}' is not set. "
            f"See the instructions at the top of config.py."
        )
    return val

# ==================== PATHS ====================
CSV_PATH          = _require("TC_CSV_PATH")
OUTPUT_BASE_DIR   = _require("OUTPUT_BASE_DIR")
DINEOF_PATH       = _require("DINEOF_PATH")
PAR_SHAPEFILE     = _require("PAR_SHAPEFILE")
IBTRACS_CSV       = _require("IBTRACS_CSV")
RAW_MODIS_DIR     = _require("RAW_MODIS_DIR")

# ==================== TC ANOMALY ANALYSIS PATHS ====================
# Seasonal climatology files — all expected inside CLIMATOLOGY_DIR
_CLIM_DIR = Path(_require("CLIMATOLOGY_DIR"))

SEASONAL_CLIMATOLOGY_PATHS = {
    'DJF': str(_CLIM_DIR / "NonTC_Chla_Climatology_DJF_2005-2024.nc"),
    'MAM': str(_CLIM_DIR / "NonTC_Chla_Climatology_MAM_2005-2024.nc"),
    'JJA': str(_CLIM_DIR / "NonTC_Chla_Climatology_JJA_2005-2024.nc"),
    'SON': str(_CLIM_DIR / "NonTC_Chla_Climatology_SON_2005-2024.nc"),
}

# ==================== DINEOF PARAMETERS (STORM MODE) ====================
# Tuned for short duration (56 days) but high complexity (Storms)
DINEOF_PARAMS = {
    'alpha':    0.01,
    'numit':    3,
    'nev':      8,
    'neini':    1,
    'ncv':      25,
    'tol':      1.0e-8,
    'nitemax':  300,
    'toliter':  1.0e-2,
    'rec':      1,
    'eof':      1,
    'norm':     1,
    'cloud_size': 1000,
    'seed':     243435,
}

# ==================== TC ANOMALY ANALYSIS PARAMETERS ====================
PRE_TC_BASELINE_DAYS      = 7    # Days before PAR entry used as baseline
ANALYSIS_DAYS             = 28   # Days after PAR entry to analyse

# Bresenham corridor parameters
CORRIDOR_LEFT_OFFSET_KM   = 100  # Left side of track
CORRIDOR_RIGHT_OFFSET_KM  = 200  # Right side of track
CORRIDOR_GRID_RESOLUTION  = 0.04 # Degrees (~4.4 km)

# Bloom detection threshold (mg/m³)
BLOOM_THRESHOLD           = 0.5

# ==================== LOGGING ====================
LOG_FILE  = 'dineof_processing.log'
LOG_LEVEL = 'INFO'

# ==================== PROCESSING OPTIONS ====================
DINEOF_TIMEOUT = 7200  # Timeout per DINEOF run (seconds)
STOP_ON_ERROR  = False

# ==================== FILE NAMING ====================
OUTPUT_FOLDER_PATTERN  = "Output_{year}_{storm_name}"
INIT_FILE_NAME         = "dineof.init"
OUTPUT_NC_PATTERN      = "dineof_chlor_a_{year}_{storm_name}.nc"
POST_PROCESSED_PATTERN = "chlorophyll_a_final_{year}_{storm_name}.nc"
ANOMALY_PATTERN        = "tc_anomaly_{year}_{storm_name}.nc"
EOF_FILE_NAME          = "eof.nc"

# ==================== VALIDATION ====================
VALIDATION_MODE = 'raw'  # 'raw' = use raw MODIS files | 'preprocessed' = use preprocessed files
