"""
Configuration file for Preprocessing Module (Multivariate Edition)

HOW TO SET YOUR PATHS:
  Create a .env file in this folder (never commit it) or set environment
  variables in your shell before running. Example .env:

      OUTPUT_DIR=D:/Thesis_2/Output_Normal
      TC_SUMMARY_CSV=D:/Thesis_2/TC_Summary_filtered_PAR_normal.csv
      IBTRACS_CSV=D:/Thesis_2/ibtracs_filtered_PAR_normal.csv
      CHL_INDEX_CSV=D:/Thesis_2/Chl-a/index_chla_l3m_custom.csv
      SST_INDEX_CSV=D:/Thesis_2/SST/index_sst.csv
      WIND_U_INDEX_CSV=D:/Thesis_2/Wind/index_u_wind_10m.csv
      WIND_V_INDEX_CSV=D:/Thesis_2/Wind/index_v_wind_10m.csv
      PAR_SHAPEFILE=D:/Thesis_2/PAR/Philippine_Area_of_Responsibility.shp
      LANDMASK_SHAPEFILE=D:/Thesis_2/world-administrative-boundaries/world-administrative-boundaries.shp
      BATHYMETRY_FILE=D:/Thesis_2/GEBCO/gebco_2025_n30.0_s0.0_w105.0_e140.0.nc
      CLIMATOLOGY_DIR=D:/Thesis_2/TC_Analysis_Output
"""

import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

def _require(var: str) -> Path:
    """Get an env var as a Path, raising a clear error if it's not set."""
    val = os.environ.get(var)
    if not val:
        raise EnvironmentError(
            f"Required environment variable '{var}' is not set. "
            f"See the instructions at the top of config.py."
        )
    return Path(val)

# ==================== MEMORY OPTIMIZATION ====================
MEMORY_THRESHOLD_MB = 200

# ==================== MAIN PATHS ====================
OUTPUT_DIR      = _require("OUTPUT_DIR")
TC_SUMMARY_CSV  = _require("TC_SUMMARY_CSV")
IBTRACS_CSV     = _require("IBTRACS_CSV")

# Index files for each data variable
CHL_INDEX_CSV    = _require("CHL_INDEX_CSV")
SST_INDEX_CSV    = _require("SST_INDEX_CSV")
WIND_U_INDEX_CSV = _require("WIND_U_INDEX_CSV")
WIND_V_INDEX_CSV = _require("WIND_V_INDEX_CSV")

# ==================== SPATIAL DATA ====================
PAR_SHAPEFILE      = _require("PAR_SHAPEFILE")
LANDMASK_SHAPEFILE = _require("LANDMASK_SHAPEFILE")  # Note: replaced by GEBCO mask for ocean processing
BATHYMETRY_FILE    = _require("BATHYMETRY_FILE")      # GEBCO — used for <30m depth mask

# ==================== CLIMATOLOGY PATHS ====================
_CLIM_DIR = _require("CLIMATOLOGY_DIR")

CLIMATOLOGY_PATHS = {
    'DJF': _CLIM_DIR / "NonTC_Chla_Climatology_DJF_2005-2024.nc",
    'MAM': _CLIM_DIR / "NonTC_Chla_Climatology_MAM_2005-2024.nc",
    'JJA': _CLIM_DIR / "NonTC_Chla_Climatology_JJA_2005-2024.nc",
    'SON': _CLIM_DIR / "NonTC_Chla_Climatology_SON_2005-2024.nc",
}

# ==================== SCIENTIFIC PARAMETERS ====================
BUFFER_DAYS    = 28      # Days before/after TC window to include
CHL_MIN        = 0.001   # Minimum valid Chl-a (mg/m³)
CHL_MAX        = 100.0   # Maximum valid Chl-a (mg/m³)
LOG_OFFSET     = 0.01    # Offset before log10 transform
SPATIAL_SMOOTH = True    # Apply Gaussian spatial smoothing
SMOOTH_SIGMA   = 0.3     # Sigma for Gaussian smoothing
DEPTH_CUTOFF   = -30.0   # Mask pixels shallower than 30m

# ==================== LOGGING ====================
LOG_FILE       = 'preprocessing_pipeline.log'
LOG_LEVEL      = 'INFO'
STOP_ON_ERROR  = False
