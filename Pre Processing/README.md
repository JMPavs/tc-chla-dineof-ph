# CHLOROPHYLL-A RESPONSE TO TROPICAL CYCLONES IN PHILIPPINE WATERS
**Author:** 
Kyla P. Bidol       
Alexa Hyacenth P. Bolima            
Jose Antonio F. Espera              
Jan Mcknere F. Pavia 
**Advisers:**
Karen P. Conda-Botin, MSc 
John Michael P. Aguado  
 
**Affiliation:** Bicol University College of Science, Physics and Meteorology Department

## Overview
This directory contains the data preparation pipeline for the Chlorophyll-a Tropical Cyclone impact study. It is responsible for taking raw Level-3 Mapped satellite data (MODIS), applying strict quality control, calculating biological anomalies, and assembling a multivariate matrix (Chl-a, SST, U-Wind, V-Wind) formatted specifically for the DINEOF Fortran engine.

## Directory Structure
```text
Pre Processing/
├── main.py                  # Central orchestrator script
├── config.py                # Global configuration and parameters
├── requirements.txt         # Optimized scientific dependencies
├── .env                     # Local environment variables (Do not commit)
├── modules/
│   ├── preprocessor.py      # Core math: Gaussian smoothing, anomalies, Z-scores
│   ├── file_finder.py       # Discovers NetCDF files using CSV index
│   ├── tc_selector.py       # Filters IBTrACS for storms entering PAR
│   └── logger.py            # Tracks progress and updates TC summary CSV
└── utils/
    ├── spatial.py           # Geospatial operations (distance to coast, etc.)
    └── validation.py        # Statistical utilities