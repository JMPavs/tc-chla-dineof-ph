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

## Project Overview
This project provides an automated pipeline for investigating the biological response of the ocean—specifically **Chlorophyll-a (Chl-a)**—following the passage of Tropical Cyclones (TCs) within the Philippine Area of Responsibility (PAR). 

The system utilizes **DINEOF** (Data Interpolating Empirical Orthogonal Functions) to reconstruct missing satellite data. It is designed to handle a 20-year dataset (2005–2024) by bridging Windows-based data management with a Linux-based (WSL) processing engine.

## Repository Structure
- `main.py`: The central orchestrator for batch processing TCs.
- `config.py`: Global configuration for paths and DINEOF parameters.
- `modules/`:
    - `dineof_init_manager.py`: Dynamically generates DINEOF input files.
    - `post_processor.py`: Reconstructs $mg/m^3$ values from log-anomalies.
    - `tc_anomaly_analyzer.py`: Calculates post-storm impact and generates dual-axis plots.
    - `validator.py`: Performs quality control against raw MODIS data.
    - `artificial_gap_generator.py`: Conducts sensitivity testing via hidden data points.
    - `drive_mount_checker.py`: Ensures WSL can access the data drives (e.g., D: drive).

## Setup & Installation
1. **Requirements**: Install the scientific Python stack:
   ```bash
   pip install -r requirements.txt