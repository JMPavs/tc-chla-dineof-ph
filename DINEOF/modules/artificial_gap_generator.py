"""
modules/artificial_gap_generator.py
"""
import numpy as np
import xarray as xr
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

class ArtificialGapGenerator:
    def __init__(self):
        np.random.seed(42)

    def create_gaps(self, input_nc_path, output_folder, percent=0.03):
        """
        Takes a PRE-PROCESSED file, hides 3% of data, saves 'corrupted' file.
        """
        input_path = Path(input_nc_path)
        output_folder = Path(output_folder)
        output_folder.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Loading original: {input_path.name}")
        ds = xr.open_dataset(input_path).load()
        
        # 1. Smart Variable Detection
        # We prioritize the standardized anomaly names, then general names
        target_vars = [
            'chlor_a_anom_norm', 
            'chlor_a_log10_anom_clim', 
            'chlorophyll_a', 
            'chlor_a'
        ]
        
        var = None
        for v in target_vars:
            if v in ds:
                var = v
                break
        
        # Fallback: Find the first 3D variable (Time, Lat, Lon)
        if var is None:
            for v in ds.data_vars:
                if len(ds[v].dims) == 3 and 'mask' not in v.lower():
                    var = v
                    break
                    
        if var is None:
            logger.error("Could not find a valid 3D chlorophyll variable to corrupt!")
            return None, None
            
        logger.info(f"Selected variable for corruption: {var}")

        # 2. Find Valid Pixels
        data = ds[var].values
        valid_coords = np.where(np.isfinite(data))
        n_valid = len(valid_coords[0])
        
        if n_valid == 0:
            logger.error("File has no valid data to hide!")
            return None, None

        # 3. Create Gaps
        n_hide = int(n_valid * percent)
        logger.info(f"Hiding {n_hide} pixels out of {n_valid} ({percent*100}%)")
        
        rand_idx = np.random.choice(n_valid, n_hide, replace=False)
        indices_to_hide = tuple(c[rand_idx] for c in valid_coords)
        
        # 4. Save Truth
        truth_arr = np.full_like(data, np.nan)
        truth_arr[indices_to_hide] = data[indices_to_hide]
        
        ds_truth = ds.copy()
        ds_truth[var].values = truth_arr
        
        # Remove other variables to save space and avoid confusion
        vars_to_drop = [v for v in ds_truth.data_vars if v != var]
        ds_truth = ds_truth.drop_vars(vars_to_drop)
        
        truth_path = output_folder / f"truth_{input_path.name}"
        ds_truth.to_netcdf(truth_path)
        
        # 5. Save Corrupted Input
        data[indices_to_hide] = np.nan
        ds[var].values = data
        
        corrupted_path = output_folder / f"corrupted_{input_path.name}"
        ds.to_netcdf(corrupted_path)
        
        ds.close()
        ds_truth.close()
        
        return corrupted_path, truth_path

    def grade_results(self, truth_path, dineof_output_path):
        """
        Compares the Truth (Hidden pixels) vs DINEOF's guess.
        """
        ds_truth = xr.open_dataset(truth_path)
        ds_filled = xr.open_dataset(dineof_output_path)
        
        # --- FIXED VARIABLE SELECTION ---
        # 1. Find Truth Variable (It should be the only one left, or use the list)
        t_var = None
        for v in ds_truth.data_vars:
            if 'mask' not in v.lower() and len(ds_truth[v].dims) >= 2:
                t_var = v
                break
        
        # 2. Find Filled Variable
        f_var = None
        for v in ds_filled.data_vars:
            if 'mask' not in v.lower() and len(ds_filled[v].dims) >= 2:
                f_var = v
                break
                
        if not t_var or not f_var:
            logger.error(f"Variable mismatch! Truth: {list(ds_truth.data_vars)}, Filled: {list(ds_filled.data_vars)}")
            return 0.0, 0.0, 0

        logger.info(f"Comparing Truth[{t_var}] vs Filled[{f_var}]")
        
        da_truth = ds_truth[t_var]
        da_filled = ds_filled[f_var]

        # --- DIMENSION ALIGNMENT ---
        # Force Filled to match Truth dimensions (e.g. Time, Lat, Lon)
        if da_filled.dims != da_truth.dims:
            # Only transpose if the set of dimensions is the same, just diff order
            if set(da_filled.dims) == set(da_truth.dims):
                logger.info(f"Aligning dims: {da_filled.dims} -> {da_truth.dims}")
                da_filled = da_filled.transpose(*da_truth.dims)
            else:
                # If dimensions are completely different, try to squeeze/expand
                # (Rare edge case)
                pass

        truth = da_truth.values
        guess = da_filled.values
        
        # Mask: Only check pixels where Truth exists (is not NaN)
        mask = np.isfinite(truth)
        
        y_true = truth[mask]
        y_pred = guess[mask]
        
        if len(y_true) == 0:
            logger.warning("No valid overlapping pixels found to grade!")
            return 0.0, 0.0, 0

        rmse = np.sqrt(np.mean((y_true - y_pred)**2))
        corr = np.corrcoef(y_true, y_pred)[0,1]
        
        ds_truth.close()
        ds_filled.close()
        
        return rmse, corr, len(y_true)