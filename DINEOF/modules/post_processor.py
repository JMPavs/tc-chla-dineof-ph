"""
Post-processing module for DINEOF reconstructed data
FIXED: Reads Stats from the specific Pre-Process location (tc_info['windows_path'])
       instead of assuming it is in the Output folder.
"""
import numpy as np
import xarray as xr
import logging
from pathlib import Path
from typing import Dict, Tuple

logger = logging.getLogger(__name__)

class PostProcessor:
    
    LOG_OFFSET = 0.01
    
    def __init__(self):
        pass
    
    def process_tc(self, tc_info: Dict, output_dir: Path) -> Tuple[bool, str]:
        tc_name = f"{tc_info['year']} {tc_info['storm_name']}"
        
        try:
            logger.info(f"Post-processing {tc_name}...")
            
            # 1. Define File Paths
            
            # A. DINEOF Output (The result inside the Output folder)
            dineof_file = output_dir / "dineof_chlor_a.nc"
            
            # Fallback for legacy naming if the simple name doesn't exist
            if not dineof_file.exists():
                dineof_file = output_dir / f"dineof_chlor_a_{tc_info['year']}_{tc_info['storm_name']}.nc"
            
            # B. Stats File (The Input File)
            # *** CRITICAL FIX ***
            # Do NOT look in output_dir. Use the path provided by the CSV (Pre Process folder)
            stats_file = Path(tc_info['windows_path'])
            
            # C. Final Output
            post_process_file = output_dir / f"chlorophyll_a_final_{tc_info['year']}_{tc_info['storm_name']}.nc"
            
            # Debugging Logs
            logger.info(f"  Looking for DINEOF Output: {dineof_file}")
            logger.info(f"  Looking for Stats/Input:   {stats_file}")
            
            if not dineof_file.exists():
                logger.error(f"DINEOF output missing: {dineof_file}")
                return False, ""
                
            if not stats_file.exists():
                logger.error(f"Stats/Input file missing at: {stats_file}")
                logger.error("Check your CSV 'Pre Process File' column!")
                return False, ""
            
            # 2. Load Data
            ds_dineof = xr.open_dataset(dineof_file)
            ds_stats = xr.open_dataset(stats_file)
            
            # 3. Extract Variables
            dineof_data = self._standardize_dims(ds_dineof['chlor_a'])
            
            # Find Stats Variables
            if 'chlor_a_mean' in ds_stats:
                mean_field = ds_stats['chlor_a_mean']
                std_field = ds_stats['chlor_a_std']
            elif 'chlor_a_log10_temporal_means_clim' in ds_stats:
                mean_field = ds_stats['chlor_a_log10_temporal_means_clim']
                std_field = ds_stats.get('chlor_a_std', 1.0)
            else:
                raise ValueError("Could not find Mean/Std stats in input file")

            mask = ds_stats['mask']
            
            # 4. Align Coordinates
            dineof_data = dineof_data.assign_coords(
                time=ds_stats.time,
                lat=ds_stats.lat,
                lon=ds_stats.lon
            )
            
            # 5. Reconstruction
            logger.info("Reconstructing data...")
            log_anomaly = dineof_data * std_field
            log_total = log_anomaly + mean_field
            chlor_a_mg_m3 = (10**log_total) - self.LOG_OFFSET
            
            # 6. Cleanup
            mask_binary = xr.where(mask > 0.5, 1, 0)
            chlor_a_mg_m3 = chlor_a_mg_m3.where(mask_binary == 1)
            chlor_a_mg_m3 = xr.where(chlor_a_mg_m3 < 0, 0.001, chlor_a_mg_m3)
            chlor_a_mg_m3 = xr.where(chlor_a_mg_m3 > 200, 200.0, chlor_a_mg_m3)

            # 7. Save
            ds_output = xr.Dataset({'chlorophyll_a': chlor_a_mg_m3.astype('float32')})
            ds_output.attrs = {
                'title': 'Reconstructed Chlorophyll-a',
                'method': 'DINEOF Multivariate',
                'storm': tc_name,
                'units': 'mg m-3'
            }
            
            comp = {"zlib": True, "complevel": 5}
            ds_output.to_netcdf(post_process_file, encoding={'chlorophyll_a': comp})
            
            logger.info(f"✓ Saved: {post_process_file.name}")
            
            ds_dineof.close()
            ds_stats.close()
            return True, str(post_process_file)
            
        except Exception as e:
            logger.error(f"Post-processing failed: {e}")
            return False, ""

    def _standardize_dims(self, da):
        rename_map = {}
        for d in da.dims:
            d_str = str(d)
            if 'time' in d_str or 'dim003' in d_str: rename_map[d] = 'time'
            elif 'lat' in d_str or 'dim002' in d_str: rename_map[d] = 'lat'
            elif 'lon' in d_str or 'dim001' in d_str: rename_map[d] = 'lon'
        if rename_map:
            return da.rename(rename_map)
        return da