"""
Module 3: Multivariate Preprocessor (Gold Standard - Mask Fixed)
- Fixed: Inverts Mask for DINEOF (1=Water, 0=Land)
- Fixed: "Data on Land" issue
- Fixed: Serial Processing and Date Parsing maintained
"""
import sys
from pathlib import Path
import numpy as np
import pandas as pd
import xarray as xr
from scipy import ndimage
from datetime import datetime
import re
import config

# Ensure project root is in path
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from modules.file_finder import FileFinder 

class Preprocessor:
    def __init__(self, landmask_shp=None, chl_min=0.001, chl_max=100.0,
                 log_offset=0.01, spatial_smooth=True, smooth_sigma=0.3):
        
        self.chl_min = chl_min
        self.chl_max = chl_max
        self.log_offset = log_offset
        self.spatial_smooth = spatial_smooth
        self.smooth_sigma = smooth_sigma
        
        # Load Static Depth Mask
        mask_path = Path(config.OUTPUT_DIR) / "static_land_mask.nc"
        if mask_path.exists():
            self.static_ds = xr.open_dataset(mask_path)
            # The GEBCO mask is 1=Invalid(Land), 0=Valid(Water)
            self.mask = self.static_ds['mask']
            print(f"  ✓ Loaded Static Depth Mask: {mask_path.name}")
        else:
            raise FileNotFoundError(f"CRITICAL: {mask_path} not found. Run 'generate_static_mask.py' first!")

    def _parse_date_single(self, filepath):
        """Helper: Parse date from a single file path string."""
        path_str = str(filepath)
        stem = Path(filepath).stem
        try:
            match_dir = re.search(r"(\d{4})[\\/](\d{2})[\\/](\d{2})[\\/]", path_str)
            if match_dir:
                y, m, d = match_dir.groups()
                return datetime(int(y), int(m), int(d))
            match8 = re.search(r"(\d{8})", stem)
            if match8:
                return datetime.strptime(match8.group(1), "%Y%m%d")
            if stem.startswith("A") and stem[1:8].isdigit():
                year = int(stem[1:5])
                doy = int(stem[5:8])
                return datetime.strptime(f"{year}-{doy}", "%Y-%j")
        except: pass
        return None

    def load_dataset(self, file_list):
        """Load Chl-a dataset."""
        if len(file_list) == 0: raise ValueError("No files provided")
        ds = xr.open_mfdataset(
            file_list, combine="nested", concat_dim="time", parallel=False, 
            chunks={'time': -1, 'lat': 100, 'lon': 100}, 
            decode_cf=True, mask_and_scale=True
        )
        dates = [self._parse_date_single(f) for f in file_list]
        ds = ds.assign_coords(time=pd.to_datetime(dates))
        ds = ds.sortby('time')
        
        if ds.nbytes / (1024**2) < config.MEMORY_THRESHOLD_MB:
            print(f"  Dataset size small, loading into RAM...")
            return ds.load()
        return ds

    def apply_quality_control(self, ds):
        """Apply QC and Masking."""
        print("\n=== QUALITY CONTROL ===")
        
        if self.mask.shape != ds['chlor_a'].shape[1:]:
             self.mask = self.mask.interp(lat=ds.lat, lon=ds.lon, method='nearest')
        
        # Store internal mask as 1=Invalid (Land), 0=Valid (Water) for processing logic
        self.mask = xr.where(self.mask > 0, 1, 0).astype('int8')
        ds["mask"] = (("lat", "lon"), self.mask.values)
        
        # Mask Data (Keep 0=Water)
        chl = ds["chlor_a"].where(ds["mask"] == 0)
        chl = chl.where((chl >= self.chl_min) & (chl <= self.chl_max))
        
        if self.spatial_smooth:
            print(f"  Applying Gaussian Smoothing (sigma={self.smooth_sigma})...")
            if hasattr(chl, 'values'):
                smoothed = []
                for i in range(chl.shape[0]):
                    frame = chl[i].values
                    mask = np.isfinite(frame)
                    if mask.sum() > 10:
                        filled = np.where(mask, frame, 0) 
                        blur = ndimage.gaussian_filter(filled, sigma=self.smooth_sigma)
                        smoothed.append(np.where(mask, blur, np.nan))
                    else:
                        smoothed.append(frame)
                chl.values = np.stack(smoothed)
            
        ds = ds.assign(chlor_a=chl.astype("float32"))
        return ds

    def compute_log_and_anomalies(self, ds):
        """Compute biological anomalies."""
        print("\n=== ANOMALY CALCULATION (BIOLOGICAL) ===")
        
        ds['chlor_a_log10'] = np.log10(ds['chlor_a'] + self.log_offset).astype("float32")
        
        mid_date = pd.to_datetime(ds.time.values[len(ds.time)//2])
        season = 'DJF' if mid_date.month in [12,1,2] else 'MAM' if mid_date.month in [3,4,5] else 'JJA' if mid_date.month in [6,7,8] else 'SON'
        
        clim_path = config.CLIMATOLOGY_PATHS.get(season)
        print(f"  Loading {season} Climatology: {Path(clim_path).name}")
        
        clim_ds = xr.open_dataset(clim_path)
        clim_var = list(clim_ds.data_vars.keys())[0]
        clim_data = clim_ds[clim_var]
        if 'time' in clim_data.dims: clim_data = clim_data.mean('time')
        
        if clim_data.max() > 5.0: 
            print("  Climatology appears linear. Log-transforming...")
            clim_data = np.log10(clim_data + self.log_offset)
            
        clim_interp = clim_data.interp(lat=ds.lat, lon=ds.lon, method='linear')
        
        ds['chlor_a_log10_anom_clim'] = (ds['chlor_a_log10'] - clim_interp).astype("float32")
        ds['chlor_a_log10_temporal_means_clim'] = clim_interp.astype("float32")
        
        return ds

    def compute_multivariate_matrix(self, ds_chl, start_date, end_date):
        """Loads, Regrids, and Normalizes SST & Wind."""
        print("\n=== MULTIVARIATE MATRIX ASSEMBLY (SERIAL) ===")
        
        sst_finder = FileFinder(config.SST_INDEX_CSV)
        u_finder = FileFinder(config.WIND_U_INDEX_CSV)
        v_finder = FileFinder(config.WIND_V_INDEX_CSV)
        
        sst_files = sorted(sst_finder.find_files(start_date, end_date))
        u_files = sorted(u_finder.find_files(start_date, end_date))
        v_files = sorted(v_finder.find_files(start_date, end_date))
        
        if not (sst_files and u_files and v_files):
            print("⚠ WARNING: Missing ancillary data. Proceeding with Univariate.")
            return ds_chl

        print(f"  Found {len(sst_files)} SST, {len(u_files)} U-Wind, {len(v_files)} V-Wind files.")

        def process_variable_serial(files, var_hint, target_ds):
            print(f"  Processing {var_hint} sequentially...")
            date_map = {}
            for f in files:
                d = self._parse_date_single(f)
                if d: date_map[d] = f
            avail_dates = sorted(list(date_map.keys()))
            if not avail_dates: raise ValueError(f"No valid dates for {var_hint}")

            output_frames = []
            for t in target_ds.time.values:
                t_dt = pd.to_datetime(t).to_pydatetime()
                nearest_date = min(avail_dates, key=lambda x: abs(x - t_dt))
                f_path = date_map[nearest_date]
                try:
                    with xr.open_dataset(f_path) as ds_single:
                        if 'latitude' in ds_single.coords: ds_single = ds_single.rename({'latitude': 'lat', 'longitude': 'lon'})
                        vname = [v for v in ds_single.data_vars if var_hint in v.lower()]
                        data = ds_single[vname[0]]
                        while data.ndim > 2: data = data[0]
                        regridded = data.interp(lat=target_ds.lat, lon=target_ds.lon, method='linear')
                        output_frames.append(regridded.values.astype(np.float32))
                except Exception as e:
                    shape = (len(target_ds.lat), len(target_ds.lon))
                    output_frames.append(np.full(shape, np.nan, dtype=np.float32))

            full_stack = np.stack(output_frames, axis=0)
            return xr.DataArray(full_stack, dims=['time', 'lat', 'lon'], coords={'time': target_ds.time, 'lat': target_ds.lat, 'lon': target_ds.lon})

        sst = process_variable_serial(sst_files, 'sst', ds_chl)
        u = process_variable_serial(u_files, 'u', ds_chl)
        v = process_variable_serial(v_files, 'v', ds_chl)

        print("  Normalizing variables (Z-Score)...")
        def z_score(da):
            mean = da.mean('time')
            std = da.std('time')
            std_safe = std.where(std > 0, 1.0)
            return ((da - mean) / std_safe).astype("float32")

        ds_chl['sst_anom_norm'] = z_score(sst)
        ds_chl['u_anom_norm'] = z_score(u)
        ds_chl['v_anom_norm'] = z_score(v)
        
        chl_std = ds_chl['chlor_a_log10'].std('time')
        chl_std_safe = chl_std.where(chl_std > 0, 1.0) 
        ds_chl['chlor_a_anom_norm'] = (ds_chl['chlor_a_log10_anom_clim'] / chl_std_safe).astype("float32")
        
        print("  ✓ Matrix Assembly Complete.")
        return ds_chl

    def save_dineof(self, ds, filepath, tc_info=None):
        """Save DINEOF matrix with explicit fill values."""
        
        # *** CRITICAL FIX: INVERT MASK FOR DINEOF ***
        # Your internal mask: 1=Land, 0=Water
        # DINEOF requirement: 0=Land, 1=Water (Valid)
        
        internal_mask = ds['mask'].values
        dineof_mask = 1 - internal_mask # FLIP IT: Now 0=Land, 1=Water
        
        print(f"  DINEOF Mask Stats: {np.sum(dineof_mask)} valid water pixels")

        # Fill NaNs
        fill_val = -9999.0
        
        out_vars = {
            "mask": (["lat", "lon"], dineof_mask.astype("int8"))
        }
        
        def prepare_var(da):
            # Fill NaNs with -9999.0
            return da.fillna(fill_val).astype("float32")
            
        out_vars["chlor_a_anom_norm"] = prepare_var(ds.get('chlor_a_anom_norm', ds['chlor_a_log10_anom_clim']))

        if 'sst_anom_norm' in ds:
            out_vars['sst_anom_norm'] = prepare_var(ds['sst_anom_norm'])
            out_vars['u_anom_norm'] = prepare_var(ds['u_anom_norm'])
            out_vars['v_anom_norm'] = prepare_var(ds['v_anom_norm'])
            
        ds_out = xr.Dataset(out_vars, coords=ds.coords)
        
        if 'chlor_a_log10_temporal_means_clim' in ds:
            ds_out['chlor_a_mean'] = ds['chlor_a_log10_temporal_means_clim'].astype("float32")
            ds_out['chlor_a_std'] = ds['chlor_a_log10'].std('time').astype("float32")
            
        comp = {"zlib": True, "complevel": 5, "dtype": "float32", "_FillValue": fill_val}
        enc = {k: comp for k in ds_out.data_vars if k != 'mask'}
        enc['mask'] = {"zlib": True, "dtype": "uint8"}
        
        ds_out.to_netcdf(filepath, encoding=enc)
        print(f"  ✓ DINEOF Matrix saved: {filepath.name}")

    def save(self, ds, filepath, tc_info):
        ds.to_netcdf(filepath)