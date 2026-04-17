"""
TC Anomaly Analyzer Module (Full Swath Edition)
- Fixed: Suppresses DtypeWarning (low_memory=False)
- Fixed: Robust Column Matching for IBTrACS
- Feature: Saves both Masked (Corridor) and Unmasked (Full Swath) Anomalies
- UPDATED: Dual Y-axis plot with Percentage Change (left) and Absolute Anomaly (right)
"""
import numpy as np
import pandas as pd
import xarray as xr
import geopandas as gpd
import rasterio
import rasterio.features
from rasterio.transform import from_bounds
from pathlib import Path
from typing import Dict, Tuple, Optional
from shapely.geometry import LineString, Polygon, Point
import logging
import matplotlib.pyplot as plt
from matplotlib.gridspec import GridSpec
import cartopy.crs as ccrs
import cartopy.feature as cfeature

logger = logging.getLogger(__name__)

class TCAnomalyAnalyzer:
    
    def __init__(self, par_shapefile_path: str, ibtracs_csv_path: str,
                 climatology_paths_dict: dict):
        self.par_shapefile = par_shapefile_path
        self.ibtracs_csv = ibtracs_csv_path
        self.climatology_paths_dict = climatology_paths_dict
        
        try:
            self.gdf_par = gpd.read_file(par_shapefile_path)
            if self.gdf_par.crs is None:
                self.gdf_par = self.gdf_par.set_crs(epsg=4326)
            elif self.gdf_par.crs.to_string() != "EPSG:4326":
                self.gdf_par = self.gdf_par.to_crs(epsg=4326)
            self.par_union = self.gdf_par.union_all()
        except Exception as e:
            logger.error(f"Failed to load PAR shapefile: {e}")
            raise

        logger.info("TC Anomaly Analyzer initialized")

    def _get_season(self, date: pd.Timestamp) -> str:
        month = date.month
        if month in [12, 1, 2]: return 'DJF'
        elif month in [3, 4, 5]: return 'MAM'
        elif month in [6, 7, 8]: return 'JJA'
        else: return 'SON'
    
    def analyze_tc(self, tc_info: Dict, post_processed_file: Path, 
                   output_dir: Path,
                   pre_tc_days: int = 7,
                   analysis_days: int = 21,
                   left_offset_km: float = 100,
                   right_offset_km: float = 200,
                   grid_resolution: float = 0.04,
                   bloom_threshold: float = 0.5) -> Tuple[bool, str]:
        
        tc_name = f"{tc_info['year']} {tc_info['storm_name']}"
        
        try:
            logger.info(f"--- Analyzing Impact: {tc_name} ---")
            
            # 1. Load Data
            if not post_processed_file.exists():
                logger.error(f"[Fail] Post-processed file missing: {post_processed_file}")
                return False, ""
                
            ds_chl = xr.open_dataset(post_processed_file)
            if 'chlorophyll_a' in ds_chl: chl = ds_chl['chlorophyll_a']
            elif 'chlor_a' in ds_chl: chl = ds_chl['chlor_a']
            else:
                logger.error("[Fail] No 'chlorophyll_a' variable in dataset")
                return False, ""

            # 2. Track & Dates
            track_data = self._load_tc_track(tc_info)
            if track_data is None: return False, ""
            track_lons, track_lats, par_entry, par_exit = track_data
            par_entry = par_entry.normalize()
            
            # 3. Climatology
            season = self._get_season(par_entry)
            clim_path = self.climatology_paths_dict.get(season)
            
            if not clim_path or not Path(clim_path).exists():
                logger.error(f"[Fail] Climatology file missing: {clim_path}")
                return False, ""
            
            ds_clim = xr.open_dataset(clim_path)
            # Get the variable (usually 'chl_a' or 'chlor_a')
            clim_var_name = list(ds_clim.data_vars)[0]
            clim_raw = ds_clim[clim_var_name]
            
            # Align Climatology to Storm Grid
            logger.info(f"Aligning {season} Climatology to storm grid...")
            clim_aligned = clim_raw.interp_like(chl, method='linear')
            
            # 4. Bresenham Corridor
            tc_polygon, mask_xr = self._create_bresenham_corridor(
                track_lons, track_lats, chl, left_offset_km, right_offset_km, grid_resolution
            )
            if tc_polygon is None: return False, ""
            
            # 5. Time Windows
            pre_start = par_entry - pd.Timedelta(days=pre_tc_days)
            anom_end = par_entry + pd.Timedelta(days=analysis_days)
            
            try:
                chl_full = chl.sel(time=slice(pre_start, anom_end))
                base_end = par_entry - pd.Timedelta(days=1)
                chl_baseline = chl.sel(time=slice(pre_start, base_end))
                
                if chl_baseline.time.size == 0: return False, ""
            except Exception: return False, ""

            # 6. Anomalies
            logger.info("Calculating anomalies...")
            
            # A. Absolute Anomaly (mg/m3) - Based on Pre-Storm Baseline
            baseline_mean = chl_baseline.mean(dim='time', skipna=True)
            chl_anom = chl_full - baseline_mean
            
            # B. Percent Anomaly (%) - Based on Long-Term Climatology
            # Formula: (Absolute Anomaly / Climatology) * 100
            pct_anom = (chl_anom / clim_aligned.where(clim_aligned > 0.001)) * 100
            
            # Masking (These are the corridor-only versions)
            chl_anom_masked = chl_anom.where(mask_xr)
            pct_anom_masked = pct_anom.where(mask_xr)
            
            # 7. Statistics
            dims = ['lat', 'lon']
            
            # Create Stats Dataset
            stats_ds = chl_anom_masked.mean(dim=dims).to_dataset(name='chl_a_anom_mean')
            stats_ds['chl_a_anom_std'] = chl_anom_masked.std(dim=dims)
            stats_ds['pct_anom_mean'] = pct_anom_masked.mean(dim=dims)
            
            # Bloom Fractions
            valid_pixels = chl_anom_masked.notnull().sum(dim=dims)
            bloom_pixels = (chl_anom_masked > bloom_threshold).sum(dim=dims)
            
            with np.errstate(divide='ignore', invalid='ignore'):
                stats_ds['bloom_fraction'] = bloom_pixels / valid_pixels
                stats_ds['data_coverage_pct'] = (valid_pixels / int(mask_xr.sum())) * 100
            
            # Export Stats CSV
            stats_df = stats_ds.to_dataframe().reset_index()
            stats_df['days_from_par_entry'] = (stats_df['time'] - par_entry).dt.days
            stats_df['season'] = season  # Add season column (DJF/MAM/JJA/SON)
            stats_df['tc_name'] = tc_info['storm_name']  # Add TC name for reference
            stats_df['tc_year'] = tc_info['year']  # Add year for reference
            
            # 8. Save Files (UPDATED: Now includes full swath variables)
            ds_output = xr.Dataset({
                # --- The Masked Data (Corridor Only) ---
                'chl_a_anomaly_corridor': chl_anom_masked.astype('float32'),
                'pct_anomaly_corridor': pct_anom_masked.astype('float32'),
                
                # --- NEW: The Whole Swath Data (Unmasked) ---
                'chl_a_anomaly_full': chl_anom.astype('float32'),
                'pct_anomaly_full': pct_anom.astype('float32'),
                
                # --- The Mask Itself ---
                'corridor_mask': mask_xr.astype('int8')
            })
            
            output_file = output_dir / f"tc_anomaly_{tc_info['year']}_{tc_info['storm_name']}.nc"
            ds_output.to_netcdf(output_file)
            
            csv_file = output_dir / f"tc_anomaly_stats_{tc_info['year']}_{tc_info['storm_name']}.csv"
            stats_df.to_csv(csv_file, index=False)
            
            # 9. Plotting
            try:
                self._create_anomaly_plots(stats_df, tc_info, ds_output, output_dir, par_entry, par_exit)
            except Exception: pass

            logger.info("✓ Analysis complete.")
            ds_chl.close()
            ds_clim.close()
            return True, str(output_file)
            
        except Exception as e:
            logger.error(f"Analysis crashed: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False, ""

    def _load_tc_track(self, tc_info: Dict) -> Optional[Tuple]:
        try:
            # Fix DtypeWarning by setting low_memory=False
            df_tc = pd.read_csv(self.ibtracs_csv, low_memory=False)
            
            # Standardize columns
            df_tc.columns = df_tc.columns.str.upper()
            
            # Find storm
            storm_name = tc_info['storm_name'].upper()
            year = tc_info['year']
            
            # Handle column name variations
            name_col = 'NAME' if 'NAME' in df_tc.columns else 'STORM_NAME'
            year_col = 'YEAR' if 'YEAR' in df_tc.columns else 'SEASON'
            
            mask = (df_tc[name_col] == storm_name) & (df_tc[year_col] == year)
            df_storm = df_tc[mask].copy()
            
            if df_storm.empty:
                logger.warning(f"Storm {storm_name} ({year}) not found in IBTrACS.")
                return None
            
            # Convert Lat/Lon to numeric (handle errors)
            df_storm['LAT'] = pd.to_numeric(df_storm['LAT'], errors='coerce')
            df_storm['LON'] = pd.to_numeric(df_storm['LON'], errors='coerce')
            df_storm = df_storm.dropna(subset=['LAT', 'LON'])
            
            # Create Geometry
            df_storm['geometry'] = df_storm.apply(lambda r: Point(r['LON'], r['LAT']), axis=1)
            gdf_tc = gpd.GeoDataFrame(df_storm, geometry='geometry', crs="EPSG:4326")
            
            # Clip to PAR
            gdf_tc_in = gdf_tc[gdf_tc.geometry.within(self.par_union)]
            
            if gdf_tc_in.empty:
                logger.warning(f"Storm {storm_name} track did not intersect PAR.")
                return None
            
            # Extract data
            par_entry = pd.to_datetime(gdf_tc_in['ISO_TIME']).min()
            par_exit = pd.to_datetime(gdf_tc_in['ISO_TIME']).max()
            
            # Return sorted track points
            gdf_tc_in = gdf_tc_in.sort_values('ISO_TIME')
            
            return gdf_tc_in['LON'].values, gdf_tc_in['LAT'].values, par_entry, par_exit
            
        except Exception as e:
            logger.error(f"Track loading error: {e}")
            return None

    def _create_bresenham_corridor(self, track_lons, track_lats, chl_data, left_km, right_km, resolution):
        try:
            lon_grid, lat_grid = chl_data.lon.values, chl_data.lat.values
            points = []
            for i in range(len(track_lons) - 1):
                seg = self._bresenham_line_geo(track_lons[i], track_lats[i], track_lons[i+1], track_lats[i+1],
                                             lon_grid, lat_grid, resolution)
                if i == 0: points.extend(seg)
                else: points.extend(seg[1:])
            
            if len(points) < 2: return None, None
            
            left_deg, right_deg = left_km/111.0, right_km/111.0
            line = LineString(points)
            
            # Buffer and Clip
            tc_polygon = line.buffer((left_deg + right_deg)/2.0).intersection(self.par_union)
            
            transform = from_bounds(lon_grid.min(), lat_grid.min(), lon_grid.max(), lat_grid.max(), len(lon_grid), len(lat_grid))
            mask_arr = rasterio.features.rasterize([(tc_polygon, 1)], out_shape=(len(lat_grid), len(lon_grid)),
                                                 transform=transform, fill=0, all_touched=True, dtype=rasterio.uint8).astype(bool)
            
            return tc_polygon, xr.DataArray(mask_arr, coords=[lat_grid, lon_grid], dims=["lat", "lon"])
        except Exception as e:
            logger.error(f"Corridor error: {e}")
            return None, None

    @staticmethod
    def _bresenham_line_geo(lon1, lat1, lon2, lat2, lon_grid, lat_grid, resolution):
        x1 = int(round((lon1 - lon_grid.min()) / resolution))
        y1 = int(round((lat1 - lat_grid.min()) / resolution))
        x2 = int(round((lon2 - lon_grid.min()) / resolution))
        y2 = int(round((lat2 - lat_grid.min()) / resolution))
        points = []
        dx, dy = abs(x2 - x1), abs(y2 - y1)
        sx, sy = (1 if x1 < x2 else -1), (1 if y1 < y2 else -1)
        err = dx - dy
        x, y = x1, y1
        
        # Safety break
        max_iter = 10000
        count = 0
        
        while True and count < max_iter:
            glon = lon_grid.min() + x * resolution
            glat = lat_grid.min() + y * resolution
            if lon_grid.min() <= glon <= lon_grid.max() and lat_grid.min() <= glat <= lat_grid.max():
                points.append((glon, glat))
            if x == x2 and y == y2: break
            e2 = 2 * err
            if e2 > -dy: err -= dy; x += sx
            if e2 < dx: err += dx; y += sy
            count += 1
            
        return points

    def _create_anomaly_plots(self, stats_df, tc_info, ds_output, output_dir, 
                            par_entry_date, par_exit_date):
        """
        Generate dashboard plot with DUAL Y-AXIS:
        - Left axis: Percentage Change (%)
        - Right axis: Absolute Chl-a Anomaly (mg/m³)
        Both sharing the same zero baseline
        """
        tc_name = f"{tc_info['year']} {tc_info['storm_name']}"
        plot_df = stats_df[stats_df['days_from_par_entry'] <= 21].copy()
        
        fig = plt.figure(figsize=(16, 8))
        gs = GridSpec(2, 2, figure=fig)
        
        # ========================================
        # 1. DUAL Y-AXIS TIME SERIES (UPDATED)
        # ========================================
        ax1 = fig.add_subplot(gs[0, :])
        ax1_right = ax1.twinx()  # Create second y-axis
        
        x = plot_df['days_from_par_entry']
        
        # LEFT AXIS: Percentage Change
        color_pct = 'darkgreen'
        line_pct = ax1.plot(x, plot_df['pct_anom_mean'], 
                           color=color_pct, marker='o', linewidth=2, 
                           label='Percentage Change (%)', zorder=3)
        ax1.set_ylabel('Percentage Change (%)', color=color_pct, fontsize=12, fontweight='bold')
        ax1.tick_params(axis='y', labelcolor=color_pct)
        ax1.grid(True, alpha=0.3, linestyle='--')
        
        # RIGHT AXIS: Absolute Anomaly
        color_abs = 'darkblue'
        line_abs = ax1_right.plot(x, plot_df['chl_a_anom_mean'], 
                                 color=color_abs, marker='s', linewidth=2, 
                                 label='Absolute Anomaly (mg/m³)', zorder=2)
        
        # Add uncertainty band for absolute anomaly
        ax1_right.fill_between(x, 
                              plot_df['chl_a_anom_mean'] - plot_df['chl_a_anom_std'],
                              plot_df['chl_a_anom_mean'] + plot_df['chl_a_anom_std'],
                              alpha=0.2, color=color_abs, zorder=1)
        
        ax1_right.set_ylabel('Absolute Chl-a Anomaly (mg/m³)', color=color_abs, 
                            fontsize=12, fontweight='bold')
        ax1_right.tick_params(axis='y', labelcolor=color_abs)
        
        # CRITICAL: Synchronize zero lines for both axes
        # Get the limits
        ylim_pct = ax1.get_ylim()
        ylim_abs = ax1_right.get_ylim()
        
        # Calculate symmetric limits so zero is centered
        max_pct = max(abs(ylim_pct[0]), abs(ylim_pct[1]))
        max_abs = max(abs(ylim_abs[0]), abs(ylim_abs[1]))
        
        ax1.set_ylim(-max_pct, max_pct)
        ax1_right.set_ylim(-max_abs, max_abs)
        
        # Zero lines
        ax1.axhline(0, color='gray', linestyle=':', linewidth=1.5, zorder=0)
        ax1.axvline(0, color='red', linestyle='--', linewidth=2, 
                   label='PAR Entry', zorder=4)
        
        # Combine legends
        lines = line_pct + line_abs
        labels = [l.get_label() for l in lines]
        ax1.legend(lines + [plt.Line2D([0], [0], color='red', linestyle='--', linewidth=2)],
                  labels + ['PAR Entry'], 
                  loc='upper left', framealpha=0.9, fontsize=10)
        
        ax1.set_xlabel('Days from PAR Entry', fontsize=12)
        ax1.set_title(f'{tc_name} Impact Analysis - Dual Y-Axis View', 
                     fontsize=14, fontweight='bold', pad=15)
        
        # ========================================
        # 2. Bloom Fraction
        # ========================================
        ax2 = fig.add_subplot(gs[1, 0])
        ax2.bar(x, plot_df['bloom_fraction']*100, color='coral', edgecolor='darkred')
        ax2.set_ylabel('% Area > Threshold', fontsize=11)
        ax2.set_xlabel('Days from Entry', fontsize=11)
        ax2.set_title('Bloom Extent', fontsize=12, fontweight='bold')
        ax2.grid(True, alpha=0.3, axis='y')
        
        # ========================================
        # 3. Map (Peak Anomaly)
        # ========================================
        try:
            ax3 = fig.add_subplot(gs[1, 1], projection=ccrs.PlateCarree())
            peak_idx = plot_df['chl_a_anom_mean'].idxmax()
            peak_time = pd.to_datetime(plot_df.loc[peak_idx, 'time'])
            
            chl_map = ds_output['chl_a_anomaly_corridor'].sel(time=peak_time, method='nearest')
            
            im = ax3.pcolormesh(chl_map.lon, chl_map.lat, chl_map, 
                               transform=ccrs.PlateCarree(), 
                               cmap='RdBu_r', vmin=-1, vmax=1)
            ax3.coastlines(linewidth=0.5)
            ax3.add_feature(cfeature.BORDERS, linewidth=0.3, alpha=0.5)
            
            cbar = plt.colorbar(im, ax=ax3, label='Chl-a Anomaly (mg/m³)', 
                               shrink=0.8, pad=0.05)
            ax3.set_title(f'Peak Anomaly Map\n{peak_time.date()}', 
                         fontsize=11, fontweight='bold')
            
            # Add gridlines
            gl = ax3.gridlines(draw_labels=True, linewidth=0.5, 
                              color='gray', alpha=0.3, linestyle='--')
            gl.top_labels = False
            gl.right_labels = False
            
        except Exception as e:
            logger.warning(f"Map plot failed: {e}")

        plt.tight_layout()
        
        # Save
        plot_file = output_dir / f"tc_anomaly_plots_{tc_info['year']}_{tc_info['storm_name']}.png"
        plt.savefig(plot_file, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"✓ Plot saved: {plot_file.name}")

    def _print_analysis_summary(self, stats_df, tc_info, par_entry, par_exit):
        pass