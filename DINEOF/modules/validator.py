"""
Validator Module (Smart Edition)
- Validates DINEOF reconstruction quality
- FIXED: Uses 'reindex' (nearest) for coordinate alignment to STRICTLY preserve NaNs
- ADDED: Detailed logging of Cloud vs Valid pixel counts
- UPDATED: Validation plots now show exact Raw Cloud % and Gaps Filled %
"""
import numpy as np
import xarray as xr
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
from typing import Dict, Tuple
from scipy import stats
from sklearn.metrics import mean_squared_error
import logging
import re

logger = logging.getLogger(__name__)

class Validator:
    
    LOG_OFFSET = 0.01  # Must match your Preprocessor/PostProcessor offset
    
    def __init__(self):
        pass

    def _load_raw_satellite_data_exact_times(self, tc_info: Dict, ds_post: xr.Dataset) -> xr.DataArray:
        """
        Load raw MODIS data matching EXACT time coordinates from post-processed file
        NO INTERPOLATION - preserves NaNs (clouds)
        
        Args:
            tc_info: TC information dictionary
            ds_post: Post-processed dataset (for exact time/lat/lon matching)
            
        Returns:
            xr.DataArray with raw chlor_a matching ds_post coordinates
        """
        raw_base_dir = Path(r"D:\Thesis_2\Chl-a\Chl-a L3 Mapped Custom")
        
        # Get exact time coordinates from post-processed file
        post_times = pd.to_datetime(ds_post.time.values)
        post_lats = ds_post.lat.values
        post_lons = ds_post.lon.values
        
        logger.info(f"  Target grid: {len(post_times)} times × {len(post_lats)} lats × {len(post_lons)} lons")
        
        # Initialize output array (filled with NaN)
        raw_data = np.full((len(post_times), len(post_lats), len(post_lons)), np.nan, dtype=np.float32)
        
        # Load each day
        files_found = 0
        total_raw_clouds = 0
        total_raw_valid = 0
        
        for i, time in enumerate(post_times):
            date_str = pd.Timestamp(time).strftime('%Y%m%d')
            filename = f"AQUA_MODIS.{date_str}.L3m.DAY.CHL.x_custom.nc"
            filepath = raw_base_dir / filename
            
            if not filepath.exists():
                logger.debug(f"  Missing: {filename} (keeping as NaN)")
                continue
            
            try:
                # Load daily file
                ds_daily = xr.open_dataset(filepath)
                
                # Get chlor_a variable
                if 'chlor_a' in ds_daily:
                    chl_daily = ds_daily['chlor_a']
                elif 'chlorophyll_a' in ds_daily:
                    chl_daily = ds_daily['chlorophyll_a']
                else:
                    logger.warning(f"  No chl variable in {filename}")
                    ds_daily.close()
                    continue
                
                # === CRITICAL FIX: CHECK IF COORDINATES MATCH ===
                lat_match = (len(chl_daily.lat) == len(post_lats) and 
                            np.allclose(chl_daily.lat.values, post_lats, atol=1e-5))
                lon_match = (len(chl_daily.lon) == len(post_lons) and 
                            np.allclose(chl_daily.lon.values, post_lons, atol=1e-5))
                
                if lat_match and lon_match:
                    # ✅ Perfect match - direct copy (preserves NaNs!)
                    raw_data[i, :, :] = chl_daily.values
                    
                else:
                    # ❌ Coordinates don't match - MUST use reindex, NOT interp
                    # logger.warning(f"  Coordinate mismatch for {filename} - Reindexing...")
                    
                    # Use reindex with nearest neighbor (NO interpolation across NaNs)
                    chl_reindexed = chl_daily.reindex(
                        lat=post_lats,
                        lon=post_lons,
                        method='nearest',
                        tolerance=0.1  # Only match if within 0.1 degrees
                    )
                    
                    raw_data[i, :, :] = chl_reindexed.values
                
                # Count clouds in this day
                day_data = raw_data[i, :, :]
                day_clouds = np.isnan(day_data).sum()
                day_valid = (~np.isnan(day_data)).sum()
                
                total_raw_clouds += day_clouds
                total_raw_valid += day_valid
                
                files_found += 1
                ds_daily.close()
                
            except Exception as e:
                logger.warning(f"  Failed to load {filename}: {e}")
                # import traceback
                # logger.debug(traceback.format_exc())
                continue
        
        if files_found == 0:
            logger.error("  No raw MODIS files successfully loaded!")
            return None
        
        logger.info(f"  ✓ Successfully loaded {files_found}/{len(post_times)} days")
        
        # Create DataArray with exact coordinates
        raw_chl_xr = xr.DataArray(
            raw_data,
            coords={
                'time': post_times,
                'lat': post_lats,
                'lon': post_lons
            },
            dims=['time', 'lat', 'lon'],
            name='chlor_a'
        )
        
        # Report DETAILED cloud statistics
        total_pixels = raw_data.size
        cloud_pixels = np.isnan(raw_data).sum()
        cloud_pct = (cloud_pixels / total_pixels) * 100
        
        logger.info(f"  ═══════════════════════════════════════")
        logger.info(f"  RAW MODIS STATISTICS (BEFORE DINEOF):")
        logger.info(f"  Total pixels:   {total_pixels:,}")
        logger.info(f"  Cloud pixels:   {cloud_pixels:,} ({cloud_pct:.1f}%)")
        logger.info(f"  Valid pixels:   {total_pixels - cloud_pixels:,}")
        logger.info(f"  ═══════════════════════════════════════")
        
        # CRITICAL CHECK: Verify clouds are preserved
        if cloud_pct < 10.0:
            logger.error("  ⚠️  WARNING: Less than 10% clouds detected!")
            logger.error("  This suggests NaNs are being filled during loading.")
            logger.error("  Validation results will be INVALID!")
        
        return raw_chl_xr

    def validate_tc(self, tc_info: Dict, output_dir: Path) -> Tuple[bool, Dict]:
        """
        Validate DINEOF output against TRUE raw satellite data
        """
        tc_name = f"{tc_info['year']} {tc_info['storm_name']}"
        
        try:
            logger.info(f"Validating {tc_name}...")
            
            # 1. Load Post-processed DINEOF output
            post_file = output_dir / f"chlorophyll_a_final_{tc_info['year']}_{tc_info['storm_name']}.nc"
            
            if not post_file.exists():
                logger.error(f"Post-processed file missing: {post_file}")
                return False, {}
            
            ds_post = xr.open_dataset(post_file)
            
            # 2. Load RAW MODIS data (NO FALLBACK!)
            logger.info("  Loading raw MODIS validation data...")
            
            raw_chl_xr = self._load_raw_satellite_data_exact_times(tc_info, ds_post)
            
            if raw_chl_xr is None:
                logger.error("  ✗ VALIDATION FAILED: Could not load raw MODIS data")
                logger.error("  Check that files exist in: D:\\Thesis_2\\Chl-a\\Chl-a L3 Mapped Custom\\")
                ds_post.close()
                return False, {}
            
            # Extract arrays
            raw_chl = raw_chl_xr.values
            post_chl = ds_post['chlorophyll_a'].values
            
            # Verify shapes match
            if raw_chl.shape != post_chl.shape:
                logger.error(f"  ✗ Shape mismatch! Raw: {raw_chl.shape}, Post: {post_chl.shape}")
                ds_post.close()
                return False, {}
            
            logger.info(f"  ✓ Validation arrays ready: {raw_chl.shape}")
            
            # 3. Calculate Metrics
            log_file = list(output_dir.glob("dineof*.log"))
            log_file = log_file[0] if log_file else None
            
            log_metrics = self._parse_dineof_log(log_file) if log_file else {}
            valid_metrics = self._validate_at_valid_points(raw_chl, post_chl)
            gap_metrics = self._assess_gap_filling(raw_chl, post_chl)
            gradient_metrics = self._check_spatial_coherence(post_chl)
            
            # Combine all metrics
            all_metrics = {
                **log_metrics, **valid_metrics, **gap_metrics, **gradient_metrics,
                'tc_name': tc_name, 
                'year': tc_info['year'], 
                'storm_name': tc_info['storm_name'],
                'validation_source': 'raw_modis'
            }
            
            # Evaluate Flags
            flags = self._evaluate_quality_flags(all_metrics)
            overall_status = "PASS" if all(flags.values()) else "WARNING"
            
            all_metrics.update({
                'validation_status': overall_status,
                'flags_passed': sum(flags.values()),
                'flags_total': len(flags),
                **{f'flag_{k}': v for k,v in flags.items()}
            })
            
            # 4. Save Results
            csv_file = output_dir / f"validation_metrics_{tc_info['year']}_{tc_info['storm_name']}.csv"
            pd.DataFrame([all_metrics]).to_csv(csv_file, index=False)
            
            # Create Plot
            try:
                self._create_validation_plots(
                    raw_chl, post_chl, all_metrics, 
                    output_dir / f"validation_plots_{tc_info['year']}_{tc_info['storm_name']}.png"
                )
            except Exception as plot_e:
                logger.warning(f"  Plotting failed: {plot_e}")
            
            self._print_validation_summary(all_metrics, flags, overall_status)
            
            ds_post.close()
            return True, all_metrics
            
        except Exception as e:
            logger.error(f"Validation failed for {tc_name}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False, {}

    def _check_spatial_coherence(self, data: np.ndarray) -> Dict:
        """Check spatial gradients for artifacts"""
        metrics = {}
        try:
            mean_val = np.nanmean(data)
            filled = np.nan_to_num(data, nan=mean_val)
            
            if data.ndim == 3:
                grad_mag_list = []
                for t in range(data.shape[0]):
                    gy, gx = np.gradient(filled[t])
                    grad_mag_list.append(np.mean(np.sqrt(gy**2 + gx**2)))
                mean_grad = np.mean(grad_mag_list)
            else:
                gy, gx = np.gradient(filled)
                mean_grad = np.mean(np.sqrt(gy**2 + gx**2))
                
            metrics['spatial_gradient_mean'] = mean_grad
            metrics['gradient_smooth_ok'] = mean_grad > 0.0005 
            metrics['gradient_noise_ok'] = mean_grad < 5.0
            
        except Exception as e:
            logger.warning(f"Gradient check error: {e}")
        return metrics

    def _validate_at_valid_points(self, pre, post) -> Dict:
        """Compare reconstruction against original valid points"""
        
        # Create mask where Original data exists (not cloud) AND is positive
        mask = ~np.isnan(pre) & (pre > 0)
        
        # === DEBUG LOGGING ===
        total_pixels = pre.size
        raw_clouds = np.isnan(pre).sum()
        raw_valid = mask.sum()
        
        logger.info(f"  ═══════════════════════════════════════")
        logger.info(f"  VALIDATION COMPARISON:")
        logger.info(f"  Raw total pixels:   {total_pixels:,}")
        logger.info(f"  Raw cloud pixels:   {raw_clouds:,} ({raw_clouds/total_pixels*100:.1f}%)")
        logger.info(f"  Raw valid pixels:   {raw_valid:,} ({raw_valid/total_pixels*100:.1f}%)")
        logger.info(f"  ═══════════════════════════════════════")
        
        if not mask.any(): 
            logger.error("  ✗ No valid pixels to validate!")
            return {'n_valid_points': 0}
        
        orig = pre[mask]
        recon = post[mask]
        
        # Filter out any NaNs in reconstruction
        valid = ~np.isnan(recon)
        orig, recon = orig[valid], recon[valid]
        
        if len(orig) == 0: 
            logger.error("  ✗ All reconstructed values are NaN!")
            return {'n_valid_points': 0}
        
        rmse = np.sqrt(mean_squared_error(orig, recon))
        bias = np.mean(recon - orig)
        corr, _ = stats.pearsonr(orig, recon)
        
        logger.info(f"  Comparing {len(orig):,} valid pixels...")
        
        return {
            'rmse_linear': rmse,
            'bias': bias,
            'correlation': corr,
            'r_squared': corr**2,
            'n_valid_points': len(orig)
        }

    def _assess_gap_filling(self, pre, post) -> Dict:
        gaps = np.isnan(pre)
        filled = gaps & ~np.isnan(post)
        total_gaps = np.sum(gaps)
        
        return {
            'gaps_filled_pct': (np.sum(filled)/total_gaps)*100 if total_gaps > 0 else 0,
            'total_gaps': int(total_gaps)
        }

    def _parse_dineof_log(self, log_file) -> Dict:
        if not log_file or not log_file.exists(): return {}
        try:
            with open(log_file, 'r') as f:
                content = f.read()
            
            match = re.search(r'Missing data:.*?\(([\d.]+)%\)', content)
            missing = float(match.group(1)) if match else 0
            return {'missing_data_pct': missing}
        except:
            return {}

    def _evaluate_quality_flags(self, metrics) -> Dict:
        """Evaluate quality flags for RAW MODIS validation"""
        flags = {}
        
        if 'correlation' in metrics:
            # More lenient thresholds for raw validation
            flags['High_correlation'] = metrics['correlation'] > 0.70  # Was 0.85
            flags['Low_bias'] = abs(metrics.get('bias', 999)) < 0.3    # Was 0.2
            flags['RMSE_acceptable'] = metrics.get('rmse_linear', 999) < 0.6  # Was 1.0
        
        if 'gradient_smooth_ok' in metrics:
            flags['Spatial_Coherence'] = metrics['gradient_smooth_ok']
        
        # NEW: Check gap filling performance
        if 'gaps_filled_pct' in metrics:
            flags['Adequate_Gap_Filling'] = metrics['gaps_filled_pct'] > 70.0
            
        return flags

    def _print_validation_summary(self, metrics, flags, status):
        print(f"  -> Validated {metrics.get('storm_name')}: R²={metrics.get('r_squared', 0):.3f} | RMSE={metrics.get('rmse_linear', 0):.3f} | {status}")

    def _create_validation_plots(self, pre, post, metrics, filename):
        """
        Creates a Scatter Plot where point color is determined by local density.
        Updates: Now calculates and displays Raw Cloud % and Gaps Filled % specifically.
        """
        plt.style.use('default') 
        
        fig = plt.figure(figsize=(14, 6))
        gs = fig.add_gridspec(1, 2, width_ratios=[1.5, 1])
        ax1 = fig.add_subplot(gs[0])
        ax2 = fig.add_subplot(gs[1])
        
        # 1. Prepare Data
        mask = ~np.isnan(pre) & ~np.isnan(post) & (pre > 0)
        
        if mask.any():
            x = pre[mask]
            y = post[mask]
            
            # --- SUBSAMPLE FOR SPEED ---
            # KDE calculation is heavy. We use 50k points which is statistically representative.
            MAX_POINTS = 50000 
            if len(x) > MAX_POINTS:
                idx = np.random.choice(len(x), MAX_POINTS, replace=False)
                x_plot, y_plot = x[idx], y[idx]
            else:
                x_plot, y_plot = x, y
            
            # --- 2. CALCULATE DENSITY ---
            # Calculate the point density
            xy = np.vstack([x_plot, y_plot])
            z = stats.gaussian_kde(xy)(xy)
            
            # Sort the points by density, so the densest points are plotted last (on top)
            idx = z.argsort()
            x_plot, y_plot, z = x_plot[idx], y_plot[idx], z[idx]
            
            # --- 3. SCATTER PLOT ---
            sc = ax1.scatter(x_plot, y_plot, c=z, s=5, cmap='inferno', edgecolor='none')
            
            # Add Colorbar
            cb = plt.colorbar(sc, ax=ax1, fraction=0.046, pad=0.04)
            cb.set_label('Point Density (Gaussian KDE)', fontsize=10)
            
            # --- REFERENCE LINES ---
            max_val = max(np.nanpercentile(x, 99.9), np.nanpercentile(y, 99.9))
            
            # 1:1 Line
            ax1.plot([0, max_val], [0, max_val], 'k--', linewidth=1.5, alpha=0.7, label='1:1 Line')
            
            # Best Fit
            if len(x) > 1:
                slope, intercept = np.polyfit(x, y, 1)
                fit_line = slope * np.array([0, max_val]) + intercept
                ax1.plot([0, max_val], fit_line, color='cyan', linewidth=1.5, label=f'Best Fit (m={slope:.2f})')
            
            # Formatting
            ax1.set_xlabel('Original Chlorophyll-a ($mg/m^3$)', fontsize=12, fontweight='bold')
            ax1.set_ylabel('Reconstructed Chlorophyll-a ($mg/m^3$)', fontsize=12, fontweight='bold')
            ax1.set_title(f"Validation Scatter: {metrics.get('storm_name', 'TC')}", fontsize=14)
            ax1.legend(loc='upper left', frameon=True)
            
            ax1.set_xlim(0, max_val)
            ax1.set_ylim(0, max_val)
            ax1.set_aspect('equal')
            ax1.grid(True, linestyle=':', alpha=0.4)

        # --- METRICS PANEL (UPDATED) ---
        ax2.axis('off')
        title_text = "Validation Statistics"
        ax2.text(0.05, 0.9, title_text, fontsize=16, fontweight='bold', color='#333333')
        
        # CALCULATE RAW CLOUD PERCENTAGE DIRECTLY FROM DATA
        # This is more accurate than relying on the log file parser
        raw_cloud_pct = (np.isnan(pre).sum() / pre.size) * 100
        
        stats_text = (
            f"STORM: {metrics.get('storm_name', 'Unknown')}\n"
            f"YEAR:  {metrics.get('year', 'Unknown')}\n\n"
            f"STATUS: {metrics.get('validation_status', 'UNKNOWN')}\n"
            f"--------------------------\n"
            f"R² Score:    {metrics.get('r_squared', 0):.4f}\n"
            f"Correlation: {metrics.get('correlation', 0):.4f}\n"
            f"RMSE:        {metrics.get('rmse_linear', 0):.4f} $mg/m^3$\n"
            f"Bias:        {metrics.get('bias', 0):.4f}\n"
            f"--------------------------\n"
            f"Valid Pixels: {metrics.get('n_valid_points', 0):,}\n"
            f"Raw Clouds:   {raw_cloud_pct:.1f}%\n"
            f"Gaps Filled:  {metrics.get('gaps_filled_pct', 0):.1f}%"
        )
        
        ax2.text(0.05, 0.85, stats_text, fontsize=12, family='monospace',
                 verticalalignment='top', bbox=dict(boxstyle='round,pad=1', facecolor='#f8f9fa', alpha=0.5))

        plt.tight_layout()
        plt.savefig(filename, dpi=300)
        plt.close()