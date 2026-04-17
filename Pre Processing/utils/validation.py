"""
Validation and statistical analysis utilities.
"""
import numpy as np
import xarray as xr

def compute_variability_ratios(chl_log10, distance_to_coast):
    """
    Compute temporal variability (MAD) by distance zones.
    
    Parameters
    ----------
    chl_log10 : xr.DataArray
        Log-transformed chlorophyll data (time, lat, lon)
    distance_to_coast : np.ndarray
        Distance in pixels (lat, lon)
    
    Returns
    -------
    dict
        Statistics for each distance zone
    """
    zones = {
        'very_coastal': (0, 5),
        'nearshore': (6, 15),
        'offshore': (16, 30),
        'open_ocean': (31, 1000)
    }
    
    results = {}
    
    for zone_name, (d_min, d_max) in zones.items():
        zone_mask = (distance_to_coast >= d_min) & (distance_to_coast < d_max)
        
        if zone_mask.sum() == 0:
            continue
        
        # Compute MAD for each pixel in zone
        temporal_mads = []
        
        lat_indices, lon_indices = np.where(zone_mask)
        
        for lat_idx, lon_idx in zip(lat_indices, lon_indices):
            pixel_ts = chl_log10[:, lat_idx, lon_idx].values
            valid = pixel_ts[np.isfinite(pixel_ts)]
            
            if len(valid) >= 5:
                median = np.median(valid)
                mad = np.median(np.abs(valid - median))
                if mad > 0:
                    temporal_mads.append(mad)
        
        if len(temporal_mads) > 0:
            results[zone_name] = {
                'n_pixels': len(temporal_mads),
                'median_mad': np.median(temporal_mads),
                'mean_mad': np.mean(temporal_mads),
                'std_mad': np.std(temporal_mads),
                'p25': np.percentile(temporal_mads, 25),
                'p75': np.percentile(temporal_mads, 75)
            }
    
    # Compute ratios
    if 'open_ocean' in results:
        baseline = results['open_ocean']['median_mad']
        for zone in results:
            results[zone]['ratio_to_open'] = results[zone]['median_mad'] / baseline
    
    return results

def compute_coverage_stats(data_array):
    """Compute spatial and temporal coverage statistics."""
    temporal_coverage = np.isfinite(data_array).mean(dim='time')
    spatial_coverage = np.isfinite(data_array).mean(dim=['lat', 'lon'])
    
    return {
        'mean_temporal': float(temporal_coverage.mean().values),
        'mean_spatial': float(spatial_coverage.mean().values),
        'min_temporal': float(temporal_coverage.min().values),
        'max_temporal': float(temporal_coverage.max().values)
    }