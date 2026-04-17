"""
Spatial operations: land masking, distance calculations, etc.
"""
import numpy as np
import geopandas as gpd
import rasterio.features
from affine import Affine
from scipy import ndimage

def create_philippine_land_mask(landmask_shp, lats, lons):
    """
    Create land mask and distance-to-coast array.
    
    Parameters
    ----------
    landmask_shp : str or Path
        Path to land polygon shapefile
    lats : np.ndarray
        Latitude array
    lons : np.ndarray
        Longitude array
    
    Returns
    -------
    mask : np.ndarray (uint8)
        Land mask (0=land, 1=sea)
    distance_to_coast : np.ndarray (float)
        Distance in pixels from coast
    """
    gdf = gpd.read_file(landmask_shp)
    
    # Check latitude ordering and ensure north-to-south
    if lats[0] < lats[-1]:
        # Lats are south-to-north, need to flip
        lats = lats[::-1]
        flip_output = True
        print("  Note: Flipping latitude array (was south-to-north)")
    else:
        flip_output = False
    
    # Calculate resolution (always positive)
    lon_res = abs(lons[1] - lons[0])
    lat_res = abs(lats[1] - lats[0])
    
    # Get bounds
    x_min = lons.min()
    y_max = lats.max()
    
    # Create standard north-up geotransform
    # Format: (x_res, 0, x_min, 0, -y_res, y_max)
    transform = Affine(lon_res, 0, x_min, 
                      0, -lat_res, y_max)
    
    print(f"  Grid: {len(lats)}x{len(lons)} pixels")
    print(f"  Resolution: {lon_res:.4f}° lon, {lat_res:.4f}° lat")
    print(f"  Bounds: lon [{lons.min():.2f}, {lons.max():.2f}], lat [{lats.min():.2f}, {lats.max():.2f}]")
    
    # Rasterize land polygons
    mask = rasterio.features.rasterize(
        [(geom, 0) for geom in gdf.geometry],
        out_shape=(len(lats), len(lons)),
        transform=transform,
        fill=1,
        all_touched=False,  # Conservative for archipelago
        dtype=np.uint8
    )
    
    # Flip back if we flipped the input
    if flip_output:
        mask = mask[::-1]
    
    # Calculate distance to coast
    distance_to_coast = ndimage.distance_transform_edt(mask)
    
    # Print statistics
    land_pixels = (mask == 0).sum()
    sea_pixels = (mask == 1).sum()
    print(f"  Land pixels: {land_pixels:,} ({land_pixels/(land_pixels+sea_pixels)*100:.1f}%)")
    print(f"  Sea pixels: {sea_pixels:,} ({sea_pixels/(land_pixels+sea_pixels)*100:.1f}%)")
    
    return mask, distance_to_coast