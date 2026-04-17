"""
Script: Static Mask Generator (GEBCO -> 4km Grid)
Purpose: Creates the <30m depth mask required by the Gold Standard.
"""
import xarray as xr
import numpy as np
from pathlib import Path
import config

def generate_mask():
    print("=== Generating Static Depth Mask ===")
    
    # 1. Load a Template to get the target 4km Grid
    # We use the FIRST file found in your Chl-a folder as the master grid
    template_path = Path(r"D:\Thesis_2\Chl-a\Chl-a L3 Mapped Custom").glob("*.nc")
    try:
        template_file = next(template_path)
    except StopIteration:
        print("Error: No files found in 'Chl-a L3 Mapped Custom'. Run L3Transform.py first!")
        return

    print(f"Template Grid Source: {template_file.name}")
    ds_template = xr.open_dataset(template_file)
    target_lat = ds_template['lat']
    target_lon = ds_template['lon']

    # 2. Load GEBCO
    print(f"Loading GEBCO: {config.BATHYMETRY_FILE}")
    ds_bathy = xr.open_dataset(config.BATHYMETRY_FILE)
    z_var = 'elevation' if 'elevation' in ds_bathy else list(ds_bathy.data_vars)[0]
    
    # 3. Interpolate to 4km
    print("Interpolating bathymetry...")
    bathy_interp = ds_bathy[z_var].interp(lat=target_lat, lon=target_lon, method='linear')

    # 4. Create Mask (<30m depth) [cite: 157]
    # GEBCO: Negative = Underwater. 
    # Mask where: Elevation >= -30 (Land OR Shallow)
    print(f"Masking depths > {config.DEPTH_CUTOFF}m...")
    invalid_mask = (bathy_interp >= config.DEPTH_CUTOFF)
    
    # 5. Save
    output_path = Path(config.OUTPUT_DIR) / "static_land_mask.nc"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    ds_out = xr.Dataset(
        {
            "mask": (("lat", "lon"), invalid_mask.values.astype("uint8")),
            "bathymetry": (("lat", "lon"), bathy_interp.values.astype("float32"))
        },
        coords={"lat": target_lat, "lon": target_lon}
    )
    ds_out.to_netcdf(output_path)
    print(f"✓ Mask saved to: {output_path}")

if __name__ == "__main__":
    generate_mask()