"""
Module 1: TC Selection and Temporal Window Definition
"""
import pandas as pd
import geopandas as gpd
from datetime import timedelta
from shapely.geometry import Point

class TCSelector:
    def __init__(self, ibtracs_path, par_shapefile, buffer_days=28):
        self.df = pd.read_csv(ibtracs_path)
        
        # Parse ISO_TIME as DATE
        self.df['DATE'] = pd.to_datetime(self.df['ISO_TIME'], errors='coerce')
        self.df = self.df.dropna(subset=['DATE'])
        
        # Convert SEASON (float) to integer YEAR
        self.df['YEAR'] = self.df['SEASON'].astype(int)
        
        self.gdf_par = gpd.read_file(par_shapefile).to_crs(epsg=4326)
        self.buffer_days = buffer_days
        
    def get_tc_list(self, year_start, year_end):
        """Get list of unique TCs in year range that entered PAR."""
        df_range = self.df[
            (self.df['YEAR'] >= year_start) & 
            (self.df['YEAR'] <= year_end)
        ].copy()
        
        # Check PAR entry
        df_range['geometry'] = df_range.apply(
            lambda r: Point(r['LON'], r['LAT']), axis=1
        )
        gdf = gpd.GeoDataFrame(df_range, geometry='geometry', crs="EPSG:4326")
        gdf['IN_PAR'] = gdf.geometry.within(self.gdf_par.union_all())
        
        # Get unique TCs that entered PAR
        tc_in_par = gdf[gdf['IN_PAR']].groupby(['NAME', 'YEAR']).size()
        
        return [(name, year) for (name, year), _ in tc_in_par.items()]
    
    def get_tc_window(self, name, year):
        """Get processing window for a specific TC."""
        df_tc = self.df[
            (self.df['NAME'] == name) & 
            (self.df['YEAR'] == year)
        ].copy()
        
        if df_tc.empty:
            return None
        
        # Get PAR entry/exit dates
        df_tc['geometry'] = df_tc.apply(
            lambda r: Point(r['LON'], r['LAT']), axis=1
        )
        gdf_tc = gpd.GeoDataFrame(df_tc, geometry='geometry', crs="EPSG:4326")
        gdf_tc['IN_PAR'] = gdf_tc.geometry.within(self.gdf_par.union_all())
        gdf_tc_in = gdf_tc[gdf_tc['IN_PAR']]
        
        if gdf_tc_in.empty:
            return None
        
        par_start = gdf_tc_in['DATE'].min().normalize()
        par_end = gdf_tc_in['DATE'].max().normalize()
        
        # Add buffer
        start_date = par_start - timedelta(days=self.buffer_days)
        end_date = par_end + timedelta(days=self.buffer_days)
        
        tc_start = df_tc['DATE'].min()
        tc_end = df_tc['DATE'].max()
        
        return {
            'tc_name': name,
            'year': year,
            'tc_start': tc_start,
            'tc_end': tc_end,
            'par_start': par_start,
            'par_end': par_end,
            'par_days': (par_end - par_start).days + 1,
            'window_start': start_date,
            'window_end': end_date,
            'total_days': (end_date - start_date).days + 1
        }