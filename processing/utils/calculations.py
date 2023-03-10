from typing import Dict

import regionmask
import xarray as xr
import geopandas as gpd
from tqdm import tqdm


class ZonalStatistics:
    def __init__(self, raster_data: xr.Dataset(), vector_data: Dict[str, gpd.GeoDataFrame]):
        self.raster_data = raster_data
        self.vector_data = vector_data

    def rasterize_vector_data(self, index_column_name: str = 'index',
                              x_coor_name: str = 'lon', y_coor_name: str = 'lat'):
        """Rasterize a GeoDataFrame using xarray Dataset
        as a reference and add it as a new variable"""
        for mask_name, gdf in tqdm(self.vector_data.items()):
            mask = regionmask.mask_geopandas(
                gdf,
                self.raster_data[x_coor_name],
                self.raster_data[y_coor_name],
                numbers=index_column_name
            )

            self.raster_data[mask_name] = mask

        return self.raster_data

