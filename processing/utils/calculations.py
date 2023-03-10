from typing import Dict

import pandas as pd
import regionmask
import xarray as xr
import geopandas as gpd
import dask.array as da
from tqdm import tqdm

from processing.utils.data import RasterData


class ZonalStatistics:
    def __init__(self, raster_data: xr.Dataset, vector_data: Dict[str, gpd.GeoDataFrame]):
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

    def compute_change(self, raster_obj: RasterData, index_column_name: str = 'index') -> Dict[str, pd.DataFrame]:
        df_list = []
        change_data = {}
        for geom_name, gdf in self.vector_data.items():
            print(geom_name)
            if raster_obj.iso() and ('political' in geom_name):
                gdf = gdf[gdf['gid_0'] == raster_obj.iso()]

            indexes = gdf[index_column_name].tolist()
            times = raster_obj.times()
            years = raster_obj.years()
            depths = list(raster_obj.depths().keys())

            for index in tqdm(indexes):
                geom = gdf[gdf[index_column_name] == index]['geometry'].iloc[0]
                xmin, ymax, xmax, ymin = geom.bounds
                ds_index = self.raster_data.sel(lon=slice(xmin, xmax), lat=slice(ymin, ymax)).copy()
                ds_index = ds_index.where(ds_index[geom_name].isin(index))
                for n, depth in enumerate(depths):
                    try:
                        # Get difference between two dates
                        diff = ds_index.loc[dict(time=times[-1], depth=depth)] - \
                               ds_index.loc[dict(time=times[0], depth=depth)]

                        # Get counts and binds of the histogram
                        if (raster_obj.dataset == 'experimental') and (raster_obj.group == 'stocks'):
                            diff = diff[raster_obj.variable()] / 10.
                        else:
                            diff = diff[raster_obj.variable()]

                        if len(depths) == len(raster_obj.n_binds()):
                            h, bins = da.histogram(diff, bins=raster_obj.n_binds()[n],
                                                   range=raster_obj.bind_ranges()[n])
                        else:
                            h, bins = da.histogram(diff, bins=raster_obj.n_binds()[0],
                                                   range=raster_obj.bind_ranges()[0])

                        sum_diff = diff.sum().values
                        count_diff = diff.count().values
                        mean_diff = sum_diff / count_diff

                        # Save values
                        df_list.append({
                            "index": index,
                            "counts": h.compute().tolist(),
                            "bins": bins.tolist(),
                            "sum_diff": sum_diff,
                            "count_diff": count_diff,
                            "mean_diff": mean_diff,
                            "depth": depth,
                            "years": [years[0], years[-1]],
                        })

                    except Exception as e:
                        pass

            df_change = pd.DataFrame(df_list)

            change_data[geom_name] = pd.merge(gdf.drop(columns='geometry'), df_change, how='left', on='index')

        return change_data

