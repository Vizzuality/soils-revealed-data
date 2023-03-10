from typing import Dict

import pandas as pd
import regionmask
import xarray as xr
import geopandas as gpd
import dask.array as da
from tqdm import tqdm

from processing.utils.data import RasterData


class ZonalStatistics:
    def __init__(self, raster_data: xr.Dataset, vector_data: Dict[str, gpd.GeoDataFrame], raster_metadata: RasterData):
        self.raster_data = raster_data
        self.vector_data = vector_data
        self.raster_metadata = raster_metadata

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

    def compute_change(self, index_column_name: str = 'index') -> Dict[str, pd.DataFrame]:
        df_list = []
        change_data = {}
        for geom_name, gdf in self.vector_data.items():
            print(f"computing change for vector data -> {geom_name}")
            if self.raster_metadata.iso() and ('political' in geom_name):
                gdf = gdf[gdf['gid_0'] == self.raster_metadata.iso()]

            indexes = gdf[index_column_name].tolist()
            times = self.raster_metadata.times()
            years = self.raster_metadata.years()
            depths = list(self.raster_metadata.depths().keys())

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
                        if (self.raster_metadata.dataset == 'experimental') and (self.raster_metadata.group == 'stocks'):
                            diff = diff[self.raster_metadata.variable()] / 10.
                        else:
                            diff = diff[self.raster_metadata.variable()]

                        if len(depths) == len(self.raster_metadata.n_binds()):
                            h, bins = da.histogram(diff, bins=self.raster_metadata.n_binds()[n],
                                                   range=self.raster_metadata.bind_ranges()[n])
                        else:
                            h, bins = da.histogram(diff, bins=self.raster_metadata.n_binds()[0],
                                                   range=self.raster_metadata.bind_ranges()[0])

                        # Get values
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

    def compute_time_series(self, index_column_name: str = 'index') -> Dict[str, pd.DataFrame]:
        df_list = []
        time_series = {}
        for geom_name, gdf in self.vector_data.items():
            print(f"computing time series for vector data -> {geom_name}")
            if self.raster_metadata.iso() and ('political' in geom_name):
                gdf = gdf[gdf['gid_0'] == self.raster_metadata.iso()]

            indexes = gdf[index_column_name].tolist()
            years = self.raster_metadata.years()
            depths = list(self.raster_metadata.depths().keys())

            for index in tqdm(indexes):
                geom = gdf[gdf[index_column_name] == index]['geometry'].iloc[0]
                xmin, ymax, xmax, ymin = geom.bounds
                ds_index = self.raster_data.sel(lon=slice(xmin, xmax), lat=slice(ymin, ymax)).copy()
                ds_index = ds_index.where(ds_index[geom_name].isin(index))

                for n, depth in enumerate(depths):
                    try:
                        if (self.raster_metadata.dataset == 'experimental') and (self.raster_metadata.group == 'stocks'):
                            ds_var = ds_index.where(ds_index[geom_name].isin(index)).sel(
                                depth=depth)[self.raster_metadata.variable()] / 10.
                        else:
                            ds_var = ds_index.where(ds_index[geom_name].isin(index)).sel(
                                depth=depth)[self.raster_metadata.variable()]

                        # Get values
                        sums = ds_var.sum(['lon', 'lat']).values
                        counts = ds_var.count(['lon', 'lat']).values
                        values = sums / counts

                        # Save values
                        df_list.append({
                            "index": index,
                            "sum_values": sums.tolist(),
                            "count_values": counts.tolist(),
                            "mean_values": values.tolist(),
                            "depth": depth,
                            "years": [years[0], years[-1]],
                        })

                    except Exception as e:
                        pass

            df_time_series = pd.DataFrame(df_list)

            time_series[geom_name] = pd.merge(gdf.drop(columns='geometry'), df_time_series, how='left', on='index')

        return time_series
