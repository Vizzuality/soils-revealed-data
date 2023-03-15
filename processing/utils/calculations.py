import json
from typing import Dict

import numpy as np
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

    def compute(self, index_column_name: str = 'index', data_type: str = 'time_series') -> Dict[str, pd.DataFrame]:
        assert data_type in ['change', 'time_series'], "data_type must be 'change' or 'time_series'"

        df_list = []
        data = {}
        for geom_name, gdf in self.vector_data.items():
            print(f"computing {data_type} for vector data -> {geom_name}")
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
                    values = {"index": index}
                    try:
                        if (self.raster_metadata.dataset == 'experimental') and (
                                self.raster_metadata.group == 'stocks'):
                            ds_var = ds_index.where(ds_index[geom_name].isin(index)).sel(
                                depth=depth)[self.raster_metadata.variable()] / 10.
                        else:
                            ds_var = ds_index.where(ds_index[geom_name].isin(index)).sel(
                                depth=depth)[self.raster_metadata.variable()]

                        if data_type == 'change':
                            # Get difference between two dates
                            diff = ds_var.loc[dict(time=times[-1])] - ds_var.loc[dict(time=times[0])]

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
                                "variable": self.raster_metadata.variable(),
                                "group_type": self.raster_metadata.group
                            })

                        elif data_type == 'time_series':
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
                                "variable": self.raster_metadata.variable(),
                                "group_type": self.raster_metadata.group
                            })

                    except Exception as e:
                        pass
            df = pd.DataFrame(df_list)
            data[geom_name] = pd.merge(gdf.drop(columns='geometry'), df, how='left', on='index')

        return data


class PostProcessing:
    def __init__(self, raster_metadata: RasterData, vector_data: Dict[str, gpd.GeoDataFrame]):
        self.raster_metadata = raster_metadata
        self.vector_data = vector_data

    def compute_level_0_data(self, data: Dict[str, pd.DataFrame], data_type: str = 'time_series'
                             ) -> Dict[str, pd.DataFrame]:
        assert data_type in ['change', 'time_series'], "data_type must be 'change' or 'time_series'"

        depths = list(self.raster_metadata.depths().keys())

        for geom_name, df in data.items():
            geom_name_0 = geom_name.replace('_1', '_0')
            df = df[df['id'].notna()]
            df = df.astype({'id': int, 'id_0': int})

            gdf = self.vector_data.get(geom_name_0)
            if self.raster_metadata.iso() and ('political' in geom_name_0):
                gdf = gdf[gdf['gid_0'] == self.raster_metadata.iso()]

            df_final = pd.DataFrame()
            for depth in depths:
                df_tmp = df[df['depth'] == depth].copy()

                if not df_tmp.empty:
                    if data_type == 'change':
                        df_tmp = df_tmp.astype({'sum_diff': 'float64', 'count_diff': 'float64', 'mean_diff': 'float64'})
                        df_tmp['counts'] = df_tmp['counts'].apply(lambda x: np.array(x))
                        df_counts = df_tmp[['id_0', 'counts']].groupby('id_0').sum().reset_index()
                        df_counts['bins'] = [df_tmp['bins'].iloc[0]]

                        df_diff = df_tmp[['id_0', 'sum_diff', 'count_diff']].groupby('id_0').sum().reset_index()
                        df_diff['mean_diff'] = df_diff['sum_diff'] / df_diff['count_diff']

                        df_depth = pd.merge(df_counts, df_diff, on='id_0', how='left')

                    elif data_type == 'time_series':
                        df_tmp['sum_values'] = df_tmp['sum_values'].apply(lambda x: np.array(x))
                        df_tmp['count_values'] = df_tmp['count_values'].apply(lambda x: np.array(x))
                        df_depth = df_tmp[['id_0', 'sum_values', 'count_values']].groupby(
                            'id_0').sum().reset_index()
                        df_depth['mean_values'] = df_depth['sum_values'] / df_depth['count_values']

                    df_depth['depth'] = depth
                    df_depth['years'] = [df_tmp['years'].iloc[0]]
                    df_depth['variable'] = df_tmp['variable'].iloc[0]
                    df_depth['group_type'] = df_tmp['group_type'].iloc[0]

                    df_final = pd.concat([df_final, df_depth])

            df_final = pd.merge(gdf.drop(columns='geometry').astype({'id_0': int}),
                                df_final.astype({'id_0': int}), on='id_0', how='left')

        data[geom_name_0] = df_final

        return data


