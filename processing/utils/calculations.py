from typing import Dict, List

import numpy as np
import pandas as pd
import regionmask
import xarray as xr
import geopandas as gpd
import dask.array as da
from tqdm import tqdm
from shapely.affinity import translate

from utils.data import RasterData, LandCoverData
from utils.util import sum_dicts, sort_dict, \
    remove_small_polygons, split_geometry_with_antimeridian, \
    get_recent_lc_statistics, get_future_lc_statistics


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
            if self.raster_metadata.iso():
                if 'political' in geom_name:
                    gdf = gdf[gdf['gid_0'] == self.raster_metadata.iso()]
                else:
                    region = gpd.read_file(self.raster_metadata.geometry_path())
                    geom = region.geometry.iloc[0]
                    gdf = gdf[gdf.intersects(geom)]

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
                            if count_diff != 0:
                                mean_diff = sum_diff / count_diff
                            else:
                                mean_diff = sum_diff

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
                                "group_type": self.raster_metadata.dataset
                            })

                        elif data_type == 'time_series':
                            # Get values
                            sums = ds_var.sum(['lon', 'lat']).values
                            counts = ds_var.count(['lon', 'lat']).values
                            if all(elem == 0 for elem in counts):
                                values = sums
                            else:
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
                                "group_type": self.raster_metadata.dataset
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

        for geom_name, gdf in self.vector_data.items():
            geom_name_1 = geom_name.replace('_0', '_1')
            df = data[geom_name_1]
            print(f"computing {data_type} for vector data -> {geom_name}")
            df = df[df['id'].notna()]
            df = df.astype({'id': int, 'id_0': int})

            if self.raster_metadata.iso() and ('political' in geom_name):
                gdf = gdf[gdf['gid_0'] == self.raster_metadata.iso()]
            else:
                region = gpd.read_file(self.raster_metadata.geometry_path())
                geom = region.geometry.iloc[0]
                gdf = gdf[gdf.intersects(geom)]

            df_final = pd.DataFrame()
            for depth in tqdm(depths):
                df_tmp = df[df['depth'] == depth].copy()

                if not df_tmp.empty:
                    if data_type == 'change':
                        df_tmp = df_tmp.astype({'sum_diff': 'float64', 'count_diff': 'float64', 'mean_diff': 'float64'})
                        df_tmp['counts'] = df_tmp['counts'].apply(lambda x: np.array(x))
                        df_counts = df_tmp[['id_0', 'counts']].groupby('id_0').sum().reset_index()
                        df_counts['bins'] = [df_tmp['bins'].iloc[0]] * len(df_counts)

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
                    df_depth['years'] = [df_tmp['years'].iloc[0]] * len(df_depth)
                    df_depth['variable'] = df_tmp['variable'].iloc[0]
                    df_depth['group_type'] = df_tmp['group_type'].iloc[0]

                    df_final = pd.concat([df_final, df_depth])

            df_final = pd.merge(gdf.drop(columns='geometry').astype({'id_0': int}),
                                df_final.astype({'id_0': int}), on='id_0', how='left')

            data[geom_name] = df_final

        return data


class LandCoverStatistics:
    def __init__(self, group_type: str, raster_data: xr.Dataset, 
                raster_metadata: LandCoverData, scenarios: List['str']):
        self.group_type = group_type
        self.raster_data = raster_data
        self.raster_metadata = raster_metadata
        self.scenarios = scenarios
        
    def _rasterize_vector_data(self, ds: xr.Dataset, gdf: gpd.GeoDataFrame,
                            index_column_name: str = 'index', 
                            x_coor_name: str = 'x', y_coor_name: str = 'y') -> xr.Dataset:
        """Rasterize a GeoDataFrame using xarray Dataset
        as a reference and add it as a new variable"""
        mask = regionmask.mask_geopandas(
            gdf,
            ds[x_coor_name],
            ds[y_coor_name],
            numbers=index_column_name
        )

        ds['mask'] = mask

        return ds
        

    def compute_level_1_data(self, vector_data_1: Dict[str, gpd.GeoDataFrame], 
                index_column_name: str = 'index',
                x_coor_name: str = 'x', 
                y_coor_name: str = 'y') -> Dict[str, pd.DataFrame]:
        
        self.vector_data = vector_data_1
        
        self.level_1_data = {}
        for geom_name, gdf in self.vector_data.items():
            print(f"Computing land cover statistics for vector data -> {geom_name}")
            indexes = gdf[index_column_name].tolist()

            df_list = []
            for index in tqdm(indexes):
                gdf_index  = gdf[gdf['index'] == index].copy()
                if (index == 960) and (geom_name == 'political_boundaries_1'):
                    # Remove small polygons 
                    gdf_index['geometry'] = remove_small_polygons(gdf_index['geometry'].iloc[0], 0.02)
                
                # Get bounds
                geom = gdf_index['geometry'].iloc[0]
                xmin, ymin, xmax, ymax = geom.bounds
                
                # Take care of the antimeridian
                if round(xmin) <= -175 and round(xmax) >= 175:
                    # Split the geometry with the antimeridian.
                    gdf_split = split_geometry_with_antimeridian(gdf_index)
                    
                    ds_list = []
                    for side in ['left', 'right']:
                        gdf_side = gdf_split[gdf_split['side'] == side].drop(columns="side")
                        geom = gdf_side['geometry'].iloc[0]
                        xmin, ymin, xmax, ymax = geom.bounds
                        ds_side = self.raster_data.sel(x=slice(xmin, xmax), y=slice(ymax, ymin)) 
                        # Rasterize vector data
                        ds_list.append(self._rasterize_vector_data(ds_side, 
                                                                    gdf_side.drop(columns="index").reset_index(),
                                                                    'index', 'x', 'y'))

                    # Combine the two datasets using combine_by_coords
                    ds_index = xr.combine_by_coords(ds_list)

                else:
                    ds_index = self.raster_data.sel(x=slice(xmin, xmax), y=slice(ymax, ymin)).copy()
                    # Rasterize vector data
                    ds_index = self._rasterize_vector_data(ds_index, 
                                                                    gdf_index.drop(columns="index").reset_index(), 
                                                                    'index', 'x', 'y')
                # Filter by geometry
                ds_index = ds_index.where(ds_index['mask'].isin(index))                
                
                # Get statistics
                try:
                    if self.group_type == 'recent':
                        data = get_recent_lc_statistics(ds_index, self.raster_metadata) 
                    elif self.group_type == 'future':
                        data = get_future_lc_statistics(ds_index, self.raster_metadata, self.scenarios) 
                        
                    # Save values
                    data["index"] = index
                    df_list.append(data)
                except Exception as e:
                        pass
                    
            df = pd.DataFrame(df_list)
            self.level_1_data[geom_name] = pd.merge(gdf.drop(columns='geometry'), df, how='left', on='index').drop(columns='index')    
                
        return self.level_1_data 
    
    
    def compute_level_0_data(self, vector_data_0: Dict[str, gpd.GeoDataFrame]):
        level_0_data = {}
        for geom_name, df in self.level_1_data.items():
            geom_name_0 = geom_name.replace('_1', '_0')
            print(f"Computing land cover statistics for vector data -> {geom_name_0}")
            
            gdf = vector_data_0[geom_name_0]
            
            df = df[df['id'].notna()]
            df = df.astype({'id': int, 'id_0': int})
            ids = list(df['id_0'].unique())
            
            df_final = pd.DataFrame()
            df_list = []
            for id in tqdm(ids):
                data_tmp = {'id_0': [id]}
                df_tmp = df[df['id_0'] == id]
                
                if self.group_type == 'recent':
                    # Land cover groups
                    for column in ['land_cover_groups', 'land_cover_group_2018']:
                        # sum dictionaries
                        list_dicts = list(df_tmp[column])
                        result_dict = sum_dicts(list_dicts)           
                        # sort dictionary
                        data_tmp[column] = [sort_dict(result_dict)]
                        
                    # Land cover 
                    list_dicts = list(list(df_tmp['land_cover']))
                    land_cover_dict = {}
                    for key in list(data_tmp['land_cover_groups'][0].keys()):
                        filtered_list = [d[key] for d in list_dicts if key in d]
                        if len(filtered_list) > 1:
                            result_dict = sum_dicts(filtered_list)
                            land_cover_dict[key] = sort_dict(result_dict)
                        else:
                            land_cover_dict[key] = filtered_list[0]
                            
                    data_tmp['land_cover'] = [land_cover_dict]
                elif self.group_type == 'future':
                    # Land cover 
                    for column in ['land_cover', 'land_cover_groups']:
                        # sum dictionaries
                        list_dicts = list(df_tmp[column])
                        result_dict = sum_dicts(list_dicts)           
                        # sort dictionary
                        data_tmp[column] = [sort_dict(result_dict)]
                    
                df_list.append(pd.DataFrame(data_tmp))
                
            df_final = pd.concat(df_list)
            
            df_final = pd.merge(gdf.drop(columns='geometry').astype({'id_0': int}),
                                df_final.astype({'id_0': int}), on='id_0', how='left')

            level_0_data[geom_name_0] = df_final.drop(columns='index')
            
            return level_0_data
                    
                
                
                
            