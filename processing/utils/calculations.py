from typing import Dict

import numpy as np
import pandas as pd
import regionmask
import xarray as xr
import geopandas as gpd
import dask.array as da
from tqdm import tqdm

from utils.data import RasterData, LandCoverData
from utils.util import sum_dicts, sort_dict


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
    def __init__(self, raster_data: xr.Dataset, raster_metadata: LandCoverData):
        self.raster_data = raster_data
        self.raster_metadata = raster_metadata
        
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
    
    def _get_statistics(self, ds: xr.Dataset):
        # Filter dataset 
        df = pd.concat([ds.isel(time=0).to_dataframe().reset_index().drop(
                                columns=['x', 'y', 'time']).rename(columns={'stocks': 'stocks_2000', 'land-cover': 'land_cover_2000'}),
                        ds.isel(time=1).to_dataframe().reset_index().drop(
                                columns=['x', 'y', 'time']).rename(columns={'stocks': 'stocks_2018', 'land-cover': 'land_cover_2018'})], axis=1)
        # Filter rows where columns A and B have equal values
        df = df[df['land_cover_2000'] != df['land_cover_2018']]
        df['stocks_change'] = df['stocks_2018'] - df['stocks_2000']
        # Remove rows with 0 change
        df = df[(df['stocks_change'] != 0.) & (~df['stocks_change'].isnull())]
        # Add category names
        df = df[['land_cover_2018', 'land_cover_2000', 'stocks_change']]
        df['land_cover_2000'] = df['land_cover_2000'].astype(int).astype(str)
        df['land_cover_2018'] = df['land_cover_2018'].astype(int).astype(str)
        df['land_cover_group_2000'] = df['land_cover_2000'].map(self.raster_metadata.child_parent())
        df['land_cover_group_2018'] = df['land_cover_2018'].map(self.raster_metadata.child_parent())

        # Create final data
        indicators = {'land_cover_groups': ['land_cover_group_2000', 'land_cover_group_2018'], 
                    'land_cover': ['land_cover_2000', 'land_cover_2018'],
                    'land_cover_group_2018': ['land_cover_2000', 'land_cover_group_2018']}

        data = {}
        for name, indicator in indicators.items():
            # Grouping the DataFrame by land cover 2000 and 2018, and applying the aggregation function to 'stocks_change'
            grouped_df = df.groupby([indicator[0], indicator[1]])['stocks_change'].sum().reset_index()
            grouped_2018_df = grouped_df.groupby([indicator[1]])['stocks_change'].sum().reset_index()
                
            data_tmp = {}
            for category in grouped_2018_df.sort_values('stocks_change')[indicator[1]]:
                records = grouped_df[grouped_df[indicator[1]] == category].sort_values('stocks_change')
                records = dict(zip(records[indicator[0]], records['stocks_change']))

                data_tmp[category] = records
                
            data[name] = data_tmp
            
            
        # Reorganize land cover data
        children = list(data['land_cover'].keys())
        parent = [self.raster_metadata.child_parent()[child]for child in children]
        child_dict = dict(zip(children, parent))

        land_cover_dict = {}
        for parent_id in list(data['land_cover_groups'].keys()):
            child_ids = [key for key, value in child_dict.items() if value == parent_id]
            land_cover_dict[parent_id] = {id: data['land_cover'][id] for id in child_ids}
            
        data['land_cover'] = land_cover_dict
        
        return data
        

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
                geom = gdf_index['geometry'].iloc[0]
                xmin, ymin, xmax, ymax = geom.bounds

                ds_index = self.raster_data.sel(x=slice(xmin, xmax), y=slice(ymax, ymin)).copy()
                
                # Rasterize vector data
                ds_index = self._rasterize_vector_data(ds_index, gdf_index, index_column_name, x_coor_name, y_coor_name)
                ds_index = ds_index.where(ds_index['mask'].isin(index))
                
                # Get statistics
                try:
                    data = self._get_statistics(ds_index)
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
                
                df_list.append(pd.DataFrame(data_tmp))
                
            df_final = pd.concat(df_list)
            
            df_final = pd.merge(gdf.drop(columns='geometry').astype({'id_0': int}),
                                df_final.astype({'id_0': int}), on='id_0', how='left')

            level_0_data[geom_name_0] = df_final.drop(columns='index')
            
            return level_0_data
                    
                
                
                
            