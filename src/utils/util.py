import os

import s3fs
import rioxarray
import xarray as xr
import pandas as pd
import geopandas as gpd
from shapely.ops import split
from dotenv import load_dotenv
from shapely.geometry import LineString, Polygon, MultiPolygon

# Load .env variables
load_dotenv()


def read_zarr_from_s3(access_key_id, secret_accsess_key, dataset, group=None):
    # AWS S3 path
    s3_path = f's3://soils-revealed/{dataset}.zarr'
    
    # Initilize the S3 file system
    s3 = s3fs.S3FileSystem(key=access_key_id, secret=secret_accsess_key)
    store = s3fs.S3Map(root=s3_path, s3=s3, check=False)
    
    # Read Zarr file
    if group:
        ds = xr.open_zarr(store=store, group=group, consolidated=True)
    else:
        ds = xr.open_zarr(store=store, consolidated=True)
       
    return ds 


def read_zarr_from_local_dir(path, group=None):
    # Read Zarr file
    if group:
        with xr.open_zarr(store=path, group=group, consolidated=True) as ds:
            return ds

    else:
        with xr.open_zarr(store=path, consolidated=True) as ds:
            return ds
        
             
def get_recent_lc_statistics(ds, raster_metadata):
    # Filter dataset 
    df = pd.concat([ds.isel(time=0).to_dataframe().reset_index().drop(
                            columns=['x', 'y', 'time']).rename(columns={'stocks': 'stocks_2000', 'land-cover': 'land_cover_2000'}),
                    ds.isel(time=1).to_dataframe().reset_index().drop(
                            columns=['x', 'y', 'time', 'mask']).rename(columns={'stocks': 'stocks_2018', 'land-cover': 'land_cover_2018'})], axis=1)
    # Filter rows where columns A and B have equal values
    df = df[df['land_cover_2000'] != df['land_cover_2018']]
    df['stocks_change'] = df['stocks_2018'] - df['stocks_2000']
    # Remove rows where mask is null
    df = df[~df['mask'].isnull()]
    # Remove rows with 0 change
    df = df[(df['stocks_change'] != 0.) & (~df['stocks_change'].isnull())]
    # Add category names
    df = df[['land_cover_2018', 'land_cover_2000', 'stocks_change']]
    df['land_cover_2000'] = df['land_cover_2000'].astype(int).astype(str)
    df['land_cover_2018'] = df['land_cover_2018'].astype(int).astype(str)
    df['land_cover_group_2000'] = df['land_cover_2000'].map(raster_metadata.child_parent())
    df['land_cover_group_2018'] = df['land_cover_2018'].map(raster_metadata.child_parent())

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
    parent = [raster_metadata.child_parent()[child]for child in children]
    child_dict = dict(zip(children, parent))

    land_cover_dict = {}
    for parent_id in list(data['land_cover_groups'].keys()):
        child_ids = [key for key, value in child_dict.items() if value == parent_id]
        land_cover_dict[parent_id] = {id: data['land_cover'][id] for id in child_ids}
        
    data['land_cover'] = land_cover_dict
    
    return data
    
    
def get_future_lc_statistics(ds, raster_metadata, scenarios):
    # Filter dataset 
    df = ds.isel(time=0).to_dataframe().reset_index().drop(columns=['x', 'y', 'time'])
    # Remove rows where mask is null
    df = df[~df['mask'].isnull()]
    # Remove rows with null or 0 change 
    #df = df[df[scenarios].notnull().all(axis=1)]
    #df = df[(df[scenarios] != 0.0).all(axis=1)]
    ## Add category names
    df = df[['land-cover']+scenarios].rename(columns={'land-cover': 'land_cover'})
    df['land_cover'] = df['land_cover'].astype(int).astype(str)
    df['land_cover_groups'] = df['land_cover'].map(raster_metadata.child_parent())

    # Create final data
    indicators = {'land_cover': ['land_cover_groups', 'land_cover'], 
                  'land_cover_groups': ['land_cover_groups']}
        
    data = {}    
    for name, indicator in indicators.items():
        data_tmp = {}
        for scenario in scenarios:
            df_sum = df.groupby(indicator)[scenario].sum().reset_index()

            records = dict(zip(df_sum[name], df_sum[scenario]))

            data_tmp[scenario] = records 
            
        # Reorder dictionary
        #sorted_keys = sorted(data_tmp.keys(), key=lambda x: sum(data_tmp[x].values()))
        #data_tmp = {key: data_tmp[key] for key in sorted_keys}
        data_tmp = sort_dict(data_tmp)
        
        data[name] = data_tmp  
        
    return data  


def shift_lon_coor(multipolygon, delta=-180):
    updated_polygons = []
    for polygon in multipolygon.geoms:
        updated_coords = []
        coords = list(polygon.exterior.coords)
        updated_coords = [(lon + delta, lat) for lon, lat in coords]
        updated_polygon = Polygon(updated_coords)
        updated_polygons.append(updated_polygon)

    return MultiPolygon(updated_polygons)


def split_geometry_with_antimeridian(gdf: gpd.GeoDataFrame):
    # Define the cutting line
    line = LineString([(0, -90), (0, 90)])
    
    # Reproject the GeoDataFrame to WGS84 datum with the prime meridian at 180 degrees longitude. 
    gdf_proj = gdf.to_crs("+proj=latlong +datum=WGS84 +lon_0=180")
    geometry = gdf_proj['geometry'].iloc[0]

    result = split(geometry, line)
    
    if type(geometry) == MultiPolygon:
        polygons_left_side = []
        polygons_right_side = []
        for geom in result.geoms:
            if geom.centroid.x <= line.coords[0][0]:
                polygons_left_side.append(geom)
            else:
                polygons_right_side.append(geom)

        geometry_left = MultiPolygon(polygons_left_side)
        geometry_right = MultiPolygon(polygons_right_side)
        
    elif type(geometry) == Polygon:
        for geom in result.geoms:
            if geom.centroid.x <= line.coords[0][0]:
                geometry_left = geom
            else:
                geometry_right = geom
    
    
    gdf_list = []
    for side, geometry in {"left": geometry_left, "right": geometry_right}.items():
        gdf_tmp = gdf_proj.copy()
        gdf_tmp['geometry'] = geometry
        gdf_tmp['side'] = side
        gdf_list.append(gdf_tmp)
     
    gdf_split = pd.concat(gdf_list)
    
    multipolygon = gdf_split['geometry'].iloc[1]
    updated_multipolygon = shift_lon_coor(multipolygon, delta=-180)
    
    gdf_split = gdf_split.to_crs("EPSG:4326")  
    gdf_split['geometry'].iloc[1] = updated_multipolygon

    return gdf_split


def remove_small_polygons(multipolygon, threshold_area):
    cleaned_polygons = []
    for polygon in multipolygon.geoms:
        if polygon.area >= threshold_area:
            cleaned_polygons.append(polygon)
    return MultiPolygon(cleaned_polygons)


def sum_two_dicts(dict1, dict2):
    result_dict = {}

    # Combine keys from both dictionaries
    all_keys = set(dict1.keys()) | set(dict2.keys())

    for key in all_keys:
        result_dict[key] = {}

        if key in dict1:
            result_dict[key].update(dict1[key])

        if key in dict2:
            for subkey in dict2[key]:
                if subkey in result_dict[key]:
                    result_dict[key][subkey] += dict2[key][subkey]
                else:
                    result_dict[key][subkey] = dict2[key][subkey]
                    
    return result_dict


def sum_dicts(list_dicts):
    for n in range(len(list_dicts)-1):
        if n == 0:
            result_dict = sum_two_dicts(list_dicts[n], list_dicts[n+1])
        else:
            result_dict = sum_two_dicts(result_dict, list_dicts[n+1])
                
    return result_dict


def sort_dict(my_dict):
    # Sort secondary keys in each sub-dictionary
    for key in my_dict:
        my_dict[key] = dict(sorted(my_dict[key].items(), key=lambda x: x[1]))

    # Sort primary keys by the sum of secondary key values
    my_dict = dict(sorted(my_dict.items(), key=lambda x: sum(x[1].values())))
    
    return my_dict