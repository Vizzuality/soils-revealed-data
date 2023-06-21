import pandas as pd
import geopandas as gpd
from shapely.ops import split
from shapely.geometry import LineString, Polygon, MultiPolygon


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
    gdf_split = gdf_split.to_crs("EPSG:4326")   

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