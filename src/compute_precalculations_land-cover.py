import os 
import ast

import pandas as pd
from dotenv import load_dotenv
from dask.distributed import Client

from utils.data import VectorData, LandCoverData, LandCoverRasterData
from utils.calculations import LandCoverStatistics
from utils.util import multiply_dict_values

# Load .env VARIABLEs
load_dotenv()

# VARIABLEs
FOLDER_PATH = '../data/processed/precalculations/'  
RASTER_PATH = '../data/processed/raster_data/'
VECTOR_PATH = '../data/processed/vector_data/'
VECTOR_PREFIXES = ['political_boundaries', 'hydrological_basins', 'biomes', 'landforms']
SCENARIOS = ['crop_I', 'crop_MG', 'crop_MGI', 'grass_part', 'grass_full', 'rewilding', 'degradation_ForestToGrass', 'degradation_ForestToCrop', 'degradation_NoDeforestation']
#SCENARIOS = None
READ_DATA_FROM = 's3'#local_dir'
VARIABLE = 'stocks'
GROUP_TYPE = 'future'#'recent'
PIX_HA = 6.25

def main():
    # Start distributed scheduler locally
    client = Client()  # start distributed scheduler locally. 
    client

    # Read vector data
    print("Reading vector data!")
    vector = VectorData(VECTOR_PATH, VECTOR_PREFIXES)
    vector_data_0 = vector.read_data(suffix='_0.geojson')
    vector_data_1 = vector.read_data(suffix='_1.geojson')

    # Read raster data
    print("Reading raster data!")
    lc_metadata = LandCoverData()
    raster = LandCoverRasterData(group_type=GROUP_TYPE, data_from=READ_DATA_FROM, 
                                 path=RASTER_PATH, scenarios=SCENARIOS)
    raster_data = raster.read_data() 

    # Compute Land Cover Statistics
    data = {}
    lc_statistics = LandCoverStatistics(GROUP_TYPE, raster_data, lc_metadata, SCENARIOS)
    try:
        # compute level 1 geometries' values
        print("Level 1 geometries.")
        data.update(lc_statistics.compute_level_1_data(vector_data_1))
        # compute level 0 geometries' values
        print("Level 0 geometries.")
        data.update(lc_statistics.compute_level_0_data(vector_data_0))
    except Exception as e:
        # Handle any exceptions that occur during the computation
        print(f"An error occurred during the computation: {str(e)}")
    
    # Save data
    print("Saving the data!")
    for geom_type in VECTOR_PREFIXES:
        df = pd.concat([data[key] for key in data if geom_type in key])
        df = df.sort_values(['id_0', 'id'])
        df['variable'] = VARIABLE
        df['group_type'] = GROUP_TYPE
        df.to_csv(f"{FOLDER_PATH}{geom_type}_land_cover_{GROUP_TYPE}.csv", index=False)
        
    client.close()

    # Multiply values with PIX_HA
    # Get the list of files in the folder
    files = os.listdir(FOLDER_PATH)
    # Iterate over the files and extract the prefix
    data_frames = {}
    for file in files:
        if "land_cover" in file:
            prefix = file.split("_land_cover")[0]
            if prefix not in data_frames:
                data_frames[prefix] = []
            data_frames[prefix].append(file)

    for prefix, files in data_frames.items():
        for file in files:
            file_path = os.path.join(FOLDER_PATH, file)
            df = pd.read_csv(file_path)
            multiply_values = lambda dictionary: {key: {nested_key: value * PIX_HA for nested_key, value in nested_dict.items()} for key, nested_dict in ast.literal_eval(dictionary).items()}

            if 'recent' in file:
                df = df[~pd.isna(df['land_cover_groups'])]
                if 'featurecla' in df.columns:
                    df = df[~pd.isna(df['featurecla'])]
                # Use map and lambda to update the column
                df['land_cover_groups'] = df['land_cover_groups'].map(multiply_values)
                df['land_cover_group_2018'] = df['land_cover_group_2018'].map(multiply_values)
                # Use apply to update the column
                df['land_cover'] = df['land_cover'].apply(lambda x: multiply_dict_values(ast.literal_eval(x), PIX_HA))
            elif 'future' in file:
                df = df[~pd.isna(df['land_cover_groups'])]
                if 'featurecla' in df.columns:
                    df = df[~pd.isna(df['featurecla'])]
                # Use map and lambda to update the column
                df['land_cover_groups'] = df['land_cover_groups'].map(multiply_values)
                # Use apply to update the column
                df['land_cover'] = df['land_cover'].apply(lambda x: multiply_dict_values(ast.literal_eval(x), PIX_HA))

            df.to_csv(file_path, index=False)

    
    # Concatenate datasets
    # Get the list of files in the folder
    files = os.listdir(FOLDER_PATH)
    # Iterate over the files and extract the prefix
    data_frames = {}
    for file in files:
        if "land_cover" in file:
            prefix = file.split("_land_cover")[0]
            if prefix not in data_frames:
                data_frames[prefix] = []
            data_frames[prefix].append(file)
    # Concatenate and save the data frames for each prefix
    for prefix, files in data_frames.items():
        dfs = []
        for file in files:
            file_path = os.path.join(FOLDER_PATH, file)
            df = pd.read_csv(file_path)
            dfs.append(df)
        concatenated_df = pd.concat(dfs)
        
        concatenated_df.sort_values(['id_0', 'id'])
        output_file = prefix + "_land_cover.csv"
        concatenated_df.to_csv(os.path.join(FOLDER_PATH, output_file), index=False)
    
    
if __name__ == '__main__':
    main()