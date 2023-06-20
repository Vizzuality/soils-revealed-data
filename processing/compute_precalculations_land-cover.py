import os 

import pandas as pd
from dotenv import load_dotenv
from dask.distributed import Client

from utils.data import VectorData, LandCoverData
from utils.raster import read_zarr_from_s3, read_zarr_from_local_dir
from utils.calculations import LandCoverStatistics

# Load .env VARIABLEs
load_dotenv()

# VARIABLEs
RASTER_PATH = '../data/processed/raster_data/'
VECTOR_PATH = '../data/processed/vector_data/'
VECTOR_PREFIXES = ['political_boundaries']
READ_DATA_FROM = 'local_dir'
VARIABLE = 'stocks'
GROUP_TYPE = 'recent'


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
    
    # read Recent dataset
    file = 'global-dataset.zarr'
    dataset = 'global-dataset'
    group = 'recent'
    if READ_DATA_FROM == 's3':
        ds = read_zarr_from_s3(access_key_id = os.getenv("S3_ACCESS_KEY_ID"), 
                            secret_accsess_key = os.getenv("S3_SECRET_ACCESS_KEY"),
                            dataset = dataset, group = group) 
    elif READ_DATA_FROM == 'local_dir':
        ds = read_zarr_from_local_dir(path=os.path.join(RASTER_PATH, file), group = group)
        
    ds = ds.drop_dims('depth').sel(time=['2000-12-31T00:00:00.000000000', '2018-12-31T00:00:00.000000000'])

    # read land cover dataset
    file = 'land-cover.zarr'
    dataset = 'land-cover'
    if READ_DATA_FROM == 's3':
        ds_lc = read_zarr_from_s3(access_key_id = os.getenv("S3_ACCESS_KEY_ID"), 
                                secret_accsess_key = os.getenv("S3_SECRET_ACCESS_KEY"),
                                dataset = dataset) 
    elif READ_DATA_FROM == 'local_dir':
        ds_lc = read_zarr_from_local_dir(path=os.path.join(RASTER_PATH, file))
        
    ds['land-cover'] = ds_lc['land-cover']

    raster_data = ds

    # Compute Land Cover Statistics
    data = {}
    lc_statistics = LandCoverStatistics(raster_data, lc_metadata)
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
        df.to_csv(f"../data/processed/precalculations/{geom_type}_land_cover_{GROUP_TYPE}.csv", index=False)
        
    client.close()
    
    
if __name__ == '__main__':
    main()