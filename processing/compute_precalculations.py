from processing.utils.data import VectorData, RasterData
from processing.utils.calculations import ZonalStatistics, PostProcessing
from processing.utils.raster import ZarrData

datasets = ['experimental'] #['global', 'scenarios', 'experimental']
groups = {'global': ['historic', 'recent'],
          'scenarios': ['crop_I', 'crop_MG', 'crop_MGI', 'grass_part', 'grass_full', 'rewilding',
                        'degradation_ForestToGrass', 'degradation_ForestToCrop', 'degradation_NoDeforestation'],
          'experimental': ['stocks', 'concentration']}

vector_path = '../data/processed/vector_data/'
vector_prefixes = ['political_boundaries'] #['political_boundaries', 'hydrological_basins',
                #'biomes', 'landforms']

if __name__ == '__main__':
    # Read vector data
    print("Reading vector data!")
    vector = VectorData(vector_path, vector_prefixes)
    vector_data = vector.read_data()

    for dataset in datasets:
        print(f"{dataset.title()}")
        for group in groups[dataset]:
            print(group)
            # Read raster data
            print("Reading raster data!")
            raster_metadata = RasterData(dataset, group)
            zarr_data = ZarrData(raster_metadata)
            raster_data = zarr_data.read_as_xarray()

            # Rasterize vector data
            print("Rasterizing vector data!")
            zonal_statistics = ZonalStatistics(raster_data, vector_data, raster_metadata)
            zonal_statistics.rasterize_vector_data()

            # Compute change histogram values
            print("Compute change values!")
            print("Level 1 geometries.")
            change_data = zonal_statistics.compute_change()
            # compute level 0 geometries' values
            print("Level 0 geometries.")
            vector_0 = VectorData(vector_path, vector_prefixes, suffix='_0.geojson')
            vector_data_0 = vector_0.read_data()

            post_processing = PostProcessing(raster_metadata)
            change_data = post_processing.compute_level_0_data(change_data, vector_data_0, data_type='change')

            # Compute time series values
            print("Compute time series values!")
            print("Level 1 geometries.")
            time_series_data = zonal_statistics.compute_time_series()
            # compute level 0 geometries' values
            print("Level 0 geometries.")
            time_series_data = post_processing.compute_level_0_data(time_series_data, vector_data_0,
                                                                    data_type='time_series')





