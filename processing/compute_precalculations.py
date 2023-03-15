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
    vector_data_0 = vector.read_data(suffix='_0.geojson')
    vector_data_1 = vector.read_data(suffix='_1.geojson')

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
            zonal_statistics = ZonalStatistics(raster_data, vector_data_1, raster_metadata)
            zonal_statistics.rasterize_vector_data()

            # Compute Zonal Statistics
            data = {}
            post_processing = PostProcessing(raster_metadata, vector_data_0)
            for data_type in ['change', 'time_series']:
                print(f"Compute {data_type} values!")
                # compute level 1 geometries' values
                print("Level 1 geometries.")
                data[data_type] = zonal_statistics.compute(data_type=data_type)

                # compute level 0 geometries' values
                print("Level 0 geometries.")
                data[data_type] = post_processing.compute_level_0_data(data[data_type],
                                                                       data_type=data_type)









