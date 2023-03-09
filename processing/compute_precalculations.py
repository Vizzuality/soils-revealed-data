from processing.utils.data import VectorData, RasterData
from processing.utils.raster import ZarrData

datasets = ['global', 'scenarios', 'experimental']
groups = {'global': ['historic', 'recent'],
          'scenarios': ['crop_I', 'crop_MG', 'crop_MGI', 'grass_part', 'grass_full', 'rewilding',
                        'degradation_ForestToGrass', 'degradation_ForestToCrop', 'degradation_NoDeforestation'],
          'experimental': ['stocks', 'concentration']}

vector_path = '../data/processed/vector_data/'
vector_files = ['political_boundaries_1.geojson', 'hydrological_basins_1.geojson',
                'biomes_1.geojson', 'landforms_1.geojson']

if __name__ == '__main__':
    # Read vector data
    print("Reading vector data!")
    vector = VectorData(vector_path, vector_files)
    vector_data = vector.read_data()

    for dataset in datasets:
        print(dataset)
        for group in groups[dataset]:
            print(group)
            # Create an instance of a GeoTiffData Data Class with all data information
            raster = RasterData(dataset, group)
            # Read raster data
            print("Reading raster data!")
            zarr_data = ZarrData(raster)
            raster_data = zarr_data.read_as_xarray()
            print(raster_data)



