from processing.utils.raster import GeoTiffConverter
from processing.utils.data import RasterData

datasets = ['global', 'scenarios', 'experimental']
groups = {'global': ['historic', 'recent'],
          'scenarios': ['crop_I', 'crop_MG', 'crop_MGI', 'grass_part', 'grass_full', 'rewilding',
                        'degradation_ForestToGrass', 'degradation_ForestToCrop', 'degradation_NoDeforestation'],
          'experimental': ['stocks', 'concentration']}

if __name__ == '__main__':
    for dataset in datasets:
        print(dataset)
        for group in groups[dataset]:
            print(group)
            # Create an instance of a GeoTiffData Data Class with all data information
            geotiff_data = RasterData(dataset, group)
            # Save GeoTIFFs as Zarr
            geotiff_converter = GeoTiffConverter(geotiff_obj=geotiff_data)
            geotiff_converter.convert_to_zarr()



