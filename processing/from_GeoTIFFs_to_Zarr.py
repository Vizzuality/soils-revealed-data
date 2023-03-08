from processing.utils.geotiff_classes import GeoTiffConverter
from processing.utils.geotiff_data import GeoTiffData

if __name__ == '__main__':
    datasets = ['global', 'scenarios', 'experimental']
    groups = {'global': ['historic', 'recent'],
              'scenarios': ['crop_I', 'crop_MG', 'crop_MGI', 'grass_part', 'grass_full', 'rewilding',
                            'degradation_ForestToGrass', 'degradation_ForestToCrop', 'degradation_NoDeforestation'],
              'experimental': ['stocks', 'concentration']}

    for dataset in datasets:
        print(dataset)
        for group in groups[dataset]:
            print(group)
            # Create an instance of a GeoTiffData Data Class with all data information
            geotiff_data = GeoTiffData(dataset, group)
            # Save GeoTIFFs as Zarr
            geotiff_converter = GeoTiffConverter(geotiff_obj=geotiff_data)
            geotiff_converter.convert_to_zarr()



