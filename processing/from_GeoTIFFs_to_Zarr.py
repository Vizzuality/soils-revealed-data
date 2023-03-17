import click

from utils.raster import GeoTiffConverter
from utils.data import RasterData


@click.command()
@click.argument('datasets', type=lambda s: s.split(','))
def convert_to_zarr(datasets):
    """
    Convert GeoTIFFs to Zarr.
    """
    groups = {'global': ['historic', 'recent'],
              'scenarios': ['crop_I', 'crop_MG', 'crop_MGI', 'grass_part', 'grass_full', 'rewilding',
                            'degradation_ForestToGrass', 'degradation_ForestToCrop', 'degradation_NoDeforestation'],
              'experimental': ['stocks', 'concentration']}

    for dataset in datasets:
        print(dataset)
        for group in groups[dataset]:
            print(group)
            # Create an instance of a GeoTiffData Data Class with all data information
            geotiff_data = RasterData(dataset, group)
            # Save GeoTIFFs as Zarr
            geotiff_converter = GeoTiffConverter(geotiff_obj=geotiff_data)
            geotiff_converter.convert_to_zarr()


if __name__ == '__main__':
    convert_to_zarr()




