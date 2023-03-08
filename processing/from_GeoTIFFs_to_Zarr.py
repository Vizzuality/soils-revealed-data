from processing.utils.geotiff import GeoTiffData, GeoTiffConverter


if __name__ == '__main__':
    datasets = ['global', 'experimental']
    groups = {'global': ['historic', 'recent'], 'experimental': ['stocks', 'concentration']}

    for dataset in datasets:
        print(dataset)
        for group in groups[dataset]:
            print(group)
            # Create an instance of a GeoTiffData Data Class with all data information
            geotiff_data = GeoTiffData(dataset, group)
            # Save GeoTIFFs as Zarr
            geotiff_converter = GeoTiffConverter(geotiff_obj=geotiff_data)
            geotiff_converter.convert_to_zarr()



