import click
import pandas as pd

from utils.data import VectorData, RasterData
from utils.calculations import ZonalStatistics, PostProcessing
from utils.raster import ZarrData


@click.command()
@click.argument('datasets', type=lambda s: s.split(','))
@click.argument('vector_prefixes', type=lambda s: s.split(','))
@click.option('--vector_path', '-vp', default='../data/processed/vector_data/',
              help='Path to vector data.')
def main(datasets, vector_prefixes, vector_path):
    """
    Compute precalculations
    """
    print('Datasets:', datasets)
    print('Vector prefixes:', vector_prefixes)

    groups = {'global': ['historic', 'recent'],
              'scenarios': ['crop_I', 'crop_MG', 'crop_MGI', 'grass_part', 'grass_full', 'rewilding',
                            'degradation_ForestToGrass', 'degradation_ForestToCrop', 'degradation_NoDeforestation'],
              'experimental': ['stocks', 'concentration']}

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
            # Save data
            print("Saving the data!")
            for data_type, values in data.items():
                data_type_data = {}
                for key, value in data[data_type].items():
                    prefix = key.rsplit('_', 1)[0]
                    if prefix not in data_type_data:
                        data_type_data[prefix] = value
                    data_type_data[prefix] = pd.concat([data_type_data[prefix], value])

                for geom_type, df in data_type_data.items():
                    df.to_csv(f"../data/processed/precalculations/{geom_type}_{data_type}_{dataset}_{group}.csv")


if __name__ == '__main__':
    main()








