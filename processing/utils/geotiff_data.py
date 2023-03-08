from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass
class GeoTiffData:
    dataset: str
    group: str

    def variable(self):
        return {'historic': 'stocks', 'recent': 'stocks',
                'crop_I': 'stocks', 'crop_MG': 'stocks', 'crop_MGI': 'stocks', 'grass_part': 'stocks',
                'grass_full': 'stocks', 'rewilding': 'stocks', 'degradation_ForestToGrass': 'stocks',
                'degradation_ForestToCrop': 'stocks', 'degradation_NoDeforestation': 'stocks',
                'stocks': 'stocks', 'concentration': 'concentration'}[self.group]

    def local_path(self):
        return {'global': '../data/processed/raster_data/global-dataset.zarr',
                'scenarios': '../data/processed/raster_data/scenarios-dataset.zarr',
                'experimental': '../data/processed/raster_data/experimental-dataset.zarr'}[self.dataset]

    def s3_path(self):
        return {'global': 's3://soils-revealed/global-dataset.zarr',
                'scenarios': 's3://soils-revealed/scenarios-dataset.zarr',
                'experimental': 's3://soils-revealed/experimental-dataset.zarr'}[self.dataset]

    def gcp_path(self):
        return {'global': {'historic': 'SOC_maps/Historic/',
                           'recent': 'SOC_maps/Recent_Nov/'},
                'scenarios': {'crop_I': 'SOC_maps/Future/',
                              'crop_MG': 'SOC_maps/Future/',
                              'crop_MGI': 'SOC_maps/Future/',
                              'grass_part': 'SOC_maps/Future/',
                              'grass_full': 'SOC_maps/Future/',
                              'rewilding': 'SOC_maps/Future/',
                              'degradation_ForestToGrass': 'SOC_maps/Future/',
                              'degradation_ForestToCrop': 'SOC_maps/Future/',
                              'degradation_NoDeforestation': 'SOC_maps/Future/'},
                'experimental': {'stocks': 'SOC_maps/SOC_stock_EJSS/',
                                 'concentration': 'SOC_maps/SOC_concentration2020/'}
                }[self.dataset][self.group]

    def file_prefix(self):
        return {'global': {'historic': 'SOCS_', 'recent': 'SOC_'},
                'scenarios': {'crop_I': 'scenario_',
                              'crop_MG': 'scenario_',
                              'crop_MGI': 'scenario_',
                              'grass_part': 'scenario_',
                              'grass_full': 'scenario_',
                              'rewilding': 'scenario_',
                              'degradation_ForestToGrass': 'scenario_',
                              'degradation_ForestToCrop': 'scenario_',
                              'degradation_NoDeforestation': 'scenario_'},
                'experimental': {'stocks': 'cstock030_', 'concentration': 'SOC_'}
                }[self.dataset][self.group]

    def file_infix(self):
        return {'global': {'historic': 'cm_year_', 'recent': ''},
                'scenarios': {'crop_I': '_SOC_Y',
                              'crop_MG': '_SOC_Y',
                              'crop_MGI': '_SOC_Y',
                              'grass_part': '_SOC_Y',
                              'grass_full': '_SOC_Y',
                              'rewilding': '_SOC_Y',
                              'degradation_ForestToGrass': '_SOC_Y',
                              'degradation_ForestToCrop': '_SOC_Y',
                              'degradation_NoDeforestation': '_SOC_Y'},
                'experimental': {'stocks': '', 'concentration': '_q0.5_D'}
                }[self.dataset][self.group]

    def file_suffix(self):
        return {'global': {'historic': '_10km.tif', 'recent': '_4326.tif'},
                'scenarios': {'crop_I': '_nov.tif',
                              'crop_MG': '_nov.tif',
                              'crop_MGI': '_nov.tif',
                              'grass_part': '_nov.tif',
                              'grass_full': '_nov.tif',
                              'rewilding': '_nov.tif',
                              'degradation_ForestToGrass': '_nov.tif',
                              'degradation_ForestToCrop': '_nov.tif',
                              'degradation_NoDeforestation': '_nov.tif'},
                'experimental': {'stocks': '_Q0.5.tif', 'concentration': '.tif'}
                }[self.dataset][self.group]

    def years(self):
        return {'global': {'historic': ['NoLU', '2010AD'],
                           'recent': np.arange(2000, 2019, 1).astype(str)},
                'scenarios': {'crop_I': np.arange(2018, 2039, 5).astype(str),
                              'crop_MG': np.arange(2018, 2039, 5).astype(str),
                              'crop_MGI': np.arange(2018, 2039, 5).astype(str),
                              'grass_part': np.arange(2018, 2039, 5).astype(str),
                              'grass_full': np.arange(2018, 2039, 5).astype(str),
                              'rewilding': np.arange(2018, 2039, 5).astype(str),
                              'degradation_ForestToGrass': np.arange(2018, 2039, 5).astype(str),
                              'degradation_ForestToCrop': np.arange(2018, 2039, 5).astype(str),
                              'degradation_NoDeforestation': np.arange(2018, 2039, 5).astype(str)},
                'experimental': {'stocks': np.arange(1982, 2018, 1).astype(str),
                                 'concentration': np.arange(1982, 2018, 1).astype(str)}
                }[self.dataset][self.group]

    def times(self):
        return {'global': {'historic': ['NoLU', '2010AD'],
                           'recent': pd.date_range("2000", "2019", freq='A-DEC', name="time")},
                'scenarios': {'crop_I': pd.date_range("2018", "2039", freq='A-DEC', name="time")[0::5],
                              'crop_MG': pd.date_range("2018", "2039", freq='A-DEC', name="time")[0::5],
                              'crop_MGI': pd.date_range("2018", "2039", freq='A-DEC', name="time")[0::5],
                              'grass_part': pd.date_range("2018", "2039", freq='A-DEC', name="time")[0::5],
                              'grass_full': pd.date_range("2018", "2039", freq='A-DEC', name="time")[0::5],
                              'rewilding': pd.date_range("2018", "2039", freq='A-DEC', name="time")[0::5],
                              'degradation_ForestToGrass': pd.date_range("2018", "2039", freq='A-DEC', name="time")[0::5],
                              'degradation_ForestToCrop': pd.date_range("2018", "2039", freq='A-DEC', name="time")[0::5],
                              'degradation_NoDeforestation': pd.date_range("2018", "2039", freq='A-DEC', name="time")[0::5]},
                'experimental': {'stocks': pd.date_range('1982', '2018', freq='A-DEC', name="time"),
                                 'concentration': pd.date_range('1982', '2018', freq='A-DEC', name="time")}
                }[self.dataset][self.group]

    def depths(self):
        return {'global': {'historic': {'0-30': '0_30', '0-100': '0_100', '0-200': '0_200'},
                           'recent': {'0-30': ''}},
                'scenarios': {'crop_I': {'0-30': ''},
                              'crop_MG': {'0-30': ''},
                              'crop_MGI': {'0-30': ''},
                              'grass_part': {'0-30': ''},
                              'grass_full': {'0-30': ''},
                              'rewilding': {'0-30': ''},
                              'degradation_ForestToGrass': {'0-30': ''},
                              'degradation_ForestToCrop': {'0-30': ''},
                              'degradation_NoDeforestation': {'0-30': ''}},
                'experimental': {'stocks': {'0-30': '_030cm'},
                                 'concentration': {'0-5': '2.5', '5-15': '10', '15-30': '22.5',
                                                   '30-60': '45', '60-100': '80', '100-200': '150'}}
                }[self.dataset][self.group]

    def no_data(self):
        return {'global': {'historic': -32767.0, 'recent': None},
                'scenarios': {'crop_I': None,
                              'crop_MG': None,
                              'crop_MGI': None,
                              'grass_part': None,
                              'grass_full': None,
                              'rewilding': None,
                              'degradation_ForestToGrass': None,
                              'degradation_ForestToCrop': None,
                              'degradation_NoDeforestation': None},
                'experimental': {'stocks': -32768., 'concentration': 0}
                }[self.dataset][self.group]

    def delta_years(self, year_name: str):
        return {'global': {},
                'scenarios': {'2018': '00', '2023': '05', '2028': '10', '2033': '15', '2038': '20'},
                'experimental': {}}[self.dataset][year_name]

    def get_file_name(self, year_name, depth_name):
        if self.dataset == 'scenarios':
            file_name = self.file_prefix() + self.group + self.file_infix() + self.delta_years(year_name) + self.file_suffix()
        elif self.group == 'historic':
            file_name = self.file_prefix() + depth_name + self.file_infix() + year_name + self.file_suffix()
        elif self.group == 'recent':
            file_name = self.file_prefix() + year_name + self.file_suffix()
        else:
            file_name = self.file_prefix() + year_name + self.file_infix() + depth_name + self.file_suffix()
        return file_name
