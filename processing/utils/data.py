import os
import warnings
from typing import Dict, List
from dataclasses import dataclass

import numpy as np
import pandas as pd
import geopandas as gpd
from tqdm import tqdm

warnings.filterwarnings('ignore', 'GeoSeries.notna', UserWarning)

@dataclass
class VectorData:
    path: str
    prefixes: List

    def read_data(self, suffix: str = '_1.geojson') -> Dict[str, gpd.GeoDataFrame]:
        dataframes: Dict[str, gpd.GeoDataFrame] = {}
        files = [prefix + suffix for prefix in self.prefixes]
        for file in tqdm(files):
            file_path = os.path.join(self.path, file)
            gdf = gpd.read_file(file_path)
            # Remove rows with None geometries
            gdf = gdf[gdf['geometry'].notnull()]
            # Make invalid geometries valid
            invalid_geometries = ~gdf['geometry'].is_valid
            if invalid_geometries.any():
                gdf['geometry'] = gdf['geometry'].apply(lambda x: x.buffer(0))

            dataframes[file.split('.')[0]] = gdf

        return dataframes


@dataclass
class RasterData:
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

    def iso(self):
        return {'global': None,
                'scenarios': None,
                'experimental': 'ARG'}[self.dataset]

    def geometry_path(self):
        return {'global': None,
                'scenarios': None,
                'experimental': '../data/processed/vector_data/argentina.geojson'}[self.dataset]

    def n_binds(self):
        return {'global': {'historic': [40, 40, 60], 'recent': [10]},
                'scenarios': {'crop_I': [30],
                              'crop_MG': [30],
                              'crop_MGI': [30],
                              'grass_part': [30],
                              'grass_full': [30],
                              'rewilding': [60],
                              'degradation_ForestToGrass': [51],
                              'degradation_ForestToCrop': [51],
                              'degradation_NoDeforestation': [51]},
                'experimental': {'stocks': [80], 'concentration': [20]}
                }[self.dataset][self.group]

    def bind_ranges(self):
        return {'global': {'historic': [[-20, 20], [-40, 40], [-60, 60]], 'recent': [[-50, 50]]},
                'scenarios': {'crop_I': [[0, 30]],
                              'crop_MG': [[0, 30]],
                              'crop_MGI': [[0, 30]],
                              'grass_part': [[0, 30]],
                              'grass_full': [[0, 30]],
                              'rewilding': [[-30, 30]],
                              'degradation_ForestToGrass': [[-50, 1]],
                              'degradation_ForestToCrop': [[-50, 1]],
                              'degradation_NoDeforestation': [[-50, 1]]},
                'experimental': {'stocks': [[-50, 50]], 'concentration': [[-10, 10]]}
                }[self.dataset][self.group]

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


@dataclass
class LandCoverData:
    def child_labels(self):
        return {"0": "No Data",
                "10": "Cropland rainfed",
                "11": "Cropland rainfed herbaceous cover",
                "12": "Cropland rainfed tree or shrub cover",
                "20": "Cropland irrigated or post-flooding",
                "30": "Mosaic cropland",
                "40": "Mosaic natural vegetation",
                "50": "Tree broadleaved evergreen closed to open",
                "60": "Tree broadleaved deciduous closed to open",
                "61": "Tree broadleaved deciduous closed",
                "62": "Tree broadleaved deciduous open",
                "70": "Tree needleleaved evergreen closed to open",
                "71": "Tree needleleaved evergreen closed",
                "72": "Tree needleleaved evergreen open",
                "80": "Tree needleleaved deciduous closed to open",
                "81": "Tree needleleaved deciduous closed",
                "82": "Tree needleleaved deciduous open",
                "90": "Tree mixed",
                "100": "Mosaic tree and shrub",
                "110": "Mosaic herbaceous",
                "120": "Shrubland",
                "121": "Shrubland evergreen",
                "122": "Shrubland deciduous",
                "130": "Grassland",
                "140": "Lichens and mosses",
                "150": "Sparse vegetation", 
                "151": "Sparse tree",
                "152": "Sparse shrub",
                "153": "Sparse herbaceous",
                "160": "Tree cover flooded fresh or brackish water",
                "170": "Tree cover flooded saline water",
                "180": "Shrub or herbaceous cover flooded",
                "190": "Urban areas",
                "200": "Bare areas",
                "201": "Bare areas consolidated",
                "202": "Bare areas unconsolidated",
                "210": "Water bodies",
                "220": "Snow and ice"
                }
    def parent_labels(self):
        return {"0": "No Data",
                "1": "Cropland",
                "2": "Tree-cover areas",
                "3": "Rangeland and pasture",
                "4": "Wetland",
                "5": "Mangroves",
                "6": "Urban areas",
                "7": "Bare areas",
                "8": "Water bodies",
                "9": "Snow and ice"}
    def child_parent(self):
        return {"0": "0",
                "10": "1",
                "11": "1",
                "12": "2",
                "20": "1",
                "30": "1",
                "40": "1",
                "50": "2",
                "60": "2",
                "61": "2",
                "62": "2",
                "70": "2",
                "71": "2",
                "72": "2",
                "80": "2",
                "81": "2",
                "82": "2",
                "90": "2",
                "100": "2",
                "110": "3",
                "120": "3",
                "121": "3",
                "122": "3",
                "130": "3",
                "140": "3",
                "150": "3",
                "151": "3",
                "152": "3",
                "153": "3",
                "160": "4",
                "170": "5",
                "180": "4",
                "190": "6",
                "200": "7",
                "201": "7",
                "202": "7",
                "210": "8",
                "220": "9"}
    def child_colors(self):
        return {"0": "#ffffff",
                "10": "#5B5B18",  
                "11": "#7D7616",  
                "20": "#A09113",  
                "30": "#C0AB10",  
                "40": "#DFC30C",  
                "12": "#124d00",  
                "50": "#136010",  
                "60": "#117221",  
                "61": "#0b842f",  
                "62": "#2a9339",  
                "70": "#4aa040",  
                "71": "#64ac48",  
                "72": "#7ab84f",  
                "80": "#91c357",  
                "81": "#a5ce5f",  
                "82": "#b9d867",  
                "90": "#cce36f",  
                "100": "#e0ed78",  
                "110": "#967216",  
                "120": "#a67d1a",  
                "121": "#b6881f",  
                "122": "#c69323",  
                "130": "#d69e27",  
                "140": "#e6a82b",  
                "150": "#f6b148",  
                "151": "#febc7a",  
                "152": "#ffcaaa",  
                "153": "#f8dcd3",  
                "160": "#016A6D",  
                "180": "#35ADAD",  
                "170": "#42DED5",  
                "190": "#3640B7",  
                "200": "#C54802",  
                "201": "#DF704F",  
                "202": "#FD9CA7",  
                "210": "#48A7FF",  
                "220": "#B9EEEF"}  
    def parent_colors(self):
        return {"0": "#ffffff",
                "1": "#dfc30c",
                "2": "#0B842F",
                "3": "#C69323",
                "4": "#016A6D",
                "5": "#42DED5",
                "6": "#3640B7",
                "7": "#C54802",
                "8": "#48A7FF",
                "9": "#B9EEEF"}