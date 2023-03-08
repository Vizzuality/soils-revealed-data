import os
from typing import Union
from pathlib import Path
from dataclasses import dataclass

import zarr
import s3fs
import rioxarray
import numpy as np
import xarray as xr
import pandas as pd
from google.cloud import storage

# Load .env variables
from dotenv import load_dotenv
load_dotenv()


@dataclass
class GeoTiffData:
    dataset: str
    group: str

    def variable(self):
        return {'historic': 'stocks', 'recent': 'stocks',
                'stocks': 'stocks', 'concentration': 'concentration'}[self.group]

    def local_path(self):
        return {'global': '../data/processed/raster_data/global-dataset.zarr',
                'experimental': '../data/processed/raster_data/experimental-dataset.zarr'}[self.dataset]

    def s3_path(self):
        return {'global': 's3://soils-revealed/global-dataset.zarr',
                'experimental': 's3://soils-revealed/experimental-dataset.zarr'}[self.dataset]

    def gcp_path(self):
        return {'global': {'historic': 'SOC_maps/Historic/', 'recent': 'SOC_maps/Recent_Nov/'},
                'experimental': {'stocks': 'SOC_maps/SOC_stock_EJSS/',
                                 'concentration': 'SOC_maps/SOC_concentration2020/'}
                }[self.dataset][self.group]

    def file_prefix(self):
        return {'global': {'historic': 'SOCS_', 'recent': 'SOC_'},
                'experimental': {'stocks': 'cstock030_', 'concentration': 'SOC_'}
                }[self.dataset][self.group]

    def file_infix(self):
        return {'global': {'historic': 'cm_year_', 'recent': '_4326.tif'},
                'experimental': {'stocks': '', 'concentration': '_q0.5_D'}
                }[self.dataset][self.group]

    def file_suffix(self):
        return {'global': {'historic': '_10km.tif', 'recent': ''},
                'experimental': {'stocks': '_Q0.5.tif', 'concentration': '.tif'}
                }[self.dataset][self.group]

    def years(self):
        return {'global': {'historic': ['NoLU', '2010AD'],
                           'recent': np.arange(2000, 2019, 1).astype(str)},
                'experimental': {'stocks': np.arange(1982, 2018, 1).astype(str),
                                 'concentration': np.arange(1982, 2018, 1).astype(str)}
                }[self.dataset][self.group]

    def times(self):
        return {'global': {'historic': ['NoLU', '2010AD'],
                           'recent': pd.date_range("2000", "2019", freq='A-DEC', name="time")},
                'experimental': {'stocks': pd.date_range('1982', '2018', freq='A-DEC', name="time"),
                                 'concentration': pd.date_range('1982', '2018', freq='A-DEC', name="time")}
                }[self.dataset][self.group]

    def depths(self):
        return {'global': {'historic': {'0-30': '0_30', '0-100': '0_100', '0-200': '0_200'},
                           'recent': {'0-30': ''}},
                'experimental': {'stocks': {'0-30': '_030cm'},
                                 'concentration': {'0-5': '2.5', '5-15': '10', '15-30': '22.5',
                                                   '30-60': '45', '60-100': '80', '100-200': '150'}}
                }[self.dataset][self.group]

    def no_data(self):
        return {'global': {'historic': -32767.0, 'recent': None},
                'experimental': {'stocks': -32768., 'concentration': 0}
                }[self.dataset][self.group]

    def get_file_name(self, year_name, depth_name):
        if self.group == 'historic':
            file_name = self.file_prefix() + depth_name + self.file_infix() + year_name + self.file_suffix()
        elif self.group == 'recent':
            file_name = self.file_prefix() + year_name + self.file_suffix()
        else:
            file_name = self.file_prefix() + year_name + self.file_infix() + depth_name + self.file_suffix()
        return file_name


class GCSGeoTiff:
    # Set environment variable for service account key file path
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv('PRIVATEKEY_PATH')
    bucket_name = os.getenv('BUCKET')

    def __init__(self, blob_name: Union[str, Path]):
        self.blob_name = blob_name

    def download(self, file_name):
        storage_client = storage.Client.from_service_account_json(os.getenv('PRIVATEKEY_PATH'))
        bucket = storage_client.bucket(self.bucket_name)
        blob = bucket.blob(self.blob_name)
        blob.download_to_filename(file_name)

        print(
            "File {} downloaded to {}.".format(
                self.blob_name, file_name
            )
        )

    def read_as_xarray(self):
        """Open the GeoTIFF file as an xarray dataset"""
        with rioxarray.open_rasterio('gs://' + self.bucket_name + '/' + self.blob_name) as dataset:
            return dataset


class GeoTiffConverter:
    s3_access_key_id = os.getenv("S3_ACCESS_KEY_ID")
    s3_secret_access_key = os.getenv("S3_SECRET_ACCESS_KEY")

    def __init__(self, geotiff_obj: GeoTiffData, save_in_s3: bool = False):
        self.geotiff_obj = geotiff_obj
        self.save_in_s3 = save_in_s3
        if save_in_s3:
            self.s3 = s3fs.S3FileSystem(key=self.s3_access_key_id, secret=self.s3_secret_access_key)

    def convert_to_zarr(self):
        for i, year in enumerate(self.geotiff_obj.years()):
            print(f'Year: {year}')
            xds_depth_list = []
            for j, (depth, depth_name) in enumerate(self.geotiff_obj.depths().items()):
                print(f'Depth: {depth}')

                # Read GeoTIFF
                geotiff_file = GCSGeoTiff(
                    os.path.join(self.geotiff_obj.gcp_path(), self.geotiff_obj.get_file_name(year, depth_name)))

                # Drop band coordinate and attributes
                xda = geotiff_file.read_as_xarray().squeeze().drop_vars("band")
                xda.attrs = {}

                # Replace nodata values with np.nan
                if self.geotiff_obj.no_data():
                    xda = xda.where(xda != self.geotiff_obj.no_data())

                # Add time coordinates
                xda = xda.assign_coords({"time": self.geotiff_obj.times()[i]}).expand_dims(['time'])

                # Convert to Dataset and add depth coordinates
                xds = xr.Dataset({self.geotiff_obj.variable(): xda}, attrs=xda.attrs)
                xds = xds.assign_coords({"depth": np.array([depth])})

                xds_depth_list.append(xds)

            # Concatenate by depth
            xds = xr.concat(xds_depth_list, dim='depth')

            # Save xr.Dataset as Zarr
            store = s3fs.S3Map(root=self.geotiff_obj.s3_path(), s3=self.s3,
                               check=False) if self.save_in_s3 else self.geotiff_obj.local_path()
            mode = "w" if i == 0 else "a"
            append_dim = None if i == 0 else "time"

            xds.to_zarr(store=store, group=self.geotiff_obj.group, mode=mode, append_dim=append_dim, consolidated=True)

            # consolidate metadata at root
            zarr.consolidate_metadata(store)
            c = self.s3.exists(f"{self.geotiff_obj.s3_path()}/.zmetadata") if self.save_in_s3 else os.path.exists(
                f"{self.geotiff_obj.local_path()}/.zmetadata")
            print(f"{self.geotiff_obj.s3_path() if self.save_in_s3 else self.geotiff_obj.local_path()} is consolidated? {c}")
            with zarr.open(store, mode='r') as z:
                print(z.tree())
