import os
from typing import Union
from pathlib import Path

import zarr
import s3fs
import rioxarray
import numpy as np
import xarray as xr
from dotenv import load_dotenv
from google.cloud import storage

from utils.data import RasterData

# Load .env variables
load_dotenv()


def read_zarr_from_s3(access_key_id, secret_accsess_key, dataset, group=None):
    # AWS S3 path
    s3_path = f's3://soils-revealed/{dataset}.zarr'
    
    # Initilize the S3 file system
    with s3fs.S3FileSystem(key=access_key_id, secret=secret_accsess_key) as s3:
        store = s3fs.S3Map(root=s3_path, s3=s3, check=False)
        
        # Read Zarr file
        if group:
            ds = xr.open_zarr(store=store, group=group, consolidated=True)
        else:
            ds = xr.open_zarr(store=store, consolidated=True)
       
        return ds


def read_zarr_from_local_dir(path, group=None):
    # Read Zarr file
    if group:
        with xr.open_zarr(store=path, group=group, consolidated=True) as ds:
            return ds

    else:
        with xr.open_zarr(store=path, consolidated=True) as ds:
            return ds
 
#def read_zarr_from_s3(access_key_id, secret_accsess_key, dataset, group=None):
#    # AWS S3 path
#    s3_path = f's3://soils-revealed/{dataset}.zarr'
#    
#    # Initilize the S3 file system
#    s3 = s3fs.S3FileSystem(key=access_key_id, secret=secret_accsess_key)
#    store = s3fs.S3Map(root=s3_path, s3=s3, check=False)
#    
#    # Read Zarr file
#    if group:
#        ds = xr.open_zarr(store=store, group=group, consolidated=True)
#    else:
#        ds = xr.open_zarr(store=store, consolidated=True)
#       
#    return ds 
#
#
#def read_zarr_from_local_dir(path, group=None):
#    # Read Zarr file
#    if group:
#        ds = xr.open_zarr(store=path, group=group, consolidated=True)
#    else:
#        ds = xr.open_zarr(store=path, consolidated=True)
#    
#    return ds
#

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

    def __init__(self, geotiff_obj: RasterData, save_in_s3: bool = False):
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


class ZarrData:
    s3_access_key_id = os.getenv("S3_ACCESS_KEY_ID")
    s3_secret_access_key = os.getenv("S3_SECRET_ACCESS_KEY")

    def __init__(self, raster_obj: RasterData, in_s3: bool = False):
        self.raster_obj = raster_obj
        self.in_s3 = in_s3

    def read_as_xarray(self):
        if self.in_s3:
            # Initilize the S3 file system
            s3 = s3fs.S3FileSystem(key=self.s3_access_key_id, secret=self.s3_secret_access_key)
            store = s3fs.S3Map(root=self.raster_obj.s3_path(), s3=s3, check=False)
            # Read Zarr file
            ds = xr.open_zarr(store=store, group=self.raster_obj.group, consolidated=True)
            # Change dimension name
            if self.raster_obj.group == 'concentration':
                ds = ds.rename({'depht': 'depth'})
        else:
            # Read Zarr file
            ds = xr.open_zarr(store=self.raster_obj.local_path(), group=self.raster_obj.group, consolidated=True)

        # Change coordinates names
        ds = ds.rename({'x': 'lon', 'y': 'lat'})

        return ds
