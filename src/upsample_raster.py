import subprocess
import rasterio
from rasterio.enums import Resampling


def align_rasters(bounds, xres, yres, input_raster, output_raster):
    cmd = f"gdalwarp -te {bounds[0]} {bounds[1]} {bounds[2]} {bounds[3]} -tr {xres} {yres} {input_raster} {output_raster}"
    print(f"Processing: {cmd}")
    r = subprocess.call(cmd, shell=True)
    if r == 0:
        print("Task created")
    else:
        print("Task failed")
    print("Finished processing")
    
    
for year in ["2018"]:
    raster1_path = '../data/raw/raster_data/SOC_2018_4326.tif'
    raster2_path = f"../data/raw/raster_data/ESA_{year}_ipcc.tif"
    input_raster = f"../data/raw/raster_data/ESA_{year}_ipcc_res.tif"
    output_raster = f"../data/raw/raster_data/ESA_{year}_ipcc_res_align.tif"

    with rasterio.open(raster1_path) as src:
        xres = src.res[0]
        yres = src.res[1]
        bounds = src.bounds
        
    with rasterio.open(raster2_path) as dataset:
        scale_factor_x = dataset.res[0]/xres
        scale_factor_y = dataset.res[1]/yres

        profile = dataset.profile.copy()
        # resample data to target shape
        print("Resampling raster")
        data = dataset.read(
            out_shape=(
                dataset.count,
                int(dataset.height * scale_factor_y),
                int(dataset.width * scale_factor_x)
            ),
            resampling=Resampling.nearest
        )

        # scale image transform
        transform = dataset.transform * dataset.transform.scale(
            (1 / scale_factor_x),
            (1 / scale_factor_y)
        )
        profile.update(
            {
                "height": data.shape[-2],
                "width": data.shape[-1],
                "transform": transform
            }
        )
        
    with rasterio.open(input_raster, "w", **profile) as dataset:
        dataset.write(data)
    
    # Align rasters   
    align_rasters(bounds, xres, yres, input_raster, output_raster)


