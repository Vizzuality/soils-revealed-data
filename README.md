# Soils Revealed Data

This folder outlines data processes for the Soils Revealed project.

## Datasets

- [Space-time statistical modelling of soil organic carbon concentration and stocks](https://drive.google.com/file/d/1MWqfLpggEZldKvtu9vVgkfKqXn3F7P1u/view?usp=sharing)
- [Soil carbon debt of 12,000 years of human land use](https://www.pnas.org/content/114/36/9575.abstract)

## Processing

This doc explains how to go from raw data to the final precalculations.
It consists of 2 scripts:
1. `from_GeoTIFFs_to_Zarr.py` which transform raw GeoTIFFs located in Google Cloud bucket into 
Zarrs in a local or remote (S3) directory.
2. `compute_precalculations.py` It takes the raster (as Zarr) and vector data and computes the precalculations.

First! transform all raw data from GeoTIFFs to Zarrs.  
```shell
python from_GeoTIFFs_to_Zarr.py experimental,global,scenarios
```

Second! Compute precalculations
```shell
python compute_precalculations.py \
experimental,global,scenarios \
political_boundaries,hydrological_basins,biomes,landforms
```