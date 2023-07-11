Soils Revealed Data
==============================

This folder outlines data processes for the Soils Revealed project.

--------

## Setup

### The environment
To run the notebooks you need to create an environment with the dependencies. There are two options:
#### Docker

If you have [docker](https://docs.docker.com/engine/install/) in your system, 
you run a jupyter lab server with:

``` bash
docker compose up --build
```

And if you want to get into the container, use a terminal in jupyter lab, 
vscode remote development or run this command:

```shell
docker exec -it soils_revealed_notebooks /bin/bash
```

#### Conda environment

Create the environment with:

``` bash
mamba env create -n soils-revealed -f environment.yml
```
This will create an environment called soils-revealed with a common set of dependencies.

### `git` (if needed) and pre-commit hooks

If this project is a new and standalone (not a module in a bigger project), you need to initialize git:

``` bash
git init
```

If the project is already in a git repository, you can skip this step.

To install the **pre-commit hooks**, with the environment activated and in the project root directory, run:

``` bash
pre-commit install
```

## Update the environment

If you need to update the environment installing a new package, you simply do it with:

``` bash
mamba install [package]  # or `pip install [package]` if you want to install it via pip
```

then update the environment.yml file so others can clone your environment with:

``` bash
mamba env export --no-builds -f environment.yml
```

------------
## Datasets

- [Space-time statistical modelling of soil organic carbon concentration and stocks](https://drive.google.com/file/d/1MWqfLpggEZldKvtu9vVgkfKqXn3F7P1u/view?usp=sharing)
- [Soil carbon debt of 12,000 years of human land use](https://www.pnas.org/content/114/36/9575.abstract)
------------

## Processing

This doc explains how to go from raw data to the final precalculations.
It consists of 2 scripts located in the `processing` folder:
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
