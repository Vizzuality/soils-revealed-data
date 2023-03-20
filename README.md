Soils Revealed Data
==============================

This folder outlines data processes for the Soils Revealed project.

Project Organization
------------

``` txt
├── LICENSE                       <- The LICENSE using this project.
├── README.md                     <- The top-level README for developers using this project.
├── CHANGELOG.md                  <- The top-level CHANGELOG for developers using this project.
├── env.default                   <- Environment vars definition
├── Makefile                      <- Makefile with commands
├──.editorconfig                  <- Helps maintain consistent coding styles
├──.pre-commit-config             <- Helps setup github basic precommit hooks
├── docker-compose.yml            <- Docker configs environment definition
├── .gitignore                    <- files don't want to copy in githubs
├── .github                       <- github configs
│   └── templates                 <- github templates for issues and pull requests
├── data
│   ├── processed                 <- The final, canonical data sets.
│   └── raw                       <- The original data.
├── processing
└── notebooks                     <- Naming convention is a number (for ordering),
    │                                the creator's initials, and a short `-` delimited e.g.
    │                                `1.0-jqp-initial-data-exploration`.
    ├──.dockerignore
    ├──environment.conda-lock.yaml<- Notebook env.lock that will be used to quick install dependencies
    ├──package.yaml               <- Notebooks requirements base on conda env
    ├──Dockerfile                 <- Sets up Jupyter notebooks environment
    ├──jupyter_server_config.py   <- Configure Jupyter notebooks
    ├──template_notebooks         <- where the notebooks template will live.
    │
    ├──Lab                        <- Testing and development
    │
    └──Final                      <- The final cleaned notebooks for reports/ designers /
                                     developers etc.

```

--------

## Steps for use:

#### Setup one of your environments

- With [docker]() and [docker-compose]() in your system, you can develop inside containers:
``` bash
make up
```
And if you want to get into the main container:
``` bash
make inside
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
<p><small>Project based on the <a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project template</a>. #cookiecutterdatascience</small></p>
