# TULFLOW

[![Build Status](https://circleci.com/gh/tulibraries/tulflow.svg?style=svg&circle-token=3f42b93c7525db72aa5411eded38a862296ed707)](https://circleci.com/gh/tulibraries/tulflow)

Small Python package / library for common ETL-adjacent functions, primarily used in the TU Libraries Airflow tasks.

## Updating/Adding Package

If you make any updates or additions to this repository, a release will need to be created in order for the related Airflow DAGS to pick it up.

Releases are currently managed by PyPi and this project can be found [here](https://pypi.org/project/tulflow/). In order for releases to be pushed to PyPi, you will need to update ```setup.py``` with the new version number. 

Within your DAG project, you will need to update the Pipfile to use the latest version of Tulflow.

* Update release version in Pipfile
* run ```pipenv shell```
* run ```pipenv install tulflow```
* Commit your changes in each repository

Right now we are using Tulflow in 3 different DAG repositories, so you should update the release in each one in order to ensure that the most recent version is actually the one being used.

* [cob_datapipeline](https://github.com/tulibraries/cob_datapipeline)
* [funcake_dags](https://github.com/tulibraries/funcake_dags)
* [mainfold_airflow_dags](https://github.com/tulibraries/manifold_airflow_dags)

## How to Use

If you would like to use one of these common functions in an Airflow DAG, you will need to include it as one of the import statements at the top of the file.

Example: ```from tulflow import process```
