# ERA5 ETL Pipeline


## Intro


This repository contains a notebook that extracts NetCDF hourly precipitation data from ERA5, adds an H3 index and converts to a queriable parquet format. 


### Context


#### ERA5

[ERA5](https://www.ecmwf.int/en/forecasts/datasets/reanalysis-datasets/era5) is a global reanalysis dataset produced by the European Centre for Medium-Range Weather Forecasts (ECMWF). It provides a consistent and accurate dataset of weather and climate variables from 1979 to present, with a horizontal resolution of approximately 31 km. The dataset includes information on temperature, precipitation, wind, and other atmospheric variables, which makes it a valuable resource for a wide range of applications including weather forecasting, climate research, and air quality studies. 

#### Converting to queriable format

NetCDF (Network Common Data Form) is convenient when storing large scientific multi-dimensional datasets however it is less than optimal when it comes to querying and analysing data. This is due to a number of reasons including its hierarchical formatting, binary format and complex meta data. Hence converting to a more queriable format is preferable. Parquet offers an effective alternative as a columnar storage format that is specifically designed to be queried and analyzed using SQL-based tools. It is optimized for reading and writing large datasets and allows for efficient compression and encoding of data, making it a more efficient storage format.

## Setup

The notebook is designed to be run within a databricks environment on a spark cluster with aproximately 16 cores and 50 gig of memory. 

To use this notebook, you will need access to a Databricks cluster and the necessary permissions to install libraries and create tables. The ERA5 data is also needed to be available via its public S3 location s3://era5-pds. 

## Summary

The notebook does the following:

* Extracts precipitation_amount_1hour_Accumulation files from ERA5 using the boto3 library.
* Converts the data to a PySpark dataframe using spark-xarray.
* Adds an H3 index using h3-spark.
* Writes the data to a queriable databricks table and parquet file.
* Tests if the table is queriable on the H3 Index
* The notebook takes in one parameter, the date of the precipitation file to use in the format YYYY/MM.


## Libaries 


The notebook uses the following libraries: boto3, botocore, spark-xarray, h3-pyspark, pyspark.sql. 

The notebook also installs the necessary libraries via pip. If the libraries are preloaded on the cluster, this step can be skipped.



## Automation 

This notebook can fit within a databricks workflow to run the data regularly as new data becomes available. 

