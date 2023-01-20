# Databricks notebook source
# MAGIC %md ### ERA5 ETL Precipitation Data
# MAGIC 
# MAGIC This notebook extracts precipitation_amount_1hour_Accumulation from ERA5, adds a H3 index and stores it in a queriable form. 
# MAGIC 
# MAGIC #### Summary
# MAGIC * Extract precipitation_amount_1hour_Accumulation files from ERA5 using the boto3 library.
# MAGIC * Convert to a PySpark dataframe using spark-xarray.
# MAGIC * Add an H3 index using h3-spark.
# MAGIC * Write to a queriable databricks table and parquet file. 
# MAGIC * Test if the table is queriable on the H3 Index
# MAGIC 
# MAGIC #### Parameters 
# MAGIC * date: date of the precipitation file to use. 

# COMMAND ----------

# MAGIC %md ##### Package install - alternatively preload on cluster rather than install within notebook

# COMMAND ----------

# MAGIC %pip install boto3 botocore h3 h3-pyspark 

# COMMAND ----------

# MAGIC %pip install git+https://github.com/andersy005/spark-xarray.git

# COMMAND ----------

# MAGIC %md ##### Load libraries

# COMMAND ----------

import boto3
import botocore
import datetime
import sparkxarray as xr
import h3
import h3_pyspark
from pyspark.sql import SparkSession
from pyspark.sql import types as T
from pyspark.sql import functions as F
from pyspark.sql import Row
from sparkxarray.reader import ncread


# COMMAND ----------

# MAGIC %md ##### Add widgets

# COMMAND ----------

dbutils.widgets.text(
    "date", "2022/05", "1. Select date for file in YYYY/MM format"
)
date = getArgument("date")

# COMMAND ----------

# MAGIC %md #### Extract

# COMMAND ----------

# MAGIC %md ##### Use Boto3 to access and download precipitation file

# COMMAND ----------

client = boto3.client('s3', config=botocore.client.Config(signature_version=botocore.UNSIGNED))

path = f'{date}/data/precipitation_amount_1hour_Accumulation.nc'
file_name = path.split('/')[-1]

print(f'Path: {path}')

# COMMAND ----------

client.download_file('era5-pds', path, file_name)

# COMMAND ----------

# MAGIC %md ##### Use spark xarray to read in the netcdf to rdd
# MAGIC 
# MAGIC Spark Xarray allows for the large netcdf to be effectively processed as an rdd

# COMMAND ----------

rdd = ncread(spark.sparkContext, file_name, mode='single', partition_on=['time1'], partitions=100)

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC Convert each rdd element to multi index dataframe 

# COMMAND ----------

rdd_pandas_df = rdd.map(lambda x: x.to_dataframe().reset_index())

# COMMAND ----------

# MAGIC %md 
# MAGIC To effectively create a pyspark df change the format again to dict

# COMMAND ----------

rdd_dict = rdd_pandas_df.map(lambda df: df.to_dict(orient='records'))

# COMMAND ----------

# MAGIC %md ##### Convert the RDD of dictionaries to an RDD of Row objects
# MAGIC Handle the pandas date time here to enable pyspark df creation

# COMMAND ----------

rdd_rows = rdd_dict.flatMap(lambda x: x).map(lambda x: Row(lon=x['lon'], lat=x['lat'], time1=x['time1'].to_pydatetime(), nv=x['nv'], time1_bounds=x['time1_bounds'].to_pydatetime(), precipitation_amount_1hour_Accumulation=x['precipitation_amount_1hour_Accumulation']))

# COMMAND ----------

# MAGIC %md ##### Set schema and create dataframe

# COMMAND ----------

schema = T.StructType([
    T.StructField("lon", T.DoubleType(), True),
    T.StructField("lat", T.DoubleType(), True),
    T.StructField("time1", T.TimestampType(), True),
    T.StructField("nv", T.IntegerType(), True),
    T.StructField("time1_bounds", T.TimestampType(), True),
    T.StructField("precipitation_amount_1hour_Accumulation", T.DoubleType(), True)
])

df = spark.createDataFrame(rdd_rows, schema)

# COMMAND ----------

# MAGIC %md #### Transform

# COMMAND ----------

# MAGIC %md ##### Add H3 index

# COMMAND ----------

df = df.withColumn('resolution', F.lit(9))
df = df.withColumn('h3_9', h3_pyspark.geo_to_h3('lat', 'lon', 'resolution'))

# COMMAND ----------

# MAGIC %md #### Load

# COMMAND ----------

table_name = 'precipitation_1_hour_2022_05'

# COMMAND ----------

# MAGIC %md ##### Write Parquet
# MAGIC Optionally write to parquet file - this could be to an S3 mount if wanting to use an additional tool like S3 select to manipulate the data.

# COMMAND ----------

write_parquet: = False

# COMMAND ----------

if write_parquet:
    df.write.mode("overwrite").parquet(f'{table_name}.parquet')

# COMMAND ----------

# MAGIC %md ##### Write as table

# COMMAND ----------

df.write.mode("overwrite").saveAsTable(f'default.{table_name}')
print(f"Wrote to default.{table_name }")

# COMMAND ----------

# MAGIC %md #### Tests

# COMMAND ----------

# MAGIC %md ##### Create and run a test to see if table is queriable on the h3 index 
# MAGIC 
# MAGIC * Check if the column exists in the dataframe.
# MAGIC * The column has data.
# MAGIC * The column is a string (hashed index).
# MAGIC * The H3 index column is valid.

# COMMAND ----------

def test_dataframe_queriable(df, h3_index_column):
    try:
        if h3_index_column not in df.columns:
            raise ValueError(f"{h3_index_column} column not found in the dataframe")

        if df.filter(df[h3_index_column].isNotNull()).count() == 0:
            raise ValueError(f"{h3_index_column} column is empty")

        if df.select(h3_index_column).dtypes[0][1] != "string":
            raise ValueError(f"{h3_index_column} column is not of type string")

        h3_indexes = (
            df.select(h3_index_column).distinct().rdd.flatMap(lambda x: x).collect()
        )
        for h3_index in h3_indexes:
            if not h3.h3_is_valid(h3_index):
                raise ValueError(f"{h3_index} is not a valid h3 index")
        print(f"Dataframe is queriable based on {h3_index_column} column")

        return True

    except Exception as e:
        print(
            f"Dataframe is not queriable based on the {h3_index_column} column: {str(e)}"
        )
        return False


df = spark.sql(f"select * from default.{table_name} limit 10")
test_dataframe_queriable(df, "h3_9")


# COMMAND ----------

# MAGIC %md ##### Queries against the data 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.precipitation_1_hour_2022_05
# MAGIC where time1_bounds > '2022-04-30'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select count(*) as records from default.precipitation_1_hour_2022_05
