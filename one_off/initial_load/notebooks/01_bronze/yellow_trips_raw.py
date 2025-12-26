# Databricks notebook source
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

#loading all parquet files in the landing layer
#the * look at this folder and all of it subfolders and appends all the files
df = spark.read.format("parquet").load("/Volumes/nyctaxi/00_landing/data_sources/nyctaxi_yellow/*")

# COMMAND ----------

#to get the timestamp at the point of execution of this code cell
df= df.withColumn("processed_timestamp", current_timestamp())


# COMMAND ----------

#in the bronze schema. soon we will change this to an incremental process
#by default this is saved as managed delta table
df.write.mode("overwrite").saveAsTable("nyctaxi.01_bronze.yellow_trips_raw")

# COMMAND ----------

#lets confirm the table exists
#spark.read.table("nyctaxi.01_bronze.yellow_trips_raw").display()