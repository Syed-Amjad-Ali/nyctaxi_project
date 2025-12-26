# Databricks notebook source
from pyspark.sql.functions import current_timestamp, lit, col
from pyspark.sql.types import TimestampType, IntegerType


# COMMAND ----------

df = spark.read.format("csv").option("header", "true").load("/Volumes/nyctaxi/00_landing/data_sources/lookup/taxi_zone_lookup.csv")

# COMMAND ----------

#display(df)
#notice all columns are string types its because csv stores as string also the column naming conventions are off

# COMMAND ----------

df = df.select(
                col("LocationID").cast(IntegerType()).alias("location_id"),
                col("Borough").alias("borough"),
                col("Zone").alias("zone"),
                col("service_zone"),
                current_timestamp().alias("effective_date"),
                lit(None).cast(TimestampType()).alias("end_date")
)

#end date column contains null value thru the lit function
#

# COMMAND ----------

#display(df)
#the records with null values in the end date
#indicate they are active records
#later when implement slowly changing dimensions type 2
#we will make the row inactive by giving it an end date

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("nyctaxi.02_silver.taxi_zone_lookup")

#we will later change the overwrite to 

# COMMAND ----------

#spark.read.table("nyctaxi.02_silver.taxi_zone_lookup").display()