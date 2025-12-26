# Databricks notebook source
#from pyspark.sql.functions import current_timestamp, lit, col
#from pyspark.sql.types import TimestampType, IntegerType


# COMMAND ----------

#df = spark.read.format("csv").option("header", "true").load("/Volumes/nyctaxi/00_landing/data_sources/lookup/taxi_zone_lookup.csv")

# COMMAND ----------

#display(df)
#notice all columns are string types its because csv stores as string also the column naming conventions are off

# COMMAND ----------

# df = df.select(
#                 col("LocationID").cast(IntegerType()).alias("location_id"),
#                 col("Borough").alias("borough"),
#                 col("Zone").alias("zone"),
#                 col("service_zone"),
#                 current_timestamp().alias("effective_date"),
#                 lit(None).cast(TimestampType()).alias("end_date")
# )

#end date column contains null value thru the lit function
#

# COMMAND ----------

#display(df)
#the records with null values in the end date
#indicate they are active records
#later when implement slowly changing dimensions type 2
#we will make the row inactive by giving it an end date

# COMMAND ----------

# df.write.mode("overwrite").saveAsTable("nyctaxi.02_silver.taxi_zone_lookup")

#we will later change the overwrite to 

# COMMAND ----------

#spark.read.table("nyctaxi.02_silver.taxi_zone_lookup").display()

# COMMAND ----------

# Databricks notebook source

from datetime import datetime
from delta.tables import DeltaTable
from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql.types import TimestampType, IntegerType, StringType

# COMMAND ----------

# Read the taxi zone lookup CSV (with header) into a DataFrame
df = spark.read.format("csv").option("header", True).load("/Volumes/nyctaxi/00_landing/data_sources/lookup/taxi_zone_lookup.csv")

# COMMAND ----------

# Select and rename fields, casting types, and add audit columns
df = df.select(
                col("LocationID").cast(IntegerType()).alias("location_id"),
                col("Borough").alias("borough"),
                col("Zone").alias("zone"),
                col("service_zone"),
                current_timestamp().alias("effective_date"),
                lit(None).cast(TimestampType()).alias("end_date")
            )

# COMMAND ----------

# COMMAND ----------

# This logic has been included to force updates and insertions to the source taxi zone lookup data for demonstration purposes only
# THIS SHOULD NOT BE INCLUDED IN THE FINAL PROJECT CODE

#from pyspark.sql.functions import *

# Insert new record to the source DataFrame
# df_new = spark.createDataFrame(
#     [(999, "New Borough", "New Zone", "New Service Zone")],
#     schema="location_id int, borough string, zone string, service_zone string"
# ).withColumn("effective_date", current_timestamp()) \
#  .withColumn("end_date", lit(None).cast("timestamp"))

# df = df_new.union(df)

# # Updating record for location_id 1
# df = df.withColumn("borough", when(col("location_id")==1, "NEWARK AIRPORT").otherwise(col("borough")))

# COMMAND ----------

# COMMAND ----------



# Fixed point-in-time used to "close" any changed active records
# Using a Python timestamp ensures the exact same value is written and can be referenced if needed
end_timestamp = datetime.now()

# Load the SCD2 Delta table
dt = DeltaTable.forName(spark, "nyctaxi.02_silver.taxi_zone_lookup")

# COMMAND ----------

# -----------------------------
# PASS 1: Close any active rows whose tracked attributes changed
# -----------------------------
# Match only the *active* target rows (end_date IS NULL) with the same business key.
# If any tracked column differs, set end_date to end_timestamp to retire that version.

dt.alias("t").\
    merge(
        source    = df.alias("s"),
        condition = "t.location_id = s.location_id AND t.end_date IS NULL AND (t.borough != s.borough OR t.zone != s.zone OR t.service_zone != s.service_zone)"
    ).\
    whenMatchedUpdate(
        set = { "t.end_date": lit(end_timestamp).cast(TimestampType()) }
    ).\
    execute()

# COMMAND ----------

# COMMAND ----------

# -----------------------------
# PASS 2: Insert new current versions
# -----------------------------
# Now insert a row for:
#   (a) keys we just closed in PASS 1 (no longer an active match), and
#   (b) brand-new keys not present in the target.
# We again match on *active* rows; anything without an active match is inserted.

# get the lists of IDs that have been closed
insert_id_list = [row.location_id for row in dt.toDF().filter(f"end_date = '{end_timestamp}' ").select("location_id").collect()]

# If the list is empty, don't try to insert anything
if len(insert_id_list) == 0:
    print("No updated records to insert")
else:
    dt.alias("t").\
        merge(
            source    = df.alias("s"),
            condition = f"s.location_id not in ({', '.join(map(str, insert_id_list))})"
        ).\
        whenNotMatchedInsert(
            values = { "t.location_id": "s.location_id",
                    "t.borough": "s.borough",
                    "t.zone": "s.zone",
                    "t.service_zone": "s.service_zone",
                    "t.effective_date": current_timestamp(),
                    "t.end_date": lit(None).cast(TimestampType()) }
        ).\
        execute()

# COMMAND ----------

# COMMAND ----------

# -----------------------------
# PASS 3: Insert brand-new keys (no historical row in target)
# -----------------------------
dt.alias("t").\
    merge(
        source    = df.alias("s"),
        condition = "t.location_id = s.location_id"
    ).\
    whenNotMatchedInsert(
        values = { "t.location_id": "s.location_id",
                "t.borough": "s.borough",
                "t.zone": "s.zone",
                "t.service_zone": "s.service_zone",
                "t.effective_date": current_timestamp(),
                "t.end_date": lit(None).cast(TimestampType()) }
    ).\
    execute()