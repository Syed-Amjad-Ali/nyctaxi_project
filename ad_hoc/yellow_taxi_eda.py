# Databricks notebook source
from pyspark.sql.functions import * 

# COMMAND ----------

# MAGIC %md
# MAGIC **Which vendor makes the most revenue?**

# COMMAND ----------

#yellowtrips enriched contains info about both vendor info and total amount for each trip

df = spark.read.table("nyctaxi.02_silver.yellow_trips_enriched")

#df.display()

# COMMAND ----------

df.\
    groupBy("vendor").\
        agg(
            round(sum("total_amount"),2).alias("total_revenue")
            ).\
        orderBy("total_revenue", ascending = False).\
        display()

# COMMAND ----------

#Curb Mobility earn the most

# COMMAND ----------

# MAGIC %md
# MAGIC **What is the most popular pickup borough?**

# COMMAND ----------

df.\
    groupBy("pu_borough").\
    agg(
        count("*").alias("number_of_trips")
    ).\
    orderBy("number_of_trips", ascending = False).\
    display()

# COMMAND ----------

#manhattan

# COMMAND ----------

# MAGIC %md
# MAGIC **What is the most common journey (borough to borough)?**

# COMMAND ----------

df.\
    groupBy("pu_borough", "do_borough").\
    agg(
        count("*").alias("number_of_trips")
    ).\
    orderBy("number_of_trips", ascending = False).\
    display()

# COMMAND ----------

#We can also do as follows

df.\
    groupBy(concat("pu_borough", lit(" -> "), "do_borough").alias("journey")).\
    agg(
        count("*").alias("number_of_trips")
    ).\
    orderBy("number_of_trips", ascending = False).\
    display()

# COMMAND ----------

#so manhattan to manhattan is most popular. also queens to manhattan

# COMMAND ----------

# MAGIC %md
# MAGIC **Create a time series chart showing the number of trips and total revenue per day**

# COMMAND ----------

#its best to use the gold layer here.

df2 = spark.read.table("nyctaxi.03_gold.daily_trip_summary")

df2.display()
#we can use the interactive chart in the below result
#but without scaling it looks funny if line chart
#so we can use combo chart

# COMMAND ----------

# so we can see weekly patterns