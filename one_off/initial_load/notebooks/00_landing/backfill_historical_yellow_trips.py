# Databricks notebook source
import urllib.request #to open and read urls download files over http or https
import shutil #high level file operations copoying moving deleting
import os #to allow us creating a directly mainly to allow functions interacting with the operating system



# COMMAND ----------

#this code chunk was built layer by layer. we started with a static 2025-01 and then made it dynamic.

#create a list of dates

#dates_to_process = ['2025-06','2025-07','2025-08','2025-09','2025-10','2025-11']
#these are the most recent months
#however once we start incremental, we are commenting this dateline out

dates_to_process = ['2025-05','2025-06','2025-07','2025-08','2025-09','2025-10'] # because something about 2 months loading time vendors.

for date in dates_to_process:

    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{date}.parquet"

    response = urllib.request.urlopen(url) #this will open the url and get it in the binary form

    dir_path = f"/Volumes/nyctaxi/00_landing/data_sources/nyctaxi_yellow/{date}" # this is copied from the catalog

    os.makedirs(dir_path, exist_ok = True)


    #i will write the contents in the specified local files path

    local_path = f"{dir_path}/yellow_tripdata_{date}.parquet"


    #i can use the shutils to cpopy the binary file to this path

    with open(local_path, 'wb') as f:
        shutil.copyfileobj(response, f)


# COMMAND ----------

