# Databricks notebook source
import urllib.request
import shutil
import os

#target URL of the public csv file to download

url= "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

#open a connection to the remote URL and fetch the Parquet file as a stream 
response = urllib.request.urlopen(url)

#create the destination directory for storing the downloaded Parquet file
dir_path = "/Volumes/nyctaxi/00_landing/data_sources/lookup"
os.makedirs(dir_path, exist_ok=True)

#define the full local path (including filename) wher ethe file will be saved
local_path = f"{dir_path}/taxi_zone_lookup.csv"

#Write the contents of the response stream to the specified local file path
with open(local_path, 'wb') as f:
    shutil.copyfileobj(response, f)