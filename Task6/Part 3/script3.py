import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import from_unixtime, date_format
from pyspark.sql.functions import to_date, count, col
# from graphframes import *



if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("Task6")\
        .getOrCreate()
    
    # shared read-only object bucket containing datasets
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']

    s3_endpoint_url = os.environ['S3_ENDPOINT_URL']+':'+os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']

    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")

    
    rideshare_data = spark.read.options(header=True).csv("s3a://" + s3_data_repository_bucket + "/ECS765/rideshare_2023/rideshare_data.csv")# /read from the bucket file

    taxi_zone_lookup_df = spark.read.options(header=True).csv("s3a://" + s3_data_repository_bucket + "/ECS765/rideshare_2023/taxi_zone_lookup.csv")# /read from the bucket file

    joined_table = rideshare_data.join(taxi_zone_lookup_df, rideshare_data.pickup_location == taxi_zone_lookup_df.LocationID, 'inner').withColumnRenamed('Borough', 'Pickup_Borough').withColumnRenamed('Zone', 'Pickup_Zone') # Joining the dataframes to obtain the pickup borough names from the location IDs and renaming the columns to desired names

    joined_table = joined_table.join(taxi_zone_lookup_df, joined_table.dropoff_location == taxi_zone_lookup_df.LocationID, 'inner').withColumnRenamed('Borough', 'Dropoff_Borough') # Second join operation to obtain Dropoff Borough names

    joined_table = joined_table.filter((col("Pickup_Borough") == 'Brooklyn') & (col('Dropoff_Borough') == 'Staten Island')) # Filtering the joined table to only retain records of taxi rides from Brooklyn to Staten Island
    
    df = joined_table.select(col('Pickup_Borough'), col('Dropoff_Borough'), col('Pickup_Zone')) # Selecting data only for the Pickup Boroughs, Dropoff Boroughs and the Pickup Zone
    
    count = df.count() # Calling action count and storing the number in a variable so that we can display both, the table and total number of trips together in a single screenshot
    
    df.show(10, truncate = False) # Displaying the final DataFrame (10 rows only) in the terminal
    
    print("Total Number of Trips from Brooklyn to Staten Island are", count) # Printing the total number of trips
    
    spark.stop()