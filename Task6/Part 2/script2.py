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

    joined_table = rideshare_data.join(taxi_zone_lookup_df, rideshare_data.pickup_location == taxi_zone_lookup_df.LocationID, 'inner').withColumnRenamed('Borough', 'Pickup_Borough') # Joining the dataframes to obtain the pickup borough names from the location IDs and renaming the column to avoid confusion

    joined_table = joined_table.filter(col("time_of_day") == 'evening') # Filtering the joined table to only retain records of taxi rides completed in evening
    
    df = joined_table.groupBy('Pickup_Borough', 'time_of_day').count() # Grouping data by the Pickup Boroughs and time of the day to find the trip count at evening for every borough.

    df = df.withColumnRenamed('count', 'trip_count') # Renaming the column as desired in the task description
    
    df.show(df.count(), truncate = False) # Displaying the final DataFrame in the terminal
    
    spark.stop()