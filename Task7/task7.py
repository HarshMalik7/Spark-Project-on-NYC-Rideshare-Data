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
from pyspark.sql.functions import to_date, count, col, concat_ws
# from graphframes import *



if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("Task7")\
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

    joined_table = rideshare_data.join(taxi_zone_lookup_df, rideshare_data.pickup_location == taxi_zone_lookup_df.LocationID).withColumnRenamed('Zone', 'Pickup_Zone') # First join operation is applied to get the Pickup Zones

    joined_table = joined_table.join(taxi_zone_lookup_df, joined_table.dropoff_location == taxi_zone_lookup_df.LocationID).withColumnRenamed('Zone', 'Dropoff_Zone') # Second join is applied to get the Dropoff Zones

    joined_concat = joined_table.select(concat_ws(" to ", joined_table.Pickup_Zone, joined_table.Dropoff_Zone).alias('Route'), 'business') # Pickup and Dropoff Zones are concatenated with the string " to " in between them and the columns business is retained.

    joined_concat = joined_concat.cache() # This DataFrame is cached because it will be used again to get 2 new DataFrames

    uber_count = joined_concat.filter(col('business') == 'Uber').groupBy('Route').count().withColumnRenamed('Count', 'uber_count') # Getting the count of total uber rides for all possible Routes

    lyft_count = joined_concat.filter(col('business') == 'Lyft').groupBy('Route').count().withColumnRenamed('Count', 'lyft_count').withColumnRenamed('Route', 'Routee') # Getting the count of total lyft rides for all possible Routes

    final_df = uber_count.join(lyft_count, uber_count.Route == lyft_count.Routee, "outer").withColumn('total_count', col('uber_count') + col('lyft_count')).drop('Routee') # Joining the DataFrames created above with an Outer join because it may be possible that Lyft may not contain all the routes that Uber has been on and vice versa. Replacing the null values with 0 for businesses that haven't been on a particular route

    final_df = final_df.sort(col('total_count').desc()) # Sorting the data to get top 10 routes

    final_df.show(10, truncate = False) # Displaying top 10 routes
    
    spark.stop()