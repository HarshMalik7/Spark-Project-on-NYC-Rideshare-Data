import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.window import Window
from pyspark.sql.functions import from_unixtime, date_format
from pyspark.sql.functions import to_date, count, col, concat_ws
from pyspark.sql.types import DoubleType
# from graphframes import *


if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("Task3")\
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

    rideshare_data = rideshare_data.withColumn("driver_total_pay",col("driver_total_pay").cast(DoubleType())) # Casting the 'driver_total_pay' column values from string to float because it will be used later to perform computation on
    
    new_joined_table = rideshare_data.join(taxi_zone_lookup_df, rideshare_data.pickup_location == taxi_zone_lookup_df.LocationID, 'inner').drop('LocationID').drop('Zone').drop('service_zone') # Join operation is applied and columns 'LocationID', 'Zone' and 'service_zone' are dropped.

    new_joined_table = new_joined_table.withColumnRenamed('Borough', 'Pickup_Borough') # New column name as required

    final_table = new_joined_table.join(taxi_zone_lookup_df, new_joined_table.dropoff_location == taxi_zone_lookup_df.LocationID, 'inner') # Second join operation is applied

    final_table = final_table.withColumnRenamed('Borough', 'Dropoff_Borough') # New Column Name (Optional Step)

    final_table = final_table.select(concat_ws(' to ', final_table.Pickup_Borough, final_table.Dropoff_Borough).alias('Route'), 'driver_total_pay') # Concatenating columns 'Pickup_Borough' and 'Dropoff_Borough' to get a new column 'Route' and also retaining the column 'total_driver_pay'
    
    final_table = final_table.groupBy('Route').sum('driver_total_pay') # Grouping the data and computing the sum of total_driver_pay for each group.

    final_table = final_table.withColumnRenamed('sum(driver_total_pay)', 'total_profit') # Renaming the column according to the desired output. (Optional)

    final_table = final_table.sort(col('total_profit').desc()) # Sorting the table according to the profit earned through each route in descending order to obtain the top routes in decreasing order.

    final_table.show(30, truncate = False) # Displaying the top 30 routes.

    spark.stop()