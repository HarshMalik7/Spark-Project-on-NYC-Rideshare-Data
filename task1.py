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
from graphframes import *


if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("TestDataset")\
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

    new_joined_table = rideshare_data.join(taxi_zone_lookup_df, rideshare_data.pickup_location == taxi_zone_lookup_df.LocationID, 'inner').drop('LocationID') # First join operation is applied

    new_joined_table = new_joined_table.withColumnRenamed('Borough', 'Pickup_Borough').withColumnRenamed('Zone', 'Pickup_Zone').withColumnRenamed('service_zone', 'Pickup_service_zone') # New column names based on Pickup_Location
    
    final_table = new_joined_table.join(taxi_zone_lookup_df, new_joined_table.dropoff_location == taxi_zone_lookup_df.LocationID, 'inner').drop('LocationID') # Second join operation is applied

    final_table = final_table.withColumnRenamed('Borough', 'Dropoff_Borough').withColumnRenamed('Zone', 'Dropoff_Zone').withColumnRenamed('service_zone', 'Dropoff_service_zone') # New column names based on Dropoff_Location

    final_table = final_table.withColumn('date', from_unixtime(col("date"),"yyyy-MM-dd")) # Changing the date from UNIX timestamp to yyyy-MM-dd format
    
    row = final_table.count()
    print("The Schema of the Joined Table is:")
    final_table.printSchema()
    print("The number of rows in the table is: ", row)
    print("The final table is")
    final_table.show(10)
    
    spark.stop()