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
from pyspark.sql.types import DoubleType
# from graphframes import *



if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("Task5")\
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

    rideshare_data = rideshare_data.withColumn('month', from_unixtime(col('date'), 'M')).withColumn('day', from_unixtime(col('date'), 'dd')) # Creating two new columns, one with just month and other with just the day

    rideshare_data = rideshare_data.filter(rideshare_data.month == '1') # Retreiving data only for January

    rideshare_data = rideshare_data.withColumn('request_to_pickup', col('request_to_pickup').cast(DoubleType())) # Casting the 'request_to_pickup' column to floating point number to enable mathematical computations on it

    df = rideshare_data.groupBy('day').avg('request_to_pickup') # Grouping data by the different days of month january

    df = df.sort(col('day').asc()) # Sorting the data according to the days of the month january

    df.show(31, truncate = False) # Displaying the final dataframe

    df.coalesce(1).write.option('header', True).csv("s3a://" + s3_bucket + '/January-data/') # Saving the data to personal bucket so it can be used for plotting the histogram
    
    spark.stop()