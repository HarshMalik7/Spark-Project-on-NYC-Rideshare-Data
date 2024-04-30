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
from pyspark.sql.types import DoubleType
from graphframes import *



if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("Task2")\
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

    rideshare_data = rideshare_data.withColumn('date', from_unixtime(col("date"),"-M")) # Changing UNIX timestamp to just month
    
    rideshare_data = rideshare_data.withColumn("driver_total_pay",col("driver_total_pay").cast(DoubleType())) # Casting driver_total_pay from string to float
    
    data = rideshare_data.groupBy('business','date').sum('driver_total_pay') # Grouping by business and date(month)
    
    data = data.cache() # Saving the dataframe in cache memory since it will be used again
    
    data_uber = data.filter(rideshare_data.business == 'Uber') # Filtering data for Uber
    data_lyft = data.filter(rideshare_data.business == 'Lyft') # Filtering data for Lyft
    
    data_uber = data_uber.select(concat_ws('', data.business, data.date).alias('business-date'), 'sum(driver_total_pay)') # Concating business name and month to get the desired labels for Uber
    data_lyft = data_lyft.select(concat_ws('', data.business, data.date).alias('business-date'), 'sum(driver_total_pay)') # Concating business name and month to get the desired labels for Lyft

    # data.show() # Visualising the dataframe in terminal
    print("The data of Driver's total pay for Uber:")
    data_uber.show()
    print("The data of Driver's total pay for Lyft:")
    data_lyft.show()
    
    data_uber.coalesce(1).write.option('header', True).csv("s3a://" + s3_bucket + '/Uber-data_part3/') # Saving the dataframe as a single csv file to my personal bucket
    data_lyft.coalesce(1).write.option('header', True).csv("s3a://" + s3_bucket + '/Lyft-data_part3/') # Saving the dataframe as a single csv file to my personal bucket
    
    spark.stop()