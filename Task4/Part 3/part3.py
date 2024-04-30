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
        .appName("Task4")\
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

    rideshare_data = rideshare_data.withColumn('trip_length', col('trip_length').cast(DoubleType())).withColumn('driver_total_pay', col('driver_total_pay').cast(DoubleType())) # Casting the column 'trip_length' and 'driver_total_pay' from string to float so that avg() function can work on it

    rideshare_data = rideshare_data.cache() # Storing this dataframe in cache memory since it will be used more than once
    
    df1 = rideshare_data.groupBy('time_of_day').avg('trip_length') # Grouping data by different times of the day and taking average of trip length during each time.

    df2 = rideshare_data.groupBy('time_of_day').avg('driver_total_pay').withColumnRenamed('time_of_day', 'time') # Grouping data by different times of the day and taking average of driver pay during each time.

    final_df = df1.join(df2, df1.time_of_day == df2.time).withColumn('average_earning_per_mile', (col('avg(driver_total_pay)') / col('avg(trip_length)'))) # Joining the dataframes created in the previous 2 subtasks and then creating a new column from the derived columns

    final_df = final_df.drop('avg(trip_length)').drop('avg(driver_total_pay)').drop('time') # Dropping the columns not required anymore

    final_df.show(truncate = False) # Displaying the final dataframe in terminal
    
    spark.stop()