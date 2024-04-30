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
from pyspark.sql.functions import to_date, count, col, row_number
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

    rideshare_data = rideshare_data.withColumn('date', from_unixtime(col("date"),"M")) # Changing UNIX timestamp to just month
    rideshare_data = rideshare_data.withColumnRenamed('date', 'Month') # Renaming column from 'date' to 'Month' as required in the desired format
    
    new_joined_table = rideshare_data.join(taxi_zone_lookup_df, rideshare_data.dropoff_location == taxi_zone_lookup_df.LocationID, 'inner') # Join operation is applied

    final_table = new_joined_table.withColumnRenamed('Borough', 'Dropoff_Borough') # New column names based on Dropoff_Location
    
    final_table = final_table.groupBy('Dropoff_Borough', 'Month').count() # Grouping by 'Dropoff_Borough' and 'Month' and applying count function to get total number of dropoffs from that borough each month

    final_table = final_table.withColumnRenamed('count', 'trip_count') # Renaming 'count' to 'trip_count' as required in the desired format

    windowMonth = Window.partitionBy("Month").orderBy(col("trip_count").desc()) # Creating partitions for each month, and within each partition, sorting the rows according to trip count in descending order
    
    final_table = final_table.withColumn("row", row_number().over(windowMonth)) # Creating another column called "row" which consists of the rank for each row in each month

    final_table = final_table.filter(col("row") <= 5) # Filtering out top 5 rows for each month

    final_table = final_table.sort(col('Month').asc(), col('row').asc()) # Sorting the final dataframe in ascending order by 'Month' column and 'row' column

    final_table.drop('row').show(25, truncate = False) # Displaying all 25 rows

    spark.stop()