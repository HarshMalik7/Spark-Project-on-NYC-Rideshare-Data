import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime

from functools import reduce
from pyspark.sql.functions import col, lit, when
from pyspark import *
from pyspark.sql import *
from pyspark.sql.types import *
import graphframes
from graphframes import *

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "graphframes:graphframes:0.8.2-spark3.2-s_2.12") \
        .appName("task8") \
        .getOrCreate()

    sqlContext = SQLContext(spark)
    # shared read-only object bucket containing datasets
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']
    s3_endpoint_url = os.environ['S3_ENDPOINT_URL'] + ':' + os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']

    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")

    # Task 1
    # Schema for the table we will use for vertex information
    vertexSchema = StructType([StructField("id", IntegerType(), False),
                               StructField("Borough", StringType(), True),
                               StructField("Zone", StringType(), True),
                               StructField("service_zone", StringType(), True)])

    # Schema for the table we will use for edge information
    edgeSchema = StructType([StructField("business", StringType(), False),
                             StructField("src", IntegerType(), False),
                             StructField("dst", IntegerType(), False)])


    # Loading the "rideshare_data.csv" dataset through the DataFrame API and implementing our predefined schema for edges of the graph.
    edgesDF = spark.read.format("csv").options(header='True').schema(edgeSchema).csv("s3a://" + s3_data_repository_bucket + "/ECS765/rideshare_2023/rideshare_data.csv")

    # Dropping the column business because it is not required in the edges dataframe.
    edgesDF = edgesDF.drop("business")

    # Loading the "taxi_zone_lookup.csv" dataset through the DataFrame API and implementing our predefined schema for vertex information
    verticesDF = spark.read.format("csv").options(header='True').schema(vertexSchema).csv("s3a://" + s3_data_repository_bucket + "/ECS765/rideshare_2023/taxi_zone_lookup.csv")

    # Task 2 : Displaying the Edges and Vertices DataFrame
    # showing 10 rows from the vertices and edges tables
    edgesDF.show(10, truncate = False)
    verticesDF.show(10, truncate = False)

    # Task 3 : Creating the graph and displaying the 'src', 'edge' and 'dst'
    # create a graph using the vertices and edges
    graph = GraphFrame(verticesDF, edgesDF)

    # Displaying 'src', 'edge' and 'dst'
    graph.triplets.show(10, truncate = False)

    # Task 4 : Vertices with same borough and service zone
    same_borough_servicezone = graph.find("(a)-[]->(b)").filter("a.Borough = b.Borough").filter("a.service_zone = b.service_zone") # Finding nodes between whom there is an edge, and filtering that result to get nodes with same 'Borough' and 'Service Zone'
    selected_points = same_borough_servicezone.select("a.id", "b.id", "b.Borough", "b.service_zone") # Selecting the desired columns for the final result
    count = selected_points.count() # Getting the total number of connected vertices with same borough and service zone
    selected_points.show(10, truncate = False) # Displaying the result
    print("The total number of connected vertices with same borough and service zone is", count) # Printing the total count of vertices with same borough and service zone

    # # Task 5 : Calculating Pagerank
    pagerank = graph.pageRank(resetProbability=0.17, tol=0.01) # Calculating the Pagerank for each node with the parameters defined in the task.
    pagerank.vertices.select("id", "pagerank").sort(col("pagerank").desc()).show(5, truncate = False) # Selecting the desired columns for final output, sorting it on the basis of Pagerank calculated in descending order and displaying the top 5 results.
    
    

    spark.stop()