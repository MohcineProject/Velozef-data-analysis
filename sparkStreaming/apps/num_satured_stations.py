from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from cassandra.cluster import Cluster

clstr=Cluster(['172.22.0.3'])
session=clstr.connect()

qry=''' 
CREATE KEYSPACE IF NOT EXISTS station_information 
WITH replication = {
  'class' : 'SimpleStrategy',
  'replication_factor' : 1
};'''


session.execute(qry)


qry = '''
CREATE TABLE IF NOT EXISTS station_information.saturated_stations (
station_id int, 
num_bikes_available int , 
num_docks_available int , 
is_installed boolean , 
is_renting boolean,
is_returning boolean, 
)
'''

session.execute(qry)

# Define the schema of the JSON data
schema = StructType([
    StructField("station_id", StringType(), True),
    StructField("num_bikes_available", IntegerType(), True),
    StructField("num_docks_available", IntegerType(), True),
    StructField("is_installed", IntegerType(), True),
    StructField("is_renting", IntegerType(), True),
    StructField("is_returning", IntegerType(), True),
    StructField("last_reported", IntegerType(), True)
    # ,StructField("vehicle_types_available", StringType(), True)
])

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Spark Structured Streaming from Kafka") \
    .getOrCreate()

# Set log level to OFF
spark.sparkContext.setLogLevel("OFF")

# Read stream from Kafka
station = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "station_information") \
    .option("startingOffsets", "latest") \
    .load() \
    .selectExpr("CAST(value AS STRING)")

# Parse the JSON data and select the desired attribute
parsed_station = station.select(from_json(col("value"), schema).alias("data")).select("data.*")

# Filter the stream to select rows where num_docks_available is less than 2
station_available_docks_stream = parsed_station.filter(col("num_docks_available") < 2)

# Print the schema of the resulting DataFrame
station_available_docks_stream.printSchema()

# Write the query to Cassandra
def writeToCassandra(df, epoch_id):
    df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode('update') \
        .options(table="saturated_stations", keyspace="station_information") \
        .save()

# Start the query to write the results to Cassandra
query = station_available_docks_stream.writeStream \
    .outputMode("append") \
    .foreachBatch(writeToCassandra) \
    .start()

# Wait for the termination signal
query.awaitTermination()
