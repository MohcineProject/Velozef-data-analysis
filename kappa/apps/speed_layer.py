"""
Demo Spark Structured Streaming + Apache Kafka + Cassandra
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,LongType,IntegerType,FloatType,StringType
from pyspark.sql.functions import split,sum,from_json,col
from cassandra.cluster import Cluster

import sys

def main():
    
    # TODO remplacer schema selon topic écouté
    stationSchema = StructType([
        StructField("station_id", StringType(), False),
        StructField("num_bikes_available", IntegerType(), False),
        StructField("num_docks_available", IntegerType(), False),
        StructField("last_reported", IntegerType(), False)
    ])

    spark = SparkSession.builder \
        .appName("Spark-Kafka-Cassandra") \
        .config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .getOrCreate()


    spark.sparkContext.setLogLevel("ERROR")

    topic = 'station_status'

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", topic) \
        .option("delimeter",",") \
        .option("startingOffsets", "latest") \
        .load()

    df1 = df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"),stationSchema).alias("data")).select("data.*")
    df1.printSchema()

    # Traitements ###
    #TODO 
    #################
    clstr=Cluster(['172.22.0.3'])
    session=clstr.connect()


    # Write to a cassandra table 
    qry=''' CREATE KEYSPACE IF NOT EXISTS velozef WITH replication = {
    'class' : 'SimpleStrategy',
    'replication_factor' : 1
     }  ;'''
	
    session.execute(qry) 

    # Create a new cassandra table
    qry='''CREATE TABLE IF NOT EXISTS demo.station_status (
    station_id text,
    num_bikes_available int,
    num_docks_available int,
    last_reported int,
    PRIMARY KEY (station_id)
);'''
    session.execute(qry)

    def writeToCassandra(writeDF, epochId):
        writeDF.write \
            .format("org.apache.spark.sql.cassandra")\
            .mode('append')\
            .options(table="station_status", keyspace="velozef")\
            .save()

    df1.writeStream \
        .option("spark.cassandra.connection.host","cassandra1:9042")\
        .foreachBatch(writeToCassandra) \
        .outputMode("update") \
        .start()\
        .awaitTermination()

if __name__ == "__main__":
    main()














