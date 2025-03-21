"""
Demo Spark Structured Streaming + Apache Kafka + Cassandra
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,LongType,IntegerType,FloatType,StringType
from pyspark.sql.functions import split,sum,from_json,col


import sys

def main():

    capteurSchema = StructType([
                StructField("date",StringType(),False),
                StructField("numero",IntegerType(),False),
                StructField("capteur",StringType(),False),
                StructField("valeur",FloatType(),False)
            ])

    spark = SparkSession.builder \
        .appName("Spark-Kafka-Cassandra") \
        .config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .getOrCreate()


    spark.sparkContext.setLogLevel("ERROR")

    topic = sys.argv[1]

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", topic) \
        .option("delimeter",",") \
        .option("startingOffsets", "latest") \
        .load()

    df1 = df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"),capteurSchema).alias("data")).select("data.*")
    df1.printSchema()

    # Traitements ###
    #TODO 
    #################

    # Write to a cassandra table 

    def writeToCassandra(writeDF, epochId):
        writeDF.write \
            .format("org.apache.spark.sql.cassandra")\
            .mode('append')\
            .options(table="transactions1", keyspace="demo")\
            .save()

    df1.writeStream \
        .option("spark.cassandra.connection.host","cassandra1:9042")\
        .foreachBatch(writeToCassandra) \
        .outputMode("update") \
        .start()\
        .awaitTermination()

if __name__ == "__main__":
    main()














