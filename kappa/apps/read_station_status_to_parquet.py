from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import split

def main():
    # Schéma des données Kafka
    stationSchema = StructType([
        StructField("station_id", StringType(), False),
        StructField("num_bikes_available", IntegerType(), False),
        StructField("num_docks_available", IntegerType(), False),
        StructField("last_reported", IntegerType(), False)
    ])

    # Initialiser une session Spark
    spark = SparkSession.builder \
        .appName("Spark-Kafka-Partitioned-Parquet") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # Lire le flux Kafka
    station_status = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "station_status") \
        .option("startingOffsets", "latest") \
        .load()

    # Parser les données Kafka
    def parse_data_from_kafka_message(sdf, schema):
        assert sdf.isStreaming == True, "DataFrame doesn't receive streaming data"
        col = split(sdf['value'], ',')
        for idx, field in enumerate(schema):
            sdf = sdf.withColumn(field.name, col.getItem(idx).cast(field.dataType))
        return sdf.select([field.name for field in schema])

    kafka_df = parse_data_from_kafka_message(station_status, stationSchema)
    kafka_df.printSchema()

    # Écrire les données dans un fichier Parquet partitionné par "station_id"
    query = kafka_df.writeStream \
        .format("parquet") \
        .outputMode("append") \
        .option("path", "file:///C:/Users/Matheo/Documents/ARCHBIG/Github/Architecture_Kappa/kappa/hdfs") \
        .option("checkpointLocation", "file:///C:/Users/Matheo/Documents/ARCHBIG/Github/Architecture_Kappa/kappa/checkpoint") \
        .partitionBy("station_id") \
        .start()

    # Démarrer le streaming
    query.awaitTermination()

if __name__ == "__main__":
    main()