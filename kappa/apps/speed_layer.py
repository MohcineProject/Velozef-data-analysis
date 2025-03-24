from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, sum, to_timestamp, count, date_format
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, BooleanType
import time
from cassandra.cluster import Cluster

# Configuration constants
IP_CASSANDRA_NODE = "172.22.0.4"
KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
TOPIC_STATION_STATUS = 'station_status'
TOPIC_STATION_INFORMATION = 'station_information'
TOPIC_BIKE_STATUS = 'free_bike_status'
KEYSPACE_NAME = 'velozef'
CHECKPOINT_LOCATION = "tmp/checkpoints"
PARQUET_CHECKPOINT = "parquet/checkpoint"
STATION_STATUS_SAVE_PATH = "parquet/station_status"
SAVE_LOGS_PATH = "parquet/save_logs.save_logs.txt"

#### SCHEMAS ####
stationstatusSchema = StructType([
    StructField("station_id", StringType(), False),
    StructField("num_bikes_available", IntegerType(), False),
    StructField("num_docks_available", IntegerType(), False),
    StructField("last_reported", IntegerType(), False),
])

stationinformationSchema = StructType([
    StructField("station_id", StringType(), False),
    StructField("capacity", IntegerType(), False),
    StructField("name", StringType(), False),
])

bikestatusSchema = StructType([
    StructField("bike_id", StringType(), False),
    StructField("station_id", StringType(), False),
    StructField("last_reported", IntegerType(), False)
])

#### SPARK ####
def create_spark_session():
    return SparkSession.builder \
        .appName("Spark-Kafka-Cassandra") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,com.datastax.spark:spark-cassandra-connector_2.12:3.4.1") \
        .config("spark.cassandra.connection.host", IP_CASSANDRA_NODE) \
        .getOrCreate()

def create_cassandra_tables(session):
    keyspace_query = f'''
    CREATE KEYSPACE IF NOT EXISTS {KEYSPACE_NAME} WITH replication = {{
        'class' : 'SimpleStrategy',
        'replication_factor' : 1
    }};'''
    
    station_status_query = f'''
    CREATE TABLE IF NOT EXISTS velozef.station_status_windowed (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    empty_count INT,
    full_count INT,
    saturation_count INT,
    total_docks_available INT,
    total_bikes_available INT,
    PRIMARY KEY ((window_start), window_end) 
    ) WITH CLUSTERING ORDER BY (window_end DESC);  
    '''

    station_information_query = f'''
    CREATE TABLE IF NOT EXISTS {KEYSPACE_NAME}.station_information (
        station_id TEXT PRIMARY KEY,
        capacity INT,
        name TEXT
    );'''

    bike_status_query = f'''
    CREATE TABLE IF NOT EXISTS {KEYSPACE_NAME}.bike_status (
        window_start TIMESTAMP,
        window_end TIMESTAMP,
        abandoned_bikes_count INT,
        PRIMARY KEY ((window_start, window_end))
    );'''

    session.execute(keyspace_query)
    session.execute(station_status_query)
    session.execute(station_information_query)
    session.execute(bike_status_query)

def read_kafka_stream(spark, topic):
    return spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

def stream_to_df(stream, schema):
    return stream.selectExpr("CAST(value AS STRING)") \
                 .select(from_json(col("value"), schema).alias("data")) \
                 .select("data.*")

def process_station_status(df_station_status, df_capacity):
    """Traitements : 
    - 1 | Calcul du nombre de stations pleines/vides/proche de saturation par fenêtre de 60 secondes
    - 2 | Calcul du nombre de docks et vélos disponibles au total sur le réseau vélozef
    """
    df_station_status = df_station_status.withColumn("timestamp", to_timestamp(col("last_reported")))
    df_joined = df_station_status.join(df_capacity, "station_id")
    
    # 1 | Calcul du nombre de stations pleines/vides/proche de saturation par fenêtre de 60 secondes
    required_columns = ["num_bikes_available", "capacity"]
    if all(column in df_joined.columns for column in required_columns):
        df_processed = df_joined.withColumn("is_full", (col("num_bikes_available") / col("capacity") == 1).cast("int")) \
                               .withColumn("saturation", (col("num_bikes_available") >= col("capacity") - 3).cast("int")) \
                               .withColumn("is_empty", (col("num_bikes_available") == 0).cast("int"))
    else:
        raise ValueError(f"Les colonnes requises {required_columns} ne sont pas toutes présentes dans le DataFrame.")

    windowed_df = df_processed \
                    .withWatermark("timestamp", "10 minutes") \
                    .groupBy(window(col("timestamp"), "60 seconds")) \
                    .agg(sum(col("num_bikes_available")).alias("total_bikes_available"),
                        sum(col('num_docks_available')).alias("total_docks_available"),
                        sum(col("is_empty")).alias("empty_count"),
                        sum(col("is_full")).alias("full_count"),
                        sum(col("saturation")).alias("saturation_count")) \
                    .withColumn("window_start", col("window.start")) \
                    .withColumn("window_end", col("window.end")) \
                    .drop("window")

    return windowed_df

def process_abandoned_bikes(df_bike_status):
    """Traitement des vélos abandonnés."""
    df_bike_status = df_bike_status.withColumn("timestamp", to_timestamp(col("last_reported")))
    
    # Filtrer les vélos abandonnés (station_id vide)
    df_abandoned = df_bike_status.filter(col("station_id") == "")
    
    # Compter les vélos abandonnés par fenêtre de 60 secondes
    abandoned_df = df_abandoned \
                            .withWatermark("timestamp", "10 minutes") \
                            .groupBy(window(col("timestamp"), "60 seconds")) \
                            .agg(count("*").alias("abandoned_bikes_count")) \
                            .withColumn("window_start", col("window.start")) \
                            .withColumn("window_end", col("window.end")) \
                            .drop("window")
    
    return abandoned_df

def write_to_cassandra_station(writeDF, epochId):
    """Write station status data to Cassandra."""
    writeDF.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode('append') \
        .options(table="station_status_windowed", keyspace=KEYSPACE_NAME) \
        .save()

def write_to_cassandra_bike(writeDF, epochId):
    """Write abandoned bikes data to Cassandra."""
    writeDF.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode('append') \
        .options(table="bike_status", keyspace=KEYSPACE_NAME) \
        .save()

def log_success(batch_df, batch_id):
    # Sauvegarde du batch en Parquet
    batch_df.write \
        .format("parquet") \
        .partitionBy("day","hour") \
        .mode("append") \
        .save(STATION_STATUS_SAVE_PATH)
    
    # Message de confirmation
    print(f"✅ Batch {batch_id} sauvegardé avec succès à {datetime.now()}")  # Log dans la console
    # (Optionnel) Écriture dans un fichier log
    with open(SAVE_LOGS_PATH, "a") as f:
        f.write(f"{datetime.now()} - Batch {batch_id} sauvegardé\n")

def write_abandoned_bikes(abandoned_bike_df):

    abandoned_bike_df.writeStream \
        .foreachBatch(write_to_cassandra_bike) \
        .option("spark.cassandra.connection.host", IP_CASSANDRA_NODE) \
        .option("checkpointLocation", f"{CHECKPOINT_LOCATION}/abandoned_bikes") \
        .outputMode("update") \
        .option("spark.cassandra.connection.keep_alive_ms", "60000") \
        .option("spark.cassandra.output.batch.size.rows", "50") \
        .trigger(processingTime="60 seconds") \
        .start()
   

def write_station_status(station_status_df):

    station_status_df.writeStream \
        .foreachBatch(write_to_cassandra_station) \
        .option("spark.cassandra.connection.host", IP_CASSANDRA_NODE) \
        .option("checkpointLocation", f"{CHECKPOINT_LOCATION}/station_status") \
        .outputMode("update") \
        .option("spark.cassandra.connection.keep_alive_ms", "60000") \
        .option("spark.cassandra.output.batch.size.rows", "50") \
        .trigger(processingTime="60 seconds") \
        .start()
        

def main():
    # Init Spark
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("OFF")
    # Init Cassandra
    cluster = Cluster([IP_CASSANDRA_NODE])
    session = cluster.connect()
    create_cassandra_tables(session)

    # Read Kafka
    station_status_stream = read_kafka_stream(spark, TOPIC_STATION_STATUS)
    station_information_stream = read_kafka_stream(spark, TOPIC_STATION_INFORMATION)
    bike_status_stream = read_kafka_stream(spark, TOPIC_BIKE_STATUS)

    # Stream -> df
    df_station_status = stream_to_df(station_status_stream, stationstatusSchema).withColumn("timestamp", to_timestamp(col("last_reported")))

    df_station_information = stream_to_df(station_information_stream, stationinformationSchema)
    df_bike_status = stream_to_df(bike_status_stream, bikestatusSchema)

    # Stocke les informations de stations_information dans une table Cassandra
    print("Stockage des informations de station_information dans Cassandra...")
    
    df_station_information.writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", KEYSPACE_NAME) \
        .option("table", "station_information") \
        .option("checkpointLocation", "tmp/checkpoints/station_information") \
        .outputMode("append") \
        .start()
    
    time.sleep(2)

    df_capacity = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", KEYSPACE_NAME) \
        .option("table", "station_information") \
        .load()

    ##### TRAITEMENTS #####
    windowed_df = process_station_status(df_station_status, df_capacity)
    abandoned_df = process_abandoned_bikes(df_bike_status) 

    # Sauvegarde du batch station_status en parquet
    df_station_status \
        .withWatermark("timestamp", "10 minutes") \
        .withColumn("day", date_format(col("timestamp"), "dd")) \
        .withColumn("hour", date_format(col("timestamp"), "HH")) \
        .writeStream \
        .foreachBatch(log_success) \
        .option("path", STATION_STATUS_SAVE_PATH) \
        .option("checkpointLocation", PARQUET_CHECKPOINT) \
        .trigger(processingTime="10 minutes") \
        .start() 
        

    # Create and start threads
    write_abandoned_bikes(abandoned_df)
    write_station_status(windowed_df)
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()