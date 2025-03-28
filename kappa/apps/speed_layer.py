from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, sum, to_timestamp, count, date_format , when
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, BooleanType
import time
from cassandra.cluster import Cluster

# Configuration constants
IP_CASSANDRA_NODE = ['cassandra1','cassandra2']
KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
TOPIC_STATION_STATUS = 'station_status'
TOPIC_STATION_INFORMATION = 'station_information'
TOPIC_BIKE_STATUS = 'free_bike_status'
KEYSPACE_NAME = 'velozef'
CHECKPOINT_LOCATION = "tmp/checkpoints"
PARQUET_CHECKPOINT = "parquet/checkpoint"
STATION_STATUS_SAVE_PATH = "parquet/station_status"

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
    StructField("last_reported", IntegerType(), False),
    StructField("is_reserved", BooleanType(),False),
    StructField("is_disabled", BooleanType(),False),
    StructField("current_range_meters" , IntegerType(), False )
])

#### SPARK ####
def create_spark_session():
    return SparkSession.builder \
        .appName("Spark-Kafka-Cassandra") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,com.datastax.spark:spark-cassandra-connector_2.12:3.4.1") \
        .config("spark.cassandra.connection.host", ",".join(IP_CASSANDRA_NODE)) \
        .getOrCreate()


### CASSANDRA SETUP ###
def create_cassandra_tables(session):
    keyspace_query = f'''
    CREATE KEYSPACE IF NOT EXISTS {KEYSPACE_NAME} WITH replication = {{
        'class' : 'SimpleStrategy',
        'replication_factor' : 1
    }};'''
    
    station_status_query = '''
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

    bike_situation_query = f'''
    CREATE TABLE IF NOT EXISTS {KEYSPACE_NAME}.bike_situation (
        timestamp TIMESTAMP,
        bike_id TEXT PRIMARY KEY,
        station_id TEXT,
        situation TEXT
    );

    '''

    most_charged_bikes_query = f'''
    CREATE TABLE IF NOT EXISTS {KEYSPACE_NAME}.most_charged_bikes(
        timestamp TIMESTAMP,
        bike_id TEXT PRIMARY KEY,
        station_id TEXT,
        name TEXT
    );

    '''

    session.execute(keyspace_query)
    session.execute(station_status_query)
    session.execute(station_information_query)
    session.execute(bike_status_query)
    session.execute(bike_situation_query)
    session.execute(most_charged_bikes_query)


### READING KAFKA STREAM ###
def read_kafka_stream(spark, topic):
    return spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()


### TRANSFORM STREAM TO DF ###
def stream_to_df(stream, schema):
    return stream.selectExpr("CAST(value AS STRING)") \
                 .select(from_json(col("value"), schema).alias("data")) \
                 .select("data.*")


### PROCESSING STEPS ###
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


def process_reserved_and_disabled_bikes(df_bike_status) :
    """Traitement des vélos réservés et non fonctionnels"""
    # Définition d'une column timestamp 
    df_bike_status = df_bike_status.withColumn("timestamp", to_timestamp(col("last_reported")))

    # 
    df_bike_stituation = df_bike_status.\
                            withWatermark("timestamp" , "120 seconds").\
                            filter((col("is_reserved") == True) | (col("is_disabled")== True )).\
                            withColumn("situation",
                            when((col("is_reserved") == True) & (col("is_disabled") == True), "BOTH")
                            .when(col("is_reserved") == True, "RESERVED")
                            .when(col("is_disabled") == True, "DISABLED")
                            .otherwise(None) 
                            )\
                            .drop("last_reported", "is_reserved", "is_disabled", "current_range_meters")
    
    return df_bike_stituation
    
def process_most_charged_bikes(df_bike_status, df_capacity):
    """List the top 3 charged bikes and their stations"""
    df_top_3_charged_bikes = df_bike_status.join(df_capacity, "station_id") \
        .withColumn("timestamp", to_timestamp(col("last_reported"))) \
        .withWatermark("timestamp", "120 seconds") \
        .drop("is_reserved", "is_disabled", "capacity", "last_reported" )\
    
    return df_top_3_charged_bikes




### WRITING BATCHES TO CASSANDRA ###
def write_to_cassandra_most_charged_bikes(write_df, epochId) : 
    write_df = write_df.sort(col("current_range_meters").desc()).limit(3).drop("current_range_meters")
    write_df.write\
            .format("org.apache.spark.sql.cassandra")\
            .mode("overwrite")\
            .option("confirm.truncate", "true") \
            .options(table="most_charged_bikes" , keyspace=KEYSPACE_NAME)\
            .save()


def write_to_cassandra_bike_situation(writeDF,epochId) :
    """Write the situation of a bike to cassandra"""
    writeDF.write\
            .format("org.apache.spark.sql.cassandra")\
            .mode("overwrite")\
            .option("confirm.truncate", "true") \
            .options(table="bike_situation", keyspace=KEYSPACE_NAME)\
            .save()

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
    


### SAVING BATCHES TO LOCAL IN PARQUETS ###
def log_success(batch_df, batch_id):
    # Sauvegarde du batch en Parquet
    batch_df.write \
        .format("parquet") \
        .partitionBy("day","hour") \
        .mode("append") \
        .save(STATION_STATUS_SAVE_PATH)
    
    # Message de confirmation
    print(f"✅ Batch {batch_id} sauvegardé avec succès à {datetime.now()}")  # Log dans la console



### WRITING STREAMS TO CASSANDRA ###
def write_abandoned_bikes(abandoned_bike_df):

    abandoned_bike_df.writeStream \
        .foreachBatch(write_to_cassandra_bike) \
        .option("checkpointLocation", f"{CHECKPOINT_LOCATION}/abandoned_bikes") \
        .outputMode("update") \
        .option("spark.cassandra.connection.keep_alive_ms", "60000") \
        .option("spark.cassandra.output.batch.size.rows", "50") \
        .trigger(processingTime="60 seconds") \
        .start()
   

def write_station_status(station_status_df):

    station_status_df.writeStream \
        .foreachBatch(write_to_cassandra_station) \
        .option("checkpointLocation", f"{CHECKPOINT_LOCATION}/station_status") \
        .outputMode("update") \
        .option("spark.cassandra.connection.keep_alive_ms", "60000") \
        .option("spark.cassandra.output.batch.size.rows", "50") \
        .trigger(processingTime="60 seconds") \
        .start()

def write_reserved_and_disabled(disabled_and_reserved_df) : 
    disabled_and_reserved_df.writeStream \
                            .foreachBatch(write_to_cassandra_bike_situation)\
                            .option("checkpointLocation", f"{CHECKPOINT_LOCATION}/bike_situation") \
                            .outputMode("update") \
                            .option("spark.cassandra.connection.keep_alive_ms", "60000") \
                            .trigger(processingTime="60 seconds") \
                            .start()
                            

def write_most_charged_bikes(most_charged_bikes_df) :
    most_charged_bikes_df.writeStream\
                         .foreachBatch(write_to_cassandra_most_charged_bikes)\
                         .option("checkpointLocation", f"{CHECKPOINT_LOCATION}/most_charged_bikes") \
                         .outputMode("update") \
                         .option("spark.cassandra.connection.keep_alive_ms", "60000") \
                         .trigger(processingTime="60 seconds") \
                         .start()



### LAUNCH MAIN PROGRAMM ###
def main():
    # Init Spark
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("OFF")
    # Init Cassandra
    cluster = Cluster(IP_CASSANDRA_NODE)
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
    disabled_and_reserved_df = process_reserved_and_disabled_bikes(df_bike_status)
    most_charged_bikes_df = process_most_charged_bikes(df_bike_status ,df_capacity )

    # Sauvegarde du batch station_status en parquet
    df_station_status \
        .withWatermark("timestamp", "30 minutes") \
        .withColumn("day", date_format(col("timestamp"), "dd")) \
        .withColumn("hour", date_format(col("timestamp"), "HH")) \
        .writeStream \
        .foreachBatch(log_success) \
        .option("path", STATION_STATUS_SAVE_PATH) \
        .option("checkpointLocation", PARQUET_CHECKPOINT) \
        .trigger(processingTime="60 minutes") \
        .start() 
        

    # Create and start threads
    write_abandoned_bikes(abandoned_df)
    write_station_status(windowed_df)
    write_reserved_and_disabled(disabled_and_reserved_df)
    write_most_charged_bikes(most_charged_bikes_df)
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()