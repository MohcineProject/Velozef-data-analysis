from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, sum, to_timestamp, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from cassandra.cluster import Cluster

IP_CASSANDRA_NODE = "172.22.0.4"

def main():
    # Définir les schémas
    stationstatusSchema = StructType([
        StructField("station_id", StringType(), False),
        StructField("num_bikes_available", IntegerType(), False),
        StructField("num_docks_available", IntegerType(), False),
        StructField("last_reported", IntegerType(), False)  # Utiliser last_reported comme timestamp
    ])

    stationinformationSchema = StructType([
        StructField("station_id", StringType(), False),
        StructField("capacity", IntegerType(), False),
        StructField("name", StringType(), False),
    ])

    # Initialiser Spark
    spark = SparkSession.builder \
        .appName("Spark-Kafka-Cassandra") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,com.datastax.spark:spark-cassandra-connector_2.12:3.4.1") \
        .config("spark.cassandra.connection.host", IP_CASSANDRA_NODE) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # Définir les topics Kafka
    topic_station_status = 'station_status'
    topic_station_information = 'station_information'
    
    ################ SESSION CASSANDRA #################

    clstr = Cluster([str(IP_CASSANDRA_NODE)])
    session = clstr.connect()
    keyspace_qry = ''' CREATE KEYSPACE IF NOT EXISTS velozef WITH replication = {
    'class' : 'SimpleStrategy',
    'replication_factor' : 1
    }  ;'''
    
    # Create a new cassandra table
    station_status_qry = '''
    CREATE TABLE IF NOT EXISTS velozef.station_status_windowed (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    empty_count INT,
    full_count INT,
    saturation_count INT,
    PRIMARY KEY ((window_start,window_end))
    );'''

    station_information_qry = '''
    CREATE TABLE IF NOT EXISTS velozef.station_information (
    station_id TEXT PRIMARY KEY,
    capacity INT,
    name TEXT);'''

    session.execute(keyspace_qry)
    session.execute(station_status_qry)
    session.execute(station_information_qry)

    ################# LECTURE FLUX KAFKA #################
    def read_kafka_stream(topic):
        return spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:29092") \
            .option("subscribe", topic) \
            .option("startingOffsets", "earliest") \
            .load()
    # Transformer les flux en DataFrame structuré
    def stream_to_df(stream, schema):
        return stream.selectExpr("CAST(value AS STRING)") \
                    .select(from_json(col("value"), schema).alias("data")) \
                    .select("data.*")
    
    # Lire les flux Kafka
    station_status_stream = read_kafka_stream(topic_station_status)
    station_information_stream = read_kafka_stream(topic_station_information)

    # Transformer les flux en DataFrames
    df_station_status = stream_to_df(station_status_stream, stationstatusSchema)
    df_station_information = stream_to_df(station_information_stream, stationinformationSchema)

    # Convertir last_reported UNIX en timestamp
    df_station_status = df_station_status.withColumn("timestamp", to_timestamp(col("last_reported")))

    # Écrire les informations de capacité dans Cassandra 
    """
    Il était impossible de réaliser une jointure entre les deux tables lues sur kafka et 
    d'écrire le résultat du traitement (qui utilisait <capacity> de station information et les attributs de station_status) en mode update, d'où l'idée d'écrire la table station_information qui ne contient que des informations statiques de toute manière.
    """
    df_station_information.writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", "velozef") \
        .option("table", "station_information") \
        .option("checkpointLocation", "/tmp/checkpoints/station_information") \
        .outputMode("append") \
        .start()

    # Lire les informations de capacité depuis Cassandra
    df_capacity = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", "velozef") \
        .option("table", "station_information") \
        .load()

    ############## TRAITEMENTS #################
    #1 | Compter le nombre de stations vides, remplies, proche de saturation

    # Joindre les DataFrames
    df1 = df_station_status.join(df_capacity, "station_id")

    # Calculer les colonnes supplémentaires
    required_columns = ["num_bikes_available", "capacity"]
    if all(column in df1.columns for column in required_columns):
        df2 = df1.withColumn("is_full", (col("num_bikes_available") / col("capacity") == 1).cast("int")) \
                .withColumn("saturation", (col("num_bikes_available") >= col("capacity") - 3).cast("int")) \
                .withColumn("is_empty", (col("num_bikes_available") == 0).cast("int"))
    else:
        raise ValueError(f"Les colonnes requises {required_columns} ne sont pas toutes présentes dans le DataFrame.")

    # Appliquer une fenêtre de 60 secondes basée sur last_reported
    windowed_df = df2.groupBy(
        window(col("timestamp"), "60 seconds")  # Fenêtre de 60 secondes                    
    ).agg(
        sum(col("is_empty")).alias("empty_count"),     
        sum(col("is_full")).alias("full_count"),   
        sum(col("saturation")).alias("saturation_count")  
    )

    # Extraire window_start et window_end
    windowed_df = windowed_df.withColumn("window_start", col("window.start")) \
                            .withColumn("window_end", col("window.end")) \
                            .drop("window")  

    
    ################## ECRITURE DANS CASSANDRA #####################
    windowed_df.printSchema()
    def writeToCassandra(writeDF, epochId):
        writeDF.write \
            .format("org.apache.spark.sql.cassandra")\
            .mode('append')\
            .options(table="station_status_windowed", keyspace="velozef")\
            .save()

    windowed_df.writeStream \
        .option("spark.cassandra.connection.host", IP_CASSANDRA_NODE)\
        .foreachBatch(writeToCassandra) \
        .outputMode("update") \
        .start()\
        .awaitTermination()

if __name__ == "__main__":
    main()