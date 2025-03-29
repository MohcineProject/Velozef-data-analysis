import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, avg, count
from datetime import datetime, timedelta
from functools import reduce
from cassandra.cluster import Cluster


#### PARAMS ####
PARQUET_PATH = "parquet/station_status/day=27"
IP_CASSANDRA = ['cassandra1','cassandra2']
KEYSPACE_NAME = "velozef"

def initialize_spark():
    """Initialise et retourne une session Spark"""
    session = SparkSession.builder \
        .appName("StationBatch") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,com.datastax.spark:spark-cassandra-connector_2.12:3.4.1") \
        .config("spark.cassandra.connection.host", ",".join(IP_CASSANDRA)) \
        .config("spark.cassandra.input.consistency.level", "ONE")\
        .config("spark.cassandra.output.consistency.level", "ONE") \
        .getOrCreate()

    sc = session.sparkContext
    sc.setLogLevel("OFF")

    return session

def create_cassandra_tables(session):
    """Crée les tables nécessaires dans cassandra"""

    keyspace_query = f'''
    CREATE KEYSPACE IF NOT EXISTS {KEYSPACE_NAME} WITH replication = {{
        'class' : 'SimpleStrategy',
        'replication_factor' : 2
    }};'''
    
    station_status_query = f'''
    CREATE TABLE IF NOT EXISTS {KEYSPACE_NAME}.batch_station_status (
        station_id TEXT PRIMARY KEY,
        name TEXT,
        hour_avg_bikes_avail FLOAT,
        hour_avg_docks_avail FLOAT,
        empty_occurrences_24h INT,
        full_occurrences_24h INT,
        computation_timestamp TIMESTAMP
    );
    '''
    session.execute(keyspace_query)
    session.execute(station_status_query)

def get_existing_partitions(base_path):
    """
    Retourne l'ensemble des partitions parquet dans les sous dossiers PARQUET_PATH 
    """
    existing_paths = []
    
    # Vérifie d'abord si le dossier de base existe
    if not os.path.exists(base_path):
        print(f"Le dossier de base {base_path} n'existe pas")
        return existing_paths
    
    # Parcourt tous les sous-dossiers du dossier de base
    for root, dirs, files in os.walk(base_path):
        # On cherche les répertoires qui contiennent des fichiers Parquet
        parquet_files = [f for f in files if f.endswith('.parquet') or f.endswith('.snappy')]
        
        if parquet_files:
            # Si on trouve des fichiers Parquet, on ajoute le chemin
            existing_paths.append(root)
            print(f"✅ Partition trouvée: {root} ({len(parquet_files)} fichiers)")
    
    return existing_paths

def load_data(spark, existing_partitions):
    """Charge les données à partir des partitions existantes"""
    if not existing_partitions:
        print("Aucune partition trouvée")
        return None

    dfs = []
    for path in existing_partitions:
        try:
            df_part = spark.read.parquet(path)
            dfs.append(df_part)
        except Exception as e:
            print(f"Erreur lecture {path}: {str(e)}")
            continue
    
    if not dfs:
        print("❌ Aucune donnée valide à charger")
        return None
    
    # Combine les dataframes en un seul
    return reduce(lambda a, b: a.union(b), dfs)

def compute_and_write_combined_stats(spark, df, current_time=datetime.now()):
    """
    Calcule et combine les statistiques, puis écrit dans Cassandra.
    
    """
    # 1. Calculer toutes les statistiques séparément
    avg_stats = df.groupBy("station_id") \
        .agg(
            avg("num_bikes_available").cast("float").alias("hour_avg_bikes_avail"),
            avg("num_docks_available").cast("float").alias("hour_avg_docks_avail")
        )
    
    empty_stats = df.filter(col("num_bikes_available") == 0) \
        .groupBy("station_id") \
        .agg(count("*").alias("empty_occurrences_24h"))
    
    full_stats = df.filter(col("num_docks_available") == 0) \
        .groupBy("station_id") \
        .agg(count("*").alias("full_occurrences_24h"))
    
    # 2. Combiner les DataFrames avec des jointures externes
    combined_df = avg_stats \
        .join(empty_stats, "station_id", "left") \
        .join(full_stats, "station_id", "left") \
        .fillna(0, subset=["empty_occurrences_24h", "full_occurrences_24h"]) \
        .withColumn("computation_timestamp", lit(current_time))
    
    # join table with cassandra's station_information on station_id key table on velozef.station_information to add column station_name
    combined_df = combined_df.join(
        spark.read.format("org.apache.spark.sql.cassandra")
        .options(table="station_information", keyspace="velozef")
        .load(),
        "station_id",
        "left"
    ).select(
        "station_id",
        "name",
        "hour_avg_bikes_avail",
        "hour_avg_docks_avail",
        "empty_occurrences_24h",
        "full_occurrences_24h",
        "computation_timestamp"
    )


    # 3. Écrire dans Cassandra
    try:
        combined_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(table="batch_station_status", keyspace="velozef") \
            .save()
        
        print("✅ Statistiques combinées écrites avec succès dans Cassandra")
    except Exception as e:
        print(f"❌ Erreur lors de l'écriture dans Cassandra: {str(e)}")
        raise
    
    return combined_df

def display_stats(avg_stats, empty_stats, full_stats):
    """Affiche les statistiques calculées"""
    print("\n=== Statistiques calculées ===")
    print("\nMoyennes sur 24h:")
    avg_stats.show(29, truncate=False)
    
    print("\nStations vides:")
    empty_stats.show(29, truncate=False)
    
    print("\nStations pleines:")
    full_stats.show(29, truncate=False)

def main():
    current_time = datetime.now()
    spark = None
    # init cassandra session
    cluster = Cluster(IP_CASSANDRA)
    session = cluster.connect() 


    try:
        # Initialisation
        spark = initialize_spark()
        
        # Récupération des partitions
        existing_partitions = get_existing_partitions(PARQUET_PATH)
        print(f"\nPartitions existantes: {len(existing_partitions)}")
        
        # Chargement des données
        df = load_data(spark, existing_partitions)
        if df is None:
            return 1
        
        # Affichage des informations de base
        print("\n✅ Chargement réussi")
        print(f"Partitions chargées: {len(existing_partitions)}")
        print(f"Nombre total de lignes: {df.count()}")
        print("\nSchéma:")
        df.printSchema()
        print("\nAperçu des données:")
        df.show(5, truncate=False)

        # Creation des tables sur cassandra
        create_cassandra_tables(session)

        # Calcul des statistiques
        compute_and_write_combined_stats(spark, df, current_time)
        
        return 0
        
    except Exception as e:
        print(f"❌ Erreur lors de l'exécution: {str(e)}", file=sys.stderr)
        return 1
    finally:
        if spark is not None:
            spark.stop()
        cluster.shutdown()

if __name__ == "__main__":
    sys.exit(main())
