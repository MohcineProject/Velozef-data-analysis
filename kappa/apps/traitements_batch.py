import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from datetime import datetime, timedelta
from functools import reduce

#### PARAMS ####
PARQUET_PATH = "parquet/station_status"
CURRENT_TIME = datetime.now()
HOURS_TO_LOAD = 24  # Nombre d'heures à charger

# Initialisation de Spark
spark = SparkSession.builder \
        .appName("ReadLast24Hours") \
        .getOrCreate()

def get_existing_partitions(base_path, hours_to_load):
    """Retourne uniquement les partitions qui existent réellement"""
    existing_paths = []
    for i in range(1, hours_to_load + 1):
        time = CURRENT_TIME - timedelta(hours=i)
        partition_path = f"day={time.strftime('%d')}/hour={time.strftime('%H')}"
        
        # Vérifie si le chemin de partition existe
        full_path = os.path.join(base_path, partition_path)
        try:
            # Liste les sous-partitions existantes
            minute_partitions = [f.path for f in os.scandir(full_path) if f.is_dir()]
            if minute_partitions:
                existing_paths.extend(minute_partitions)
            else:
                print(f"⚠️ Aucune donnée pour {partition_path}")
        except FileNotFoundError:
            print(f"⏭️ Partition non trouvée: {partition_path}")
            continue
    
    return existing_paths

# 1. Récupération des partitions existantes
existing_partitions = get_existing_partitions(PARQUET_PATH, HOURS_TO_LOAD)

if not existing_partitions:
    print("❌ Aucune partition trouvée pour les 24 dernières heures")
    spark.stop()
    exit()

# 2. Lecture des partitions existantes
try:
    dfs = []
    for path in existing_partitions:
        try:
            df_part = spark.read.parquet(path)
            # df_part = df_part.withColumn("data_source", lit(path))  # Ajoute le chemin source
            dfs.append(df_part)
        except Exception as e:
            print(f"⚠️ Erreur lecture {path}: {str(e)}")
            continue
    
    if not dfs:
        print("❌ Aucune donnée valide à charger")
        spark.stop()
        exit()
    
    # Union de tous les DataFrames
    df = reduce(lambda a, b: a.union(b), dfs)
    
    # Statistiques
    print("\n✅ Chargement réussi")
    print(f"📂Partitions chargées: {len(dfs)}")
    print(f"Nombre total de lignes: {df.count()}")
    print("\n Schéma:")
    df.printSchema()
    print("\n Aperçu des données:")
    df.show(5, truncate=False)
    
except Exception as e:
    print(f"❌ Erreur majeure: {str(e)}")
finally:
    spark.stop()