from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import from_json, col
import time
import os

def main():
    try:
        # Schéma des données Kafka (JSON)
        stationSchema = StructType([
            StructField("station_id", StringType(), False),
            StructField("num_bikes_available", IntegerType(), False),
            StructField("num_docks_available", IntegerType(), False),
            StructField("last_reported", IntegerType(), False)
        ])

        # Initialiser une session Spark
        spark = SparkSession.builder \
            .appName("Spark-Kafka-JSON-Parquet") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
            .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")

        # Vérifier que Kafka est accessible
        try:
            # Lire le flux Kafka
            station_status = spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "kafka:29092") \
                .option("subscribe", "station_status") \
                .option("startingOffsets", "earliest") \
                .load()
        except Exception as e:
            print(f"Erreur lors de la connexion à Kafka : {e}")
            raise

        # Convertir les données binaires Kafka en chaîne de caractères
        df = station_status.selectExpr("CAST(value AS STRING)")

        # Afficher les données brutes Kafka dans la console
        raw_query = df.writeStream \
            .format("console") \
            .start()

        # Parser les données JSON
        try:
            kafka_df = df.select(from_json(col("value"), stationSchema).alias("data")).select("data.*")
        except Exception as e:
            print(f"Erreur lors du parsing des données JSON : {e}")
            raise

        # Afficher les données parsées dans la console
        parsed_query = kafka_df.writeStream \
            .format("console") \
            .start()

        # Vérifier que le répertoire de destination existe
        output_path = "file:///C:/Users/Matheo/Documents/ARCHBIG/Github/Architecture_Kappa/kappa/hdfs"
        checkpoint_path = "file:///C:/Users/Matheo/Documents/ARCHBIG/Github/Architecture_Kappa/kappa/checkpoint"

        if not os.path.exists(output_path.replace("file://", "")):
            print(f"Le répertoire de destination n'existe pas : {output_path}")
            os.makedirs(output_path.replace("file://", ""))

        if not os.path.exists(checkpoint_path.replace("file://", "")):
            print(f"Le répertoire de checkpoint n'existe pas : {checkpoint_path}")
            os.makedirs(checkpoint_path.replace("file://", ""))

        # Écrire les données dans un fichier Parquet partitionné par "station_id"
        parquet_query = kafka_df.writeStream \
            .format("parquet") \
            .outputMode("append") \
            .option("path", output_path) \
            .option("checkpointLocation", checkpoint_path) \
            .partitionBy("station_id") \
            .start()

        # Attendre 30 secondes
        time.sleep(30)

        # Arrêter manuellement les requêtes
        raw_query.stop()
        parsed_query.stop()
        parquet_query.stop()

        # Vérifier que des fichiers Parquet ont été créés
        if os.path.exists(output_path.replace("file://", "")):
            print("Vérification des fichiers Parquet dans le répertoire de destination...")
            for root, dirs, files in os.walk(output_path.replace("file://", "")):
                for file in files:
                    print(f"Fichier trouvé : {os.path.join(root, file)}")
        else:
            print("Aucun fichier Parquet n'a été créé dans le répertoire de destination.")

    except Exception as e:
        print(f"Une erreur s'est produite : {e}")
    finally:
        # Arrêter la session Spark
        spark.stop()

if __name__ == "__main__":
    main()