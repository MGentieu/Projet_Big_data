from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType
from pyspark.sql import Row
import sys
import time
import subprocess


def rename_hdfs_file(hdfs_path):
    subprocess.run(["hdfs", "dfs", "-rm", f"{hdfs_path}/_SUCCESS"], check=True)  # Supprime _SUCCESS
    subprocess.run([
        "hdfs", "dfs", "-mv",
        f"{hdfs_path}/part-00000-*",  # Part-00000 avec identifiant aléatoire
        f"{hdfs_path}/../GT.csv"  # Nouveau nom de fichier
    ], check=True)
    subprocess.run(["hdfs", "dfs", "-rmdir", "projet/GT_doc.csv"], check=True)
    print(f"Le fichier a été renommé en 'final_output.csv' dans le répertoire HDFS : {hdfs_path}")


def process_partition(rows):
    # Convertir les lignes en une liste modifiable
    rows = list(rows)
    initial_rows = rows.copy()

    for i in range(len(rows)):
        if rows[i][1] is None or rows[i][2] is None:  # Vérifie les valeurs manquantes
            # Récupérer les précédentes et suivantes valeurs valides dans la copie initiale
            prev_lavg_temp = None
            next_lavg_temp = None
            prev_max_temp = None
            next_max_temp = None
            prev_min_temp = None
            next_min_temp = None
            prev_lo_avg_temp = None
            next_lo_avg_temp = None

            # Rechercher les valeurs précédentes valides
            for j in range(i - 1, -1, -1):
                # Booléans permettant de ne sélectionner que le premier nombre trouvé. On quitte quand check = 4 (tous les nombres trouvés)
                verif1 = False
                verif2 = False
                verif3 = False
                verif4 = False
                check = 0
                if initial_rows[j][1] is not None and not verif1:
                    prev_lavg_temp = initial_rows[j][1]
                    check += 1
                    verif1 = True

                if initial_rows[j][3] is not None and not verif2:
                    prev_max_temp = initial_rows[j][3]
                    check += 1
                    verif2 = True

                if initial_rows[j][5] is not None and not verif3:
                    prev_min_temp = initial_rows[j][5]
                    check += 1
                    verif3 = True

                if initial_rows[j][7] is not None and not verif4:
                    prev_lo_avg_temp = initial_rows[j][7]
                    check += 1
                    verif4 = True

                if check >= 4:
                    break

            # Rechercher les valeurs suivantes valides
            for j in range(i + 1, len(rows)):
                # Booléans permettant de ne sélectionner que le premier nombre trouvé. On quitte quand check = 4 (tous les nombres trouvés)
                verif1 = False
                verif2 = False
                verif3 = False
                verif4 = False
                check = 0

                if initial_rows[j][1] is not None and not verif1:
                    next_lavg_temp = initial_rows[j][1]
                    check += 1
                    verif1 = True

                if initial_rows[j][3] is not None and not verif2:
                    next_max_temp = initial_rows[j][3]
                    check += 1
                    verif2 = True

                if initial_rows[j][5] is not None and not verif3:
                    next_min_temp = initial_rows[j][5]
                    check += 1
                    verif3 = True

                if initial_rows[j][7] is not None and not verif4:
                    next_lo_avg_temp = initial_rows[j][7]
                    check += 1
                    verif4 = True

                if check >= 4:
                    break

            # Calculer les moyennes uniquement si les deux valeurs sont valides
            avg_lavg_temp = prev_lavg_temp if next_lavg_temp is None else (
                next_lavg_temp if prev_lavg_temp is None else (prev_lavg_temp + next_lavg_temp) / 2
            )
            avg_max_temp = prev_max_temp if next_max_temp is None else (
                next_max_temp if prev_max_temp is None else (prev_max_temp + next_max_temp) / 2
            )
            avg_min_temp = prev_min_temp if next_min_temp is None else (
                next_min_temp if prev_min_temp is None else (prev_min_temp + next_min_temp) / 2
            )
            avg_lo_avg_temp = prev_lo_avg_temp if next_lo_avg_temp is None else (
                next_lo_avg_temp if prev_lo_avg_temp is None else (prev_lo_avg_temp + next_lo_avg_temp) / 2
            )

            # Remplir les valeurs manquantes avec ces moyennes
            rows[i] = (
                rows[i][0],  # dt
                float(avg_lavg_temp) if avg_lavg_temp is not None else None,
                rows[i][2],
                float(avg_max_temp) if avg_max_temp is not None else None,
                rows[i][4],
                float(avg_min_temp) if avg_min_temp is not None else None,
                rows[i][6],
                float(avg_lo_avg_temp) if avg_lo_avg_temp is not None else None,
                rows[i][8]
            )
"""
            rows[i] = (
                rows[i][0],  # dt
                avg_lavg_temp,  # LandAverageTemperature
                rows[i][2],  # LandAverageTemperatureUncertainty
                avg_max_temp,  # LandMaxTemperature
                rows[i][4],  # LandMaxTemperatureUncertainty
                avg_min_temp,  # LandMinTemperature
                rows[i][6],  # LandMinTemperatureUncertainty
                avg_lo_avg_temp,  # LandAndOceanAverageTemperature,
                rows[i][8]  # LandAndOceanAverageTemperatureUncertainty
            )
"""
    return rows


def fill_missing_values(file_path, output_path):
    # Initialiser une session Spark
    spark = SparkSession.builder \
        .appName("Handle Missing Values") \
        .getOrCreate()

    # Charger le fichier CSV dans un DataFrame Spark
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    print(df.columns)
    # Conversion des colonnes AverageTemperature et AverageTemperatureUncertainty en Double
    df = df.withColumn("LandAverageTemperature", col("LandAverageTemperature").cast(DoubleType()))
    df = df.withColumn("LandMaxTemperature", col("LandMaxTemperature").cast(DoubleType()))
    df = df.withColumn("LandMinTemperature", col("LandMinTemperature").cast(DoubleType()))
    df = df.withColumn("LandAndOceanAverageTemperature", col("LandAndOceanAverageTemperature").cast(DoubleType()))

    # Convertir le DataFrame en RDD pour un traitement partitionné
    original_rdd = df.rdd

    # Appliquer le traitement partition par partition
    filled_rdd = original_rdd.mapPartitions(lambda partition: process_partition(partition))

    print("Exemple de lignes dans le RDD traité :")
    print(filled_rdd.take(5))

    # Convertir l'RDD corrigé en DataFrame

    schema = StructType([
        StructField("dt", StringType(), True),
        StructField("LandAverageTemperature", DoubleType(), True),
        StructField("LandAverageTemperatureUncertainty", DoubleType(), True),
        StructField("LandMaxTemperature", DoubleType(), True),
        StructField("LandMaxTemperatureUncertainty", DoubleType(), True),
        StructField("LandMinTemperature", DoubleType(), True),
        StructField("LandMinTemperatureUncertainty", DoubleType(), True),
        StructField("LandAndOceanAverageTemperature", DoubleType(), True),
        StructField("LandAndOceanAverageTemperatureUncertainty", DoubleType(), True),
    ])

    filled_df = spark.createDataFrame(filled_rdd, schema=schema)
"""
    filled_df = spark.createDataFrame(filled_rdd, schema=[
        "dt", "LandAverageTemperature", "LandAverageTemperatureUncertainty",
        "LandMaxTemperature", "LandMaxTemperatureUncertainty",
        "LandMinTemperature", "LandMinTemperatureUncertainty",
        "LandAndOceanAverageTemperature", "LandAndOceanAverageTemperatureUncertainty"
    ])"""
    filled_df.show(15)

    # Regrouper les partitions en une seule
    filled_df.coalesce(1).write.option("header", True).mode("overwrite").csv(output_path)

    time.sleep(1)
    rename_hdfs_file(output_path)


if __name__ == "__main__":
    input_csv_path = "hdfs:///user/root/projet/GlobalTemperatures.csv"
    output_csv_path = "hdfs:///user/root/projet/GT_doc.csv"

    # Appeler la fonction pour traiter le fichier
    fill_missing_values(input_csv_path, output_csv_path)

