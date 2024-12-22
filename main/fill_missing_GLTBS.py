from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType
import time
import subprocess


def rename_hdfs_file(hdfs_path):

    subprocess.run(["hdfs", "dfs", "-rm", f"{hdfs_path}/_SUCCESS"], check=True)  # Supprime _SUCCESS
    subprocess.run([
        "hdfs", "dfs", "-mv",
        f"{hdfs_path}/part-00000-*",  # Part-00000 avec identifiant aléatoire
        f"{hdfs_path}/../GLTBS2.csv"  # Nouveau nom de fichier
    ], check=True)
    subprocess.run(["hdfs", "dfs", "-rmdir", "projet/GLTBS_doc.csv"], check=True)
    print(f"Le fichier a été renommé en 'final_output.csv' dans le répertoire HDFS : {hdfs_path}")

def process_partition(rows):
    # On utilise une copie de référence des lignes pour ne pas affecter les calculs suivants
    rows = list(rows)
    initial_rows = rows.copy()

    for i in range(len(rows)):
        if rows[i][0] is None or rows[i][1] is None:  # Vérifie les valeurs manquantes
            # Récupérer les précédentes et suivantes valeurs valides dans la copie initiale
            prev_temp=None
            prev_uncert=None
            next_temp=None
            next_uncert=None

            # Rechercher les valeurs précédentes valides
            for j in range(i - 1, -1, -1):
                if initial_rows[j][0] is not None and initial_rows[j][1] is not None:
                    prev_temp, prev_uncert = initial_rows[j][0], initial_rows[j][1]
                    break

            # Rechercher les valeurs suivantes valides
            for j in range(i + 1, len(rows)):
                if initial_rows[j][0] is not None and initial_rows[j][1] is not None:
                    next_temp, next_uncert = initial_rows[j][0], initial_rows[j][1]
                    break

            # Calculer les moyennes uniquement si les deux valeurs sont valides
            avg_temp = prev_temp if next_temp is None else (
                next_temp if prev_temp is None else (prev_temp + next_temp) / 2
                    )
            avg_uncert = prev_uncert if next_uncert is None else (
                next_uncert if prev_uncert is None else (prev_uncert + next_uncert) / 2
            )

            # Remplir les valeurs manquantes avec ces moyennes
            rows[i] = (avg_temp, avg_uncert, rows[i][2], rows[i][3], rows[i][4],rows[i][5], rows[i][6])
    return rows

def fill_missing_values(file_path, output_path):
    spark = SparkSession.builder \
    .appName("Handle Missing Values") \
    .getOrCreate()

    df = spark.read.csv(file_path, header=True, inferSchema=True)

    df = df.withColumn("AverageTemperature", col("AverageTemperature").cast(DoubleType()))
    df = df.withColumn("AverageTemperatureUncertainty", col("AverageTemperatureUncertainty").cast(DoubleType()))
    original_rdd = df.rdd

    filled_rdd = original_rdd.mapPartitions(lambda partition: process_partition(partition))
    print("Exemple de lignes dans le RDD traité :")
    print(filled_rdd.take(5))

    filled_df = spark.createDataFrame(filled_rdd, schema=["AverageTemperature", "AverageTemperatureUncertainty","State", "Country","year",
    "month","day"])
    filled_df.show(15)

    filled_df.coalesce(1).write.option("header", True).mode("overwrite").csv(output_path)

    time.sleep(1)

    rename_hdfs_file(output_path)


if __name__ == "__main__":
    input_csv_path = "hdfs:///user/root/projet/GLTBS.csv"
    output_csv_path = "hdfs:///user/root/projet/GLTBS_doc.csv"
    fill_missing_values(input_csv_path, output_csv_path)