from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
import subprocess
import time
from pyspark.sql.window import Window
from pyspark.sql.functions import col, avg, last, first, lit, when, lag, lead
from pyspark.sql.window import Window

def rename_hdfs_file(hdfs_path):
    try:
        subprocess.run(["hdfs", "dfs", "-rm", f"{hdfs_path}/_SUCCESS"], check=True)
        subprocess.run([
            "hdfs", "dfs", "-mv",
            f"{hdfs_path}/part-00000-*",
            f"{hdfs_path}/../GLTBCi2.csv"
        ], check=True)
        subprocess.run(["hdfs", "dfs", "-rmdir", "projet/GLTBCi_doc.csv"], check=True)
        print(f"Le fichier a été renommé avec succès dans HDFS : {hdfs_path}")
    except subprocess.CalledProcessError as e:
        print(f"Erreur lors de l'exécution des commandes HDFS : {e}")
"""
def process_partition(rows):
    rows = list(rows)
    for i in range(len(rows)):
        if rows[i][1] is None or rows[i][2] is None:
            prev_temp, next_temp = None, None
            prev_uncert, next_uncert = None, None

            for j in range(i - 1, -1, -1):
                if rows[j][1] is not None:
                    prev_temp, prev_uncert = rows[j][1], rows[j][2]
                    break

            for j in range(i + 1, len(rows)):
                if rows[j][1] is not None:
                    next_temp, next_uncert = rows[j][1], rows[j][2]
                    break

            avg_temp = prev_temp if next_temp is None else (next_temp if prev_temp is None else (prev_temp + next_temp) / 2)
            avg_uncert = prev_uncert if next_uncert is None else (next_uncert if prev_uncert is None else (prev_uncert + next_uncert) / 2)

            rows[i] = (rows[i][0], avg_temp, avg_uncert, rows[i][3], rows[i][4], rows[i][5], rows[i][6])
    return rows

"""
def fill_missing_values(file_path, output_path):
    spark = SparkSession.builder \
    .appName("Handle Missing Values") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "1g") \
    .config("spark.executor.cores", "2") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.master", "local[2]") \
    .getOrCreate()


    df = spark.read.csv(file_path, header=True, inferSchema=True)

    # Convertir les colonnes à leurs types appropriés
    df = df.withColumn("AverageTemperature", col("AverageTemperature").cast(DoubleType()))
    df = df.withColumn("AverageTemperatureUncertainty", col("AverageTemperatureUncertainty").cast(DoubleType()))

    # Créer une fenêtre ordonnée par colonne temporelle
    window_spec = Window.partitionBy("City").orderBy("year", "month", "day")

    # Remplir les valeurs manquantes par interpolation
    df = df.withColumn("AverageTemperature",
                       when(col("AverageTemperature").isNotNull(), col("AverageTemperature"))
                       .otherwise((lag("AverageTemperature").over(window_spec) + lead("AverageTemperature").over(window_spec)) / 2))

    df = df.withColumn("AverageTemperatureUncertainty",
                       when(col("AverageTemperatureUncertainty").isNotNull(), col("AverageTemperatureUncertainty"))
                       .otherwise((lag("AverageTemperatureUncertainty").over(window_spec) + lead("AverageTemperatureUncertainty").over(window_spec)) / 2))

    # Écrire le fichier final sur HDFS
    df.coalesce(1).write.option("header", True).mode("overwrite").csv(output_path)
    rename_hdfs_file(output_path)
    spark.stop()


if __name__ == "__main__":
    input_csv_path = "hdfs:///user/root/projet/GLTBCi.csv"
    output_csv_path = "hdfs:///user/root/projet/GLTBCi_doc.csv"
    fill_missing_values(input_csv_path, output_csv_path)

