from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType, StructType, StructField, StringType, IntegerType
import time
import subprocess

def rename_hdfs_file(hdfs_path):
    subprocess.run(["hdfs", "dfs", "-rm", f"{hdfs_path}/_SUCCESS"], check=True)  # Supprime _SUCCESS
    subprocess.run([
        "hdfs", "dfs", "-mv",
        f"{hdfs_path}/part-00000-*",  # Part-00000 avec identifiant aléatoire
        f"{hdfs_path}/../GT2.csv"  # Nouveau nom de fichier
    ], check=True)
    subprocess.run(["hdfs", "dfs", "-rmdir", "projet/GT_doc.csv"], check=True)
    print(f"Le fichier a été renommé en 'final_output.csv' dans le répertoire HDFS : {hdfs_path}")

def process_partition(rows):
    rows = list(rows)  # Convertir les lignes en une liste modifiable
    initial_rows = rows.copy()

    for i in range(len(rows)):
        if rows[i][0] is None or rows[i][2] is None or rows[i][4] is None or rows[i][6] is None:  # Vérifie les valeurs manquantes
            prev_lavg_temp, next_lavg_temp = None, None
            prev_max_temp, next_max_temp = None, None
            prev_min_temp, next_min_temp = None, None
            prev_lo_avg_temp, next_lo_avg_temp = None, None

            # Rechercher les valeurs précédentes valides
            for j in range(i - 1, -1, -1):
                check, verif1, verif2, verif3, verif4 = 0, False, False, False, False
                if initial_rows[j][0] is not None and not verif1:
                    prev_lavg_temp = initial_rows[j][0]
                    check += 1
                    verif1 = True
                if initial_rows[j][2] is not None and not verif2:
                    prev_max_temp = initial_rows[j][2]
                    check += 1
                    verif2 = True
                if initial_rows[j][4] is not None and not verif3:
                    prev_min_temp = initial_rows[j][4]
                    check += 1
                    verif3 = True
                if initial_rows[j][6] is not None and not verif4:
                    prev_lo_avg_temp = initial_rows[j][6]
                    check += 1
                    verif4 = True
                if check >= 4:
                    break

            # Rechercher les valeurs suivantes valides
            for j in range(i + 1, len(rows)):
                check, verif1, verif2, verif3, verif4 = 0, False, False, False, False
                if initial_rows[j][0] is not None and not verif1:
                    next_lavg_temp = initial_rows[j][0]
                    check += 1
                    verif1 = True
                if initial_rows[j][2] is not None and not verif2:
                    next_max_temp = initial_rows[j][2]
                    check += 1
                    verif2 = True
                if initial_rows[j][4] is not None and not verif3:
                    next_min_temp = initial_rows[j][4]
                    check += 1
                    verif3 = True
                if initial_rows[j][6] is not None and not verif4:
                    next_lo_avg_temp = initial_rows[j][6]
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

            rows[i] = (
                float(avg_lavg_temp) if avg_lavg_temp is not None else None,
                rows[i][1],
                float(avg_max_temp) if avg_max_temp is not None else None,
                rows[i][3],
                float(avg_min_temp) if avg_min_temp is not None else None,
                rows[i][5],
                float(avg_lo_avg_temp) if avg_lo_avg_temp is not None else None,
                rows[i][7], # l and o uncertainty
                rows[i][8], # year
                rows[i][9], # month
                rows[i][10] # day
            )

    return rows

def fill_missing_values(file_path, output_path):
    spark = SparkSession.builder \
        .appName("Handle Missing Values") \
        .getOrCreate()

    df = spark.read.csv(file_path, header=True, inferSchema=True)

    df = df.withColumn("LandAverageTemperature", col("LandAverageTemperature").cast(DoubleType()))
    df = df.withColumn("LandMaxTemperature", col("LandMaxTemperature").cast(DoubleType()))
    df = df.withColumn("LandMinTemperature", col("LandMinTemperature").cast(DoubleType()))
    df = df.withColumn("LandAndOceanAverageTemperature", col("LandAndOceanAverageTemperature").cast(DoubleType()))

    original_rdd = df.rdd
    print(original_rdd.take(10))
    filled_rdd = original_rdd.mapPartitions(lambda partition: process_partition(partition))

    schema = StructType([
        StructField("LandAverageTemperature", DoubleType(), True),
        StructField("LandAverageTemperatureUncertainty", DoubleType(), True),
        StructField("LandMaxTemperature", DoubleType(), True),
        StructField("LandMaxTemperatureUncertainty", DoubleType(), True),
        StructField("LandMinTemperature", DoubleType(), True),
        StructField("LandMinTemperatureUncertainty", DoubleType(), True),
        StructField("LandAndOceanAverageTemperature", DoubleType(), True),
        StructField("LandAndOceanAverageTemperatureUncertainty", DoubleType(), True),
        StructField("year", IntegerType(), True),
        StructField("month", IntegerType(), True),
        StructField("day", IntegerType(), True)
    ])

    filled_df = spark.createDataFrame(filled_rdd, schema=schema)
    filled_df.coalesce(1).write.option("header", True).mode("overwrite").csv(output_path)

    time.sleep(1)
    rename_hdfs_file(output_path)

if __name__ == "__main__":
    input_csv_path = "hdfs:///user/root/projet/GT.csv"
    output_csv_path = "hdfs:///user/root/projet/GT_doc.csv"
    fill_missing_values(input_csv_path, output_csv_path)