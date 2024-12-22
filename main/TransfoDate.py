import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split
from pyspark.sql.types import IntegerType
import subprocess

def rename_hdfs_file(hdfs_path, title):
    try:
        subprocess.run(["hdfs", "dfs", "-rm", f"{hdfs_path}/_SUCCESS"], check=True)
        file_to_rename = subprocess.check_output(
            ["hdfs", "dfs", "-ls", hdfs_path], universal_newlines=True
        ).split("\n")

        # Identifier le fichier généré
        for line in file_to_rename:
            if "part-00000" in line:
                file_path = line.split()[-1]
                new_path = f"{hdfs_path}/../{title}.csv"
                subprocess.run(["hdfs", "dfs", "-mv", file_path, new_path], check=True)
                print(f"Fichier renommé : {file_path} -> {new_path}")
                return new_path
        subprocess.run(["hdfs", "dfs", "-rmdir", f"{hdfs_path}.csv"], check=True)
    except Exception as e:
        print(f"Erreur dans le renommage : {e}")

def process_csv_files(csv_files, output_dir, titles):
    spark = SparkSession.builder \
        .appName("Process CSV Files") \
        .getOrCreate()

    for i, file_path in enumerate(csv_files):
        df = spark.read.csv(file_path, header=True, inferSchema=True)

        if "dt" in df.columns:
            df = df.withColumn("year", split(col("dt"), "-").getItem(0).cast(IntegerType()))
            df = df.withColumn("month", split(col("dt"), "-").getItem(1).cast(IntegerType()))
            df = df.withColumn("day", split(col("dt"), "-").getItem(2).cast(IntegerType()))

            df = df.drop("dt")

        # Sauvegarder directement dans HDFS
        output_path = f"{output_dir}/{titles[i]}"
        df.coalesce(1).write.csv(output_path, header=True, mode="overwrite")
        print(f"Fichier écrit : {output_path}")

        # Renommer les fichiers HDFS
        rename_hdfs_file(output_path, titles[i])

if __name__ == "__main__":
    csv_files = [
        "hdfs:///user/root/projet/GlobalLandTemperaturesByCountry.csv",
        "hdfs:///user/root/projet/GlobalLandTemperaturesByMajorCity.csv",
        "hdfs:///user/root/projet/GlobalTemperatures.csv",
        "hdfs:///user/root/projet/GlobalLandTemperaturesByState.csv",
        "hdfs:///user/root/projet/GlobalLandTemperaturesByCity.csv"
    ]
    titles = ["GLTBCo", "GLTBMC", "GT", "GLTBS", "GLTBCi"]
    output_dir = "hdfs:///user/root/projet"

    process_csv_files(csv_files, output_dir, titles)

    subprocess.run(["hdfs", "dfs", "-rmdir","projet/GT"], check=True)
    subprocess.run(["hdfs", "dfs", "-rmdir","projet/GLTB*"], check=True)