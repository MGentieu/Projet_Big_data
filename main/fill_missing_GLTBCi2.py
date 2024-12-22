import subprocess
from pyspark.sql.window import Window
from change_latitude import *

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

    df = normalise_latitude_longitude(df)
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