import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split
from pyspark.sql.types import IntegerType
import subprocess


def rename_hdfs_file(hdfs_path, title):

    subprocess.run(["hdfs", "dfs", "-rm", f"{hdfs_path}/_SUCCESS"], check=True)  # Supprime _SUCCESS
    subprocess.run([
        "hdfs", "dfs", "-mv",
        f"{hdfs_path}/part-00000-*",  # Part-00000 avec identifiant aléatoire
        f"{hdfs_path}/../{title}.csv"  # Nouveau nom de fichier
    ], check=True)
    print(f"Le fichier a été renommé en 'GLTBC_final.csv' dans le répertoire HDFS : {hdfs_path}")


def process_csv_files(csv_files, output_dir):
    # Créer le dossier de sortie si nécessaire
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Initialiser une session Spark
    spark = SparkSession.builder \
        .appName("Process CSV Files") \
        .getOrCreate()

    for file_path in csv_files:
        # Charger le fichier CSV dans un DataFrame Spark
        df = spark.read.csv(file_path, header=True, inferSchema=True)

        # Vérifier si la colonne "dt" existe
        if "dt" in df.columns:
            # Séparer la colonne "dt" en trois colonnes : year, month, day
            df = df.withColumn("year", split(col("dt"), "-").getItem(0).cast(IntegerType()))
            df = df.withColumn("month", split(col("dt"), "-").getItem(1).cast(IntegerType()))
            df = df.withColumn("day", split(col("dt"), "-").getItem(2).cast(IntegerType()))

            # Supprimer la colonne "dt"
            df = df.drop("dt")

            # Générer un nom de fichier de sortie basé sur le fichier d'entrée
            file_name = os.path.basename(file_path).replace(".csv", "_processed.csv")
            output_path = os.path.join(output_dir, file_name)

            # Sauvegarder le DataFrame modifié directement dans le dossier output_dir
            df.coalesce(1).write.csv(output_path, header=True, mode="overwrite")

            # Déplacer le fichier CSV hors des répertoires créés par Spark
            for root, _, files in os.walk(output_path):
                for file in files:
                    if file.endswith(".csv"):
                        os.rename(os.path.join(root, file), os.path.join(output_dir, file_name))
            # Nettoyer les répertoires inutiles créés par Spark
            for root, dirs, files in os.walk(output_path):
                for directory in dirs:
                    os.rmdir(os.path.join(root, directory))

            print(f"Fichier traité sauvegardé : {output_path}")
        else:
            print(f"La colonne 'dt' est absente dans le fichier : {file_path}")

if __name__ == "__main__":
    # Liste des fichiers CSV à traiter
    csv_files = [
        "hdfs:///user/root/projet/GlobalLandTemperaturesByCountry.csv",
        "hdfs:///user/root/projet/GlobalLandTemperaturesByMajorCity.csv",
        "hdfs:///user/root/projet/GlobalTemperatures.csv",
        "hdfs:///user/root/projet/GlobalLandTemperaturesByState.csv",
        "hdfs:///user/root/projet/GlobalLandTemperaturesByCity.csv"
    ]
    titles=["GLTBCo","GLTBMC","GL","GLTBS","GLTBCi"]

    # Répertoire de sortie pour les fichiers traités
    output_dir = "/user/root/PBD"

    # Traiter les fichiers CSV
    process_csv_files(csv_files, output_dir)

    for i in range (len(csv_files)):
        rename_hdfs_file(csv_files[i], titles[i])
    subprocess.run(["hdfs", "dfs", "-rmdir", f"{output_dir}/Global*"], check=True)
