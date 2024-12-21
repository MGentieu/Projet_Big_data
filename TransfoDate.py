import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split
from pyspark.sql.types import IntegerType
import subprocess

def rename_hdfs_file(hdfs_path, title):
    try:
        subprocess.run(["hdfs", "dfs", "-test", "-e", f"{hdfs_path}/_SUCCESS"], check=True)
        subprocess.run(["hdfs", "dfs", "-rm", f"{hdfs_path}/_SUCCESS"], check=True)
        file_to_rename = subprocess.check_output(
            ["hdfs", "dfs", "-ls", hdfs_path], universal_newlines=True
        ).split("\n")

        # Identifier le fichier généré
        for line in file_to_rename:
            if "part-00000" in line:
                file_path = line.split()[-1]
                subprocess.run(["hdfs", "dfs", "-mv", file_path, f"{hdfs_path}/../{title}.csv"], check=True)
                print(f"Fichier renommé : {file_path} -> {title}.csv")
                break
    except Exception as e:
        print(f"Erreur dans le renommage : {e}")


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
            file_name = os.path.basename(file_path).replace(".csv", ".csv")
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
    new_paths = [
        "hdfs:///user/root/projet/GlobalLTBC.csv",
        "hdfs:///user/root/projet/GlobalLTBMC.csv",
        "hdfs:///user/root/projet/GlobalT.csv",
        "hdfs:///user/root/projet/GlobalLTBS.csv",
        "hdfs:///user/root/projet/GlovalLTBCi.csv"
    ]
    titles=["GLTBCo","GLTBMC","GL","GLTBS","GLTBCi"]

    output_dir = "hdfs:///user/root/projet"

    # Répertoire de sortie pour les fichiers traités

    # Traiter les fichiers CSV
    process_csv_files(csv_files, output_dir)

    for i in range (len(new_paths)):
        rename_hdfs_file(new_paths[i], titles[i])
    subprocess.run(["hdfs", "dfs", "-rmdir", f"{output_dir}/Global*"], check=True)
