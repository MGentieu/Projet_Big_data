from pyspark.sql import SparkSession
import subprocess

def rename_hdfs_file(hdfs_path):

    subprocess.run(["hdfs", "dfs", "-rm", f"{hdfs_path}/_SUCCESS"], check=True)  # Supprime _SUCCESS
    subprocess.run([
        "hdfs", "dfs", "-mv",
        f"{hdfs_path}/part-00000-*",  # Part-00000 avec identifiant aléatoire
        f"{hdfs_path}/../AvgTempByCo.csv"  # Nouveau nom de fichier
    ], check=True)
    subprocess.run(["hdfs", "dfs", "-rmdir", "projet/AvgTempByCo_doc.csv"], check=True)
    print(f"Le fichier a été renommé en 'final_output.csv' dans le répertoire HDFS : {hdfs_path}")

def calculate_average_temperature_by_country(input_csv_path, output_path):
    # Créer une session Spark
    spark = SparkSession.builder.appName("Temperature Moyenne Pays").getOrCreate()

    # Lire le fichier CSV depuis HDFS
    try:
        df = spark.read.csv(input_csv_path, header=True, inferSchema=True)
    except Exception as e:
        print(f"Erreur lors de la lecture du fichier CSV : {e}")
        spark.stop()
        return

    # Vérifier les colonnes nécessaires
    if "Country" not in df.columns or "AverageTemperature" not in df.columns:
        print("Les colonnes 'Country' et/ou 'AverageTemperature' sont manquantes dans le fichier CSV.")
        spark.stop()
        return

    # Filtrer les lignes où AverageTemperature n'est pas null
    df = df.filter(df["AverageTemperature"].isNotNull())

    # Calculer la température moyenne par pays
    avg_temp_by_country = df.groupBy("Country").avg("AverageTemperature")
    avg_temp_by_country = avg_temp_by_country.withColumnRenamed("avg(AverageTemperature)", "AverageTemperature")

    # Afficher les résultats
    avg_temp_by_country.show(truncate=False)

    # Sauvegarder les résultats dans HDFS
    avg_temp_by_country.write.csv(output_path, header=True, mode="overwrite")
    avg_temp_by_country.coalesce(1).write.option("header", True).mode("overwrite").csv(output_path)
    print(f"Les résultats ont été sauvegardés dans HDFS à : {output_path}")
    rename_hdfs_file(output_path)
    # Fermer la session Spark
    spark.stop()

if __name__ == "__main__":
    import sys
    #if len(sys.argv) != 2:
    #    print("Usage : spark-submit TemperatureMoyennePays.py <input_csv_path>")
    #    sys.exit(1)

    #input_csv_path = sys.argv[1]
    input_csv_path = "hdfs:///user/root/PBD/GLTBC.csv"
    output_path = "hdfs:///user/root/projet/AvgTempByCo_doc.csv"
    # Appeler la fonction pour calculer et afficher les températures moyennes
    calculate_average_temperature_by_country(input_csv_path, output_path)

