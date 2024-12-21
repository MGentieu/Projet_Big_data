from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import seaborn as sns
import os
import subprocess


def calculate_and_plot_average_temperature_by_country(input_csv_path):
    # Créer une session Spark
    spark = SparkSession.builder.appName("Temperature Moyenne Pays").getOrCreate()

    # Lire le fichier CSV depuis HDFS
    try:
        df = spark.read.csv(input_csv_path, header=True, inferSchema=True)
        if df.rdd.isEmpty():
            raise ValueError("Le fichier CSV est vide.")
    except Exception as e:
        print(f"Erreur lors de la lecture du fichier CSV : {e}")
        spark.stop()
        return

    # Vérifier que la colonne AverageTemperature existe
    if "AverageTemperature" not in df.columns:
        print("Erreur : La colonne 'AverageTemperature' est absente du fichier.")
        spark.stop()
        return

    # Convertir les résultats en Pandas DataFrame pour le diagramme
    try:
        pandas_df = df.select("AverageTemperature").dropna().toPandas()
    except Exception as e:
        print(f"Erreur lors de la conversion en Pandas DataFrame : {e}")
        spark.stop()
        return

    # Créer un boxplot des températures moyennes
    plt.figure(figsize=(8, 6))
    sns.boxplot(y=pandas_df["AverageTemperature"], color="skyblue")
    plt.title("Boxplot des Températures Moyennes", fontsize=16)
    plt.ylabel("Température Moyenne (°C)", fontsize=12)
    plt.tight_layout()

    # Sauvegarder le graphique localement
    local_output_image = "Boxplot_Temperature.png"
    try:
        plt.savefig(local_output_image)
        print(f"Le boxplot a été sauvegardé localement sous le nom : {local_output_image}")
    except Exception as e:
        print(f"Erreur lors de la sauvegarde du graphique : {e}")
        spark.stop()
        return

    # Fermer la session Spark
    spark.stop()


if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage : spark-submit TemperatureMoyennePays.py <input_csv_path>")
        sys.exit(1)

    input_csv_path = sys.argv[1]

    # Appeler la fonction pour calculer et afficher les températures moyennes
    calculate_and_plot_average_temperature_by_country(input_csv_path)

