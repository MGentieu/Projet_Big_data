from pyspark.sql import SparkSession
import matplotlib.pyplot as plt

def calculate_and_plot_average_temperature_by_country(input_csv_path):
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

    # Convertir les résultats en Pandas DataFrame pour le diagramme
    pandas_df = avg_temp_by_country.toPandas()

    # Trier les données par température moyenne décroissante
    pandas_df = pandas_df.sort_values(by="AverageTemperature", ascending=False)

    # Sauvegarder les résultats dans HDFS
    output_path = "/user/root/projet/AverageTemperatureByCountry"
    avg_temp_by_country.write.csv(output_path, header=True, mode="overwrite")
    print(f"Les résultats ont été sauvegardés dans HDFS à : {output_path}")

    # Création du diagramme en barres trié
    plt.figure(figsize=(27, 8))
    plt.bar(pandas_df["Country"], pandas_df["AverageTemperature"], color='skyblue')
    plt.xlabel("Pays", fontsize=12)
    plt.ylabel("Température Moyenne (°C)", fontsize=12)
    plt.title("Température Moyenne par Pays (triée par ordre décroissant)", fontsize=16)
    plt.xticks(rotation=45, fontsize=8)  # Rotation des labels pour les pays
    plt.tight_layout()

    # Sauvegarder le graphique
    output_image = "TemperatureMoyenneParPays_Tri.png"
    plt.savefig(output_image)
    print(f"Le graphique trié a été sauvegardé sous le nom : {output_image}")

    # Afficher le graphique
    plt.show()

    # Fermer la session Spark
    spark.stop()

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage : spark-submit TemperatureMoyennePaysDiagramme.py <input_csv_path>")
        sys.exit(1)

    input_csv_path = sys.argv[1]

    # Appeler la fonction pour calculer et afficher les températures moyennes
    calculate_and_plot_average_temperature_by_country(input_csv_path)

