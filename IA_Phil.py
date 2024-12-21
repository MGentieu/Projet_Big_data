from pyspark.sql.functions import lag, col
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import SparkSession
import pandas as pd
import matplotlib.pyplot as plt

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split
from pyspark.sql.types import IntegerType


def process_date_column(df):
    """
    Traite la colonne 'dt' d'un DataFrame Spark, la divise en trois colonnes : 'year', 'month', 'day'.
    La colonne 'dt' est ensuite supprimée.

    Args:
    df (DataFrame): DataFrame Spark contenant la colonne 'dt'.

    Returns:
    DataFrame: DataFrame modifié avec les nouvelles colonnes 'year', 'month', 'day'.
    """
    if "dt" in df.columns:
        # Séparer la colonne 'dt' en trois colonnes : 'year', 'month', 'day'
        df = df.withColumn("year", split(col("dt"), "-").getItem(0).cast(IntegerType()))
        df = df.withColumn("month", split(col("dt"), "-").getItem(1).cast(IntegerType()))
        df = df.withColumn("day", split(col("dt"), "-").getItem(2).cast(IntegerType()))

        # Supprimer la colonne 'dt'
        df = df.drop("dt")
    else:
        print("La colonne 'dt' est absente dans le DataFrame.")

    return df




# Étape 1 : Initialisation de la session Spark
spark = SparkSession.builder \
    .appName("TemperatureEvolution") \
    .getOrCreate()

# Étape 2 : Chargement des données
df = spark.read.csv("hdfs:///user/root/projet/GlobalTemperatures.csv", header=True, inferSchema=True)

# Afficher un aperçu des données
df.show()

# Étape 3 : Sélection des colonnes pertinentes
df = df.select("dt", "LandAverageTemperature")

# Étape 4 : Traitement de la colonne 'dt' pour extraire l'année, le mois et le jour
df = process_date_column(df)

# Étape 5 : Calcul de la température moyenne par année
df_yearly_avg = df.groupBy("year").avg("LandAverageTemperature").withColumnRenamed("avg(LandAverageTemperature)", "YearlyAverageTemperature")

# Conversion en Pandas pour la visualisation
df_pd = df_yearly_avg.toPandas()

# Trier les données par année
df_pd = df_pd.sort_values(by="year")

# Étape 6 : Visualisation avec Matplotlib
plt.figure(figsize=(12, 6))
plt.plot(df_pd['year'], df_pd['YearlyAverageTemperature'], color='blue', label='Température moyenne annuelle')

# Ajout des détails au graphique
plt.title("Évolution de la température moyenne annuelle", fontsize=16)
plt.xlabel("Année", fontsize=12)
plt.ylabel("Température moyenne (°C)", fontsize=12)
plt.grid(True, linestyle='--', alpha=0.6)
plt.legend(fontsize=12)
plt.tight_layout()

# Sauvegarder le graphique en tant qu'image PNG
plt.savefig("temperature_evolution_yearly.png")
print("Graphique sauvegardé sous le nom 'temperature_evolution_yearly.png'.")

# Arrêt de la session Spark
spark.stop()






def IA():
    # Étape 1 : Initialisation de Spark
    spark = SparkSession.builder \
        .appName("PredictionTemp") \
        .getOrCreate()

    # Étape 2 : Chargement des données
    # Remplacez 'path_to_file.csv' par le chemin de votre fichier de données
    data_path = "path_to_file.csv"
    df = spark.read.csv(data_path, header=True, inferSchema=True)
    # Vérifiez les premières lignes du dataset
    df.show()

    # Étape 3 : Préparation des données
    # Assurez-vous que la colonne de température est bien identifiée (ex. : "LandAverageTemperature")
    # Vous pouvez ajuster ce nom en fonction de votre dataset
    window = Window.orderBy("dt")  # 'dt' est la colonne des dates, ajustez si nécessaire

    # Création des colonnes décalées (lags)
    df = df.withColumn("Temp_Lag1", lag("LandAverageTemperature", 1).over(window))
    df = df.withColumn("Temp_Lag2", lag("LandAverageTemperature", 2).over(window))
    df = df.withColumn("Temp_Lag3", lag("LandAverageTemperature", 3).over(window))
    # Supprimez les lignes contenant des valeurs nulles
    df = df.na.drop()

    # Créez un vecteur de caractéristiques pour l'entraînement
    assembler = VectorAssembler(inputCols=["Temp_Lag1", "Temp_Lag2", "Temp_Lag3"], outputCol="features")
    data = assembler.transform(df).select("features", "LandAverageTemperature")

    # Étape 4 : Division des données en ensembles d'entraînement et de test
    train_data, test_data = data.randomSplit([0.8, 0.2], seed=123)

    # Étape 5 : Entraînement du modèle
    lr = LinearRegression(featuresCol="features", labelCol="LandAverageTemperature")
    model = lr.fit(train_data)

    # Étape 6 : Prédictions
    predictions = model.transform(test_data)

    # Affichage des résultats
    predictions.select("features", "LandAverageTemperature", "prediction").show()

    # Étape 7 : Évaluation du modèle
    evaluator = RegressionEvaluator(labelCol="LandAverageTemperature", predictionCol="prediction", metricName="rmse")
    rmse = evaluator.evaluate(predictions)
    print(f"RMSE (Root Mean Squared Error): {rmse}")

    # Étape 8 : Prédictions futures
    # Pour prédire les températures futures, utilisez les dernières valeurs de votre dataset comme entrée
    latest_data = df.orderBy(col("dt").desc()).limit(1)  # Obtenez la dernière ligne
    latest_features = assembler.transform(latest_data).select("features")
    future_prediction = model.transform(latest_features)

    print("Prédiction future :")
    future_prediction.select("features", "prediction").show()

    # Arrêtez la session Spark
    spark.stop()
