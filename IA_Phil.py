import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split
from pyspark.sql.types import IntegerType
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
import numpy as np


# Fonction pour traiter la colonne 'dt'
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


spark = SparkSession.builder \
    .appName("TemperatureEvolution") \
    .getOrCreate()

df = spark.read.csv("hdfs:///user/root/projet/GlobalTemperatures.csv", header=True, inferSchema=True)
df.show()

df = df.select("dt", "LandAverageTemperature")

# Étape 4 : Traitement de la colonne 'dt' pour extraire l'année, le mois et le jour
df = process_date_column(df)

# Étape 5 : Calcul de la température moyenne par année
df_yearly_avg = df.groupBy("year").avg("LandAverageTemperature").withColumnRenamed("avg(LandAverageTemperature)",
                                                                                   "YearlyAverageTemperature")

# Conversion en Pandas pour la visualisation et la régression linéaire
df_pd = df_yearly_avg.toPandas()
df_pd = df_pd.sort_values(by="year")

# Préparation des données pour la régression linéaire
X = df_pd['year'].values.reshape(-1, 1)  # Année (variable indépendante)
y = df_pd['YearlyAverageTemperature'].values  # Température moyenne annuelle (variable dépendante)

# Création et entraînement du modèle de régression linéaire
model = LinearRegression()
model.fit(X, y)

# Génération des prédictions
y_pred = model.predict(X)

# Étape 6 : Visualisation avec Matplotlib
plt.figure(figsize=(12, 6))
plt.plot(df_pd['year'], df_pd['YearlyAverageTemperature'], color='blue', label='Température moyenne annuelle')
plt.plot(df_pd['year'], y_pred, color='red', label='Tendance (Régression Linéaire)', linestyle='--')

plt.title("Évolution de la température moyenne annuelle avec tendance", fontsize=16)
plt.xlabel("Année", fontsize=12)
plt.ylabel("Température moyenne (°C)", fontsize=12)
plt.grid(True, linestyle='--', alpha=0.6)
plt.legend(fontsize=12)
plt.tight_layout()

plt.savefig("temperature_evolution_regression.png")
print("Graphique sauvegardé sous le nom 'temperature_evolution_regression.png'.")

spark.stop()
