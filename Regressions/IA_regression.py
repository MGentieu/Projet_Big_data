from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split
from pyspark.sql.types import IntegerType
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
import numpy as np
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.model_selection import train_test_split


def evaluate_regression_model(model, X_test, y_test):
    """
    Évalue et affiche la performance d'un modèle de régression linéaire.

    Args:
    - model : Le modèle de régression déjà entraîné (par ex. sklearn.linear_model.LinearRegression).
    - X_test : Données de test (indépendantes).
    - y_test : Valeurs réelles correspondantes (cibles).
    """
    # Étape 6 : Visualisation avec Matplotlib (Tendance sur tout l'ensemble de données)
    plt.figure(figsize=(12, 6))
    plt.plot(df_pd['year'], df_pd['YearlyAverageTemperature'], color='blue', label='Température moyenne annuelle')
    plt.plot(df_pd['year'], model.predict(X), color='red', label='Tendance (Régression Linéaire)', linestyle='--')

    plt.title("Évolution de la température moyenne annuelle avec tendance", fontsize=16)
    plt.xlabel("Année", fontsize=12)
    plt.ylabel("Température moyenne (°C)", fontsize=12)
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.legend(fontsize=12)
    plt.tight_layout()

    # Prédictions sur le jeu de test
    y_pred = model.predict(X_test)

    # Calcul du RMSE
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))

    # Calcul du coefficient de détermination R²
    r2 = r2_score(y_test, y_pred)

    # Calcul du pourcentage d'erreur moyenne relative
    relative_errors = np.abs((y_test - y_pred) / y_test) * 100
    mean_relative_error = np.mean(relative_errors)

    # Affichage des résultats
    print("\n--- Évaluation du Modèle ---")
    print(f"RMSE : {rmse:.4f}")
    print(f"R² : {r2:.4f}")
    print(f"Pourcentage d'erreur moyenne relative : {mean_relative_error:.2f} %")


spark = SparkSession.builder \
    .appName("TemperatureEvolution") \
    .getOrCreate()

df = spark.read.csv("hdfs:///user/root/projet/GlobalTemperatures.csv", header=True, inferSchema=True)

# Afficher les premières lignes du dataset
print("\n--- Dataset chargé ---")
df.show(10)

# Filtrer les données avant 1850
df = df.filter(col("year") >= 1850)

# Étape 5 : Calcul de la température moyenne par année
df_yearly_avg = df.groupBy("year").avg("LandAverageTemperature").withColumnRenamed("avg(LandAverageTemperature)",
                                                                                   "YearlyAverageTemperature")
# Conversion en Pandas pour la visualisation et la régression linéaire
df_pd = df_yearly_avg.toPandas()
df_pd = df_pd.sort_values(by="year")

# Préparation des données pour la régression linéaire
X = df_pd['year'].values.reshape(-1, 1)  # Année (variable indépendante)
y = df_pd['YearlyAverageTemperature'].values  # Température moyenne annuelle (variable dépendante)

# Division des données en ensembles d'entraînement (70%) et de test (30%)
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

# Création et entraînement du modèle de régression linéaire
model = LinearRegression()
model.fit(X_train, y_train)
# Génération des prédictions pour l'ensemble d'entraînement et de test
y_pred_train = model.predict(X_train)
y_pred_test = model.predict(X_test)

evaluate_regression_model(model, X, y)

# Sauvegarde du graphique
plt.savefig("temperature_evolution_regression.png")
print("Graphique sauvegardé sous le nom 'temperature_evolution_regression.png'.")

spark.stop()
