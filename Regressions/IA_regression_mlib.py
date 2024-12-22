from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split
from pyspark.sql.types import IntegerType
from pyspark.mllib.regression import LabeledPoint, LinearRegressionWithSGD
from pyspark.mllib.evaluation import RegressionMetrics
import matplotlib.pyplot as plt


def evaluate_mllib_model(predictions_and_labels):
    """
    Évalue un modèle MLlib en calculant des métriques comme le RMSE et R².
    """
    # Convertir les prédictions en un objet RegressionMetrics
    metrics = RegressionMetrics(predictions_and_labels)

    # Extraire les métriques
    rmse = metrics.rootMeanSquaredError
    r2 = metrics.r2

    print("\n--- Évaluation du Modèle ---")
    print(f"RMSE : {rmse:.4f}")
    print(f"R² : {r2:.4f}")


# Créer une session Spark
spark = SparkSession.builder \
    .appName("TemperatureEvolution_MLlib") \
    .getOrCreate()

# Charger les données
df = spark.read.csv("hdfs:///user/root/projet/GlobalTemperatures.csv", header=True, inferSchema=True)
# Afficher les premières lignes du dataset
print("\n--- Dataset chargé ---")
df.show(10)
df = df.select("dt", "LandAverageTemperature")


# Filtrer les données pour garder uniquement celles après 1850
df = df.filter(col("year") >= 1850)

# Calculer la température moyenne par année
df_yearly_avg = df.groupBy("year").avg("LandAverageTemperature").withColumnRenamed("avg(LandAverageTemperature)",
                                                                                   "YearlyAverageTemperature")

# Convertir les données en RDD pour MLlib
rdd = df_yearly_avg.rdd.map(lambda row: LabeledPoint(row["YearlyAverageTemperature"], [row["year"]]))

# Diviser les données en ensembles d'entraînement et de test (70% / 30%)
train_rdd, test_rdd = rdd.randomSplit([0.7, 0.3], seed=42)

# Créer et entraîner le modèle de régression linéaire avec descente de gradient
model = LinearRegressionWithSGD.train(train_rdd, iterations=100, step=0.0001, intercept=True)

# Générer des prédictions sur l'ensemble de test
predictions_and_labels = test_rdd.map(lambda point: (model.predict(point.features), point.label))

# Évaluer le modèle
evaluate_mllib_model(predictions_and_labels)

# Collecter les données pour la visualisation
year_temperature = df_yearly_avg.sort("year").toPandas()

# Visualiser les résultats avec Matplotlib
plt.figure(figsize=(12, 6))
plt.plot(year_temperature["year"], year_temperature["YearlyAverageTemperature"], label="Température moyenne annuelle", color="blue")
plt.plot(year_temperature["year"], [model.predict([x]) for x in year_temperature["year"]], label="Tendance (MLlib)", color="red", linestyle="--")

plt.title("Évolution de la température moyenne annuelle avec tendance (MLlib)", fontsize=16)
plt.xlabel("Année", fontsize=12)
plt.ylabel("Température moyenne (°C)", fontsize=12)
plt.legend(fontsize=12)
plt.grid(True, linestyle='--', alpha=0.6)
plt.tight_layout()

# Sauvegarder le graphique
plt.savefig("temperature_evolution_mllib.png")
print("Graphique sauvegardé sous le nom 'temperature_evolution_mllib.png'.")

spark.stop()