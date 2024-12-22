import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split
from pyspark.ml.feature import VectorAssembler, PolynomialExpansion
from pyspark.ml.regression import LinearRegression, RandomForestRegressor, GBTRegressor
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
import matplotlib.pyplot as plt
import numpy as np


# Fonction d'évaluation du modèle
def evaluate_mllib_model(predictions, evaluator, model_name):
    rmse = evaluator.evaluate(predictions, {evaluator.metricName: "rmse"})
    r2 = evaluator.evaluate(predictions, {evaluator.metricName: "r2"})

    print(f"\n--- Évaluation du Modèle ({model_name}) ---")
    print(f"RMSE : {rmse:.4f}")
    print(f"R² : {r2:.4f}")

    # Visualisation
    plt.figure(figsize=(12, 6))

    # Utilisez les données complètes pour l'axe x
    plt.plot(df_pd['year'], df_pd['YearlyAverageTemperature'], color='blue', label='Température moyenne annuelle')

    # Utilisez les prédictions sur l'ensemble de test (les prédictions sont calculées sur les années)
    predictions_pd = predictions.select("year", "prediction").toPandas()
    predictions_pd = predictions_pd.sort_values(by="year")

    # Tracez la tendance
    plt.plot(predictions_pd['year'], predictions_pd['prediction'], label=f'Tendance ({model_name})', linestyle='--')
    plt.title(f"Évolution de la température moyenne avec {model_name}")
    plt.xlabel("Année")
    plt.ylabel("Température moyenne (°C)")
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.legend()
    plt.tight_layout()
    plt.savefig(f"temperature_evolution_{model_name}.png")
    print(f"Graphique sauvegardé sous le nom 'temperature_evolution_{model_name}.png'.")

# Démarrage de la session Spark
spark = SparkSession.builder \
    .appName("TemperatureEvolution_MLlib") \
    .getOrCreate()

# Chargement des données
df = spark.read.csv("hdfs:///user/root/projet/GlobalTemperatures.csv", header=True, inferSchema=True)

# Afficher les premières lignes du dataset
print("\n--- Dataset chargé ---")
df.show(10)

# Calcul de la température moyenne par année
df_yearly_avg = df.groupBy("year").avg("LandAverageTemperature").withColumnRenamed("avg(LandAverageTemperature)",
                                                                                   "YearlyAverageTemperature")

# Conversion en Pandas
df_pd = df_yearly_avg.toPandas()
df_pd = df_pd.sort_values(by="year")

# Préparation des données
assembler = VectorAssembler(inputCols=["year"], outputCol="features")
df_yearly_avg = assembler.transform(df_yearly_avg)

# Appliquer la transformation polynomiale
degree = 2
poly_expansion = PolynomialExpansion(degree=degree, inputCol="features", outputCol="polyFeatures")
df_poly = poly_expansion.transform(df_yearly_avg)

# Séparer les données en ensembles d'entraînement et de test
train_data, test_data = df_poly.randomSplit([0.7, 0.3], seed=42)

# Evaluation avec un Regresseur
evaluator = RegressionEvaluator(labelCol="YearlyAverageTemperature", predictionCol="prediction")

# Modèle 1 : Régression Linéaire
lr = LinearRegression(featuresCol="features", labelCol="YearlyAverageTemperature")
lr_model = lr.fit(train_data)
lr_predictions = lr_model.transform(test_data)
evaluate_mllib_model(lr_predictions, evaluator, "Régression Linéaire")

# Modèle 2 : Régression Polynomiale
lr_poly = LinearRegression(featuresCol="polyFeatures", labelCol="YearlyAverageTemperature")
lr_poly_model = lr_poly.fit(train_data)
poly_predictions = lr_poly_model.transform(test_data)
evaluate_mllib_model(poly_predictions, evaluator, f"Régression Polynomiale (degré {degree})")

# Modèle 3 : Random Forest
rf = RandomForestRegressor(featuresCol="features", labelCol="YearlyAverageTemperature")
rf_model = rf.fit(train_data)
rf_predictions = rf_model.transform(test_data)
evaluate_mllib_model(rf_predictions, evaluator, "Forêt Aléatoire")

# Modèle 4 : Gradient Boosting
gbt = GBTRegressor(featuresCol="features", labelCol="YearlyAverageTemperature")
gbt_model = gbt.fit(train_data)
gbt_predictions = gbt_model.transform(test_data)
evaluate_mllib_model(gbt_predictions, evaluator, "Gradient Boosting")

# Arrêt de Spark
spark.stop()
