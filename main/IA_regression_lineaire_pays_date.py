from pyspark.sql import Row
from pyspark.ml.linalg import Vectors
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth, col, split, to_date, when
from pyspark.sql.types import IntegerType, StringType
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import RandomForestRegressor
import xgboost as xgb
from sklearn.metrics import mean_squared_error, r2_score
import pandas as pd

# Initialisation de la session Spark
spark = SparkSession.builder \
    .appName("TemperaturePrediction") \
    .getOrCreate()

# Charger le fichier CSV depuis HDFS
file_path = "hdfs:///user/root/projet/GLTBC2.csv" #GlobalLandTemperaturesByCountry.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)

# Prétraitement des données
# Sélectionner uniquement les colonnes nécessaires
df = df.select("dt", "Country", "AverageTemperature")

# Transformer la colonne "dt" en colonnes "year" et "month" (avec ta méthode)
df = df.withColumn("year", split(col("dt"), "-").getItem(0).cast(IntegerType()))
df = df.withColumn("month", split(col("dt"), "-").getItem(1).cast(IntegerType()))

# Encoder la colonne "Country" en indices numériques
indexer = StringIndexer(inputCol="Country", outputCol="CountryIndex")
indexer_model = indexer.fit(df)
df = indexer.fit(df).transform(df)

# Création du vecteur de features
assembler = VectorAssembler(inputCols=["CountryIndex", "year", "month"], outputCol="features")
df = assembler.transform(df)

# Diviser les données en ensembles d'entraînement et de test
train_data, test_data = df.randomSplit([0.8, 0.2], seed=1234)

# Préparation des données
X_train = train_data.select("CountryIndex", "year", "month").collect()
y_train = [row["AverageTemperature"] for row in train_data.collect()]
X_test = test_data.select("CountryIndex", "year", "month").collect()
y_test = [row["AverageTemperature"] for row in test_data.collect()]

# Conversion en tableaux NumPy/Pandas
X_train = pd.DataFrame(X_train, columns=["CountryIndex", "year", "month"])
X_test = pd.DataFrame(X_test, columns=["CountryIndex", "year", "month"])

# Modèle XGBoost
xgb_model = xgb.XGBRegressor(objective="reg:squarederror", eval_metric="rmse")
xgb_model.fit(X_train, y_train)

# Prédictions
xgb_predictions = xgb_model.predict(X_test)

# Évaluer les performances
xgb_rmse = mean_squared_error(y_test, xgb_predictions, squared=False)
xgb_r2 = r2_score(y_test, xgb_predictions)

print(f"XGBoost - RMSE: {xgb_rmse:.2f}, R²: {xgb_r2:.2f}")

#################################PREDICTION####################################
predictions_df = pd.DataFrame({
    "CountryIndex": X_test["CountryIndex"],
    "year": X_test["year"],
    "month": X_test["month"],
    "prediction": xgb_predictions
})

spark_predictions = spark.createDataFrame(predictions_df)

spark_predictions = spark_predictions.join(df.select("Country", "CountryIndex").distinct(), on="CountryIndex", how="left")

# Afficher les résultats
spark_predictions.select("Country", "year", "month", "prediction").show(10)

def prediction_temperature(pays, annee, mois, model, indexer_model):
    # Indexer le pays
    pays_index = indexer_model.transform(
        spark.createDataFrame([Row(Country=pays)])
    ).select("CountryIndex").collect()[0][0]

    # Créer les features pour la prédiction
    features = pd.DataFrame([[pays_index, annee, mois]], columns=["CountryIndex", "year", "month"])

    # Prédiction avec le modèle XGBoost
    temperature_predite = model.predict(features)[0]

    return temperature_predite


# Exemples d'appel de la fonction
pays = "France"
annee = 2025
mois = 4

pays2 = "France"
annee2 = 2040
mois2 = 7

pays3 = "France"
annee3 = 2000
mois3 = 2

# Estimation de la température
temperature_estimee = prediction_temperature(pays, annee, mois, xgb_model, indexer_model)
print(f"La température prédite pour {pays} en {annee}-{mois} est de : {temperature_estimee:.2f}°C")

temperature_estimee2 = prediction_temperature(pays2, annee2, mois2, xgb_model, indexer_model)
print(f"La température prédite pour {pays2} en {annee2}-{mois2} est de : {temperature_estimee2:.2f}°C")

temperature_estimee3 = prediction_temperature(pays3, annee3, mois3, xgb_model, indexer_model)
print(f"La température prédite pour {pays3} en {annee3}-{mois3} est de : {temperature_estimee3:.2f}°C")

# Arrêter Spark
spark.stop()
