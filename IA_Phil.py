from pyspark.sql import SparkSession
from pyspark.sql.functions import lag, col
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

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
