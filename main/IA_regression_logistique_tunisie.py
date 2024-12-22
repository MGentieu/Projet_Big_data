import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.ml.feature import VectorAssembler, StringIndexer, StandardScaler
from pyspark.ml.classification import LogisticRegression

spark = SparkSession.builder \
    .appName("TemperatureSeasonPrediction") \
    .getOrCreate()

df = spark.read.csv("hdfs:///user/root/projet/GlobalLandTemperaturesByCountry.csv", header=True, inferSchema=True)

print("\n--- Dataset chargé ---")
df.show(10)

df_tunisia = df.filter(df['Country'] == 'Tunisia')

# Créer une colonne catégorielle "Season" en fonction de la colonne 'month'
df_tunisia = df_tunisia.withColumn(
    'Season',
    when((col('month') == 12) | (col('month') <= 2), 'Winter')
    .when((col('month') >= 6) & (col('month') <= 8), 'Summer')
    .otherwise('Transition')
)

df_tunisia = df_tunisia.select('AverageTemperature', 'Season').dropna()
df_tunisia_pd = df_tunisia.toPandas()

# Créer un graphique initial des données du dataset
plt.figure(figsize=(12, 6))
plt.scatter(df_tunisia_pd["AverageTemperature"], df_tunisia_pd["Season"], alpha=0.6, s=20)
plt.title("Données Initiales des Saisons en Tunisie", fontsize=16)
plt.xlabel("Température Moyenne (°C)", fontsize=12)
plt.ylabel("Saison Réelle", fontsize=12)
plt.grid(True, linestyle="--", alpha=0.5)
plt.savefig("dataset_initial_seasons_tunisia.png", dpi=300)
print("Graphique des données initiales sauvegardé sous le nom 'dataset_initial_seasons_tunisia.png'.")

# Convertir la colonne catégorielle "Season" en numérique
indexer = StringIndexer(inputCol="Season", outputCol="SeasonIndex")
df_tunisia = indexer.fit(df_tunisia).transform(df_tunisia)

# Préparer les données pour la régression logistique
assembler = VectorAssembler(inputCols=["AverageTemperature"], outputCol="features")
df_tunisia = assembler.transform(df_tunisia)

# Normaliser la température moyenne
scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures")
df_tunisia = scaler.fit(df_tunisia).transform(df_tunisia)

train_data, test_data = df_tunisia.randomSplit([0.8, 0.2], seed=42)

lr = LogisticRegression(featuresCol="scaledFeatures", labelCol="SeasonIndex", maxIter=20, regParam=0.3, elasticNetParam=0.8)
lr_model = lr.fit(train_data)

predictions = lr_model.transform(test_data)
predictions_pd = predictions.select("AverageTemperature", "Season", "prediction").toPandas()

index_to_season = {0: "Transition", 1: "Summer", 2: "Winter"}
predictions_pd["PredictedSeason"] = predictions_pd["prediction"].map(index_to_season)

# Créer un graphique pour les données de prédiction
plt.figure(figsize=(12, 6))
plt.scatter(predictions_pd["AverageTemperature"], predictions_pd["PredictedSeason"], alpha=0.6, s=20)
plt.title("Prédictions des Saisons en Tunisie (Régression Logistique)", fontsize=16)
plt.xlabel("Température Moyenne (°C)", fontsize=12)
plt.ylabel("Saison Prédite", fontsize=12)
plt.grid(True, linestyle="--", alpha=0.5)
plt.savefig("predicted_seasons_tunisia.png", dpi=300)
print("Graphique des prédictions sauvegardé sous le nom 'predicted_seasons_tunisia.png'.")

spark.stop()