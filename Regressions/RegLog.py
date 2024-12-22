import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, month, when
from pyspark.ml.feature import VectorAssembler, StringIndexer, StandardScaler
from pyspark.ml.classification import LogisticRegression

# Initialisation de la session Spark
spark = SparkSession.builder \
    .appName("TemperatureSeasonPrediction") \
    .getOrCreate()

# Charger le dataset depuis HDFS
df = spark.read.csv("hdfs:///user/root/projet/GlobalLandTemperaturesByCountry.csv", header=True, inferSchema=True)

# Filtrer les données pour la Tunisie
df_tunisia = df.filter(df['Country'] == 'Tunisia')

# Convertir la colonne 'dt' en type date et extraire le mois
df_tunisia = df_tunisia.withColumn('dt', to_date(col('dt'), 'yyyy-MM-dd'))
df_tunisia = df_tunisia.withColumn('Month', month(col('dt')))

# Créer une colonne catégorielle "Season"
df_tunisia = df_tunisia.withColumn(
    'Season',
    when((col('Month') == 12) | (col('Month') <= 2), 'Winter')
    .when((col('Month') >= 6) & (col('Month') <= 8), 'Summer')
    .otherwise('Transition')
)

df_tunisia.show(10)

# Garder les colonnes nécessaires et supprimer les valeurs nulles
df_tunisia = df_tunisia.select('AverageTemperature', 'Season').dropna()

# Convertir la colonne catégorielle "Season" en numérique
indexer = StringIndexer(inputCol="Season", outputCol="SeasonIndex")
df_tunisia = indexer.fit(df_tunisia).transform(df_tunisia)

# Préparer les données pour la régression logistique
assembler = VectorAssembler(inputCols=["AverageTemperature"], outputCol="features")
df_tunisia = assembler.transform(df_tunisia)

# Normaliser la température moyenne
scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures")
df_tunisia = scaler.fit(df_tunisia).transform(df_tunisia)

# Séparer les données en ensemble d'entraînement et de test
train_data, test_data = df_tunisia.randomSplit([0.8, 0.2], seed=42)

# Définir le modèle de régression logistique
lr = LogisticRegression(featuresCol="scaledFeatures", labelCol="SeasonIndex", maxIter=20, regParam=0.3, elasticNetParam=0.8)

# Entraîner le modèle
lr_model = lr.fit(train_data)

# Faire des prédictions sur l'ensemble de test
predictions = lr_model.transform(test_data)

# Convertir les prédictions en Pandas
predictions_pd = predictions.select("AverageTemperature", "Season", "prediction").toPandas()

# Convertir les index de prédiction en noms de saisons
index_to_season = {0: "Transition", 1: "Summer", 2: "Winter"}
predictions_pd["PredictedSeason"] = predictions_pd["prediction"].map(index_to_season)

# Associer une couleur : vert si correct, rouge si incorrect
predictions_pd["Color"] = predictions_pd.apply(
    lambda row: "green" if row["Season"] == row["PredictedSeason"] else "red",
    axis=1
)

# Créer un graphique
plt.figure(figsize=(12, 6))
for color, group in predictions_pd.groupby("Color"):
    plt.scatter(
        group["AverageTemperature"],
        group["Season"],
        c=color,
        label="Correct" if color == "green" else "Incorrect",
        alpha=0.6,
        s=20
    )

# Ajouter des labels et une légende
plt.title("Prédictions des saisons en Tunisie", fontsize=16)
plt.xlabel("Température Moyenne (°C)", fontsize=12)
plt.ylabel("Saison Réelle", fontsize=12)
plt.legend(title="Précision", fontsize=10)
plt.grid(True, linestyle="--", alpha=0.5)

# Sauvegarder le graphique
plt.savefig("season_predictions_tunisia.png", dpi=300)
print("Graphique sauvegardé sous le nom 'season_predictions_tunisia.png'.")

# Arrêter la session Spark
spark.stop()