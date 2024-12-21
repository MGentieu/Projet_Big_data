from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, month, when
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# Initialisation de la session Spark
spark = SparkSession.builder \
    .appName("TemperatureSeasonPrediction") \
    .getOrCreate()

# Charger le dataset GlobalLandTemperaturesByCountry depuis un fichier local ou HDFS
df = spark.read.csv("hdfs:///user/root/projet/GlobalLandTemperaturesByCountry.csv", header=True, inferSchema=True)

# Filtrer les données pour la Tunisie
df_tunisia = df.filter(df['Country'] == 'Tunisia')

# Convertir la colonne 'dt' en type date
df_tunisia = df_tunisia.withColumn('dt', to_date(col('dt'), 'yyyy-MM-dd'))

# Ajouter une colonne 'Month' pour extraire le mois à partir de la date
df_tunisia = df_tunisia.withColumn('Month', month(col('dt')))

# Créer une colonne catégorielle "Season" avec 3 catégories : "Winter", "Summer", "Transition"
df_tunisia = df_tunisia.withColumn(
    'Season',
    when((col('Month') == 12) | (col('Month') <= 2), 'Winter')
    .when((col('Month') >= 6) & (col('Month') <= 8), 'Summer')
    .otherwise('Transition')  # Fusion de Spring et Autumn
)

# Garder les colonnes nécessaires et supprimer les lignes avec des valeurs nulles
df_tunisia = df_tunisia.select('AverageTemperature', 'Season').dropna()

# Convertir la colonne catégorielle "Season" en une colonne numérique à l'aide de StringIndexer
indexer = StringIndexer(inputCol="Season", outputCol="SeasonIndex")
df_tunisia = indexer.fit(df_tunisia).transform(df_tunisia)

# Préparer les données pour la régression logistique
assembler = VectorAssembler(inputCols=["AverageTemperature"], outputCol="features")
df_tunisia = assembler.transform(df_tunisia)

# Séparer les données en ensemble d'entraînement et de test
train_data, test_data = df_tunisia.randomSplit([0.8, 0.2], seed=42)

# Définir le modèle de régression logistique
lr = LogisticRegression(featuresCol="features", labelCol="SeasonIndex", maxIter=10)

# Entraîner le modèle
lr_model = lr.fit(train_data)

# Faire des prédictions sur l'ensemble de test
predictions = lr_model.transform(test_data)

# Évaluer les performances du modèle
evaluator = MulticlassClassificationEvaluator(
    labelCol="SeasonIndex", predictionCol="prediction", metricName="accuracy"
)
accuracy = evaluator.evaluate(predictions)

print(f"Précision du modèle : {accuracy:.2f}")

# Afficher un exemple de prédictions
predictions.select("AverageTemperature", "Season", "prediction").show()

# Arrêter la session Spark
spark.stop()
