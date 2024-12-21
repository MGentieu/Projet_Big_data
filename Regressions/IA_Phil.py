import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml.clustering import KMeans
import matplotlib.pyplot as plt
import pandas as pd

# Initialisation de la session Spark
spark = SparkSession.builder \
    .appName("TemperatureClusteringTunisia") \
    .getOrCreate()

# Charger le dataset depuis HDFS (ou stockage local compatible avec Hadoop)
df = spark.read.csv("hdfs:///user/root/projet/GlobalLandTemperaturesByCity.csv", header=True, inferSchema=True)

# Filtrer les données pour la Tunisie
df_tunisia = df.filter(df['Country'] == 'Tunisia')

# Convertir la colonne 'dt' en type date sans séparer les colonnes
df_tunisia = df_tunisia.withColumn('dt', to_date(col('dt'), 'yyyy-MM-dd'))

# Sélectionner les colonnes nécessaires : 'dt' et 'AverageTemperature'
df_tunisia_temp = df_tunisia.select('dt', 'AverageTemperature').dropna()

# Utilisation de VectorAssembler pour transformer 'AverageTemperature' en un vecteur
assembler = VectorAssembler(inputCols=["AverageTemperature"], outputCol="features")
df_tunisia_temp = assembler.transform(df_tunisia_temp)

# Normalisation des températures avec StandardScaler
scaler = StandardScaler(inputCol="features", outputCol="scaledTemp")
df_tunisia_temp = scaler.fit(df_tunisia_temp).transform(df_tunisia_temp)

# Appliquer KMeans avec 3 clusters
kmeans = KMeans(k=3, featuresCol="scaledTemp", predictionCol="Cluster")
model = kmeans.fit(df_tunisia_temp)
df_tunisia_temp = model.transform(df_tunisia_temp)

# Convertir en Pandas pour la visualisation
df_tunisia_temp_pd = df_tunisia_temp.select('dt', 'AverageTemperature', 'Cluster').toPandas()

# Visualisation des résultats du clustering
plt.figure(figsize=(10, 6))
plt.scatter(df_tunisia_temp_pd['dt'], df_tunisia_temp_pd['AverageTemperature'], c=df_tunisia_temp_pd['Cluster'], cmap='viridis', s=10)
plt.title('Clustering des températures en Tunisie (3 clusters)', fontsize=16)
plt.xlabel('Date', fontsize=12)
plt.ylabel('Température moyenne (°C)', fontsize=12)
plt.grid(True, linestyle='--', alpha=0.6)
plt.legend(title='Cluster')

# Afficher le graphique
plt.show()

# Sauvegarde du graphique
plt.savefig("temperature_clustering_tunisia.png")
print("Graphique sauvegardé sous le nom 'temperature_clustering_tunisia.png'.")

# Compter le nombre de valeurs dans chaque cluster
cluster_counts = df_tunisia_temp.groupBy('Cluster').count().collect()

# Afficher le nombre de valeurs par cluster
print("Nombre de valeurs par cluster :")
for row in cluster_counts:
    print(f"Cluster {row['Cluster']}: {row['count']} occurrences")

# Arrêter la session Spark
spark.stop()
