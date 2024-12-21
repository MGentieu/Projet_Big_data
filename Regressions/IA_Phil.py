import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, to_date
from pyspark.ml.feature import StandardScaler
from pyspark.ml.clustering import KMeans
import matplotlib.pyplot as plt
import pandas as pd

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

# Initialisation de la session Spark
spark = SparkSession.builder \
    .appName("TemperatureClusteringTunisia") \
    .getOrCreate()


df = spark.read.csv("hdfs:///user/root/projet/GlobalTemperaturesByCountry.csv", header=True, inferSchema=True)

# Filtrer les données pour la Tunisie
df_tunisia = df.filter(df['Country'] == 'Tunisia')

df_tunisia = df_tunisia.withColumn('dt', to_date(col('dt'), 'yyyy-MM-dd'))
df_tunisia = process_date_column(df_tunisia)


# Sélectionner les colonnes nécessaires : 'dt' et 'AverageTemperature'
df_tunisia_temp = df_tunisia.select('year', 'month', 'AverageTemperature').dropna()

# Normalisation des températures
scaler = StandardScaler(inputCol="AverageTemperature", outputCol="scaledTemp")
df_tunisia_temp = scaler.fit(df_tunisia_temp).transform(df_tunisia_temp)

# Appliquer KMeans avec 3 clusters
kmeans = KMeans(k=3, featuresCol="scaledTemp", predictionCol="Cluster")
model = kmeans.fit(df_tunisia_temp)
df_tunisia_temp = model.transform(df_tunisia_temp)


# Convertir en Pandas pour la visualisation
df_tunisia_temp_pd = df_tunisia_temp.select('year', 'month', 'AverageTemperature', 'Cluster').toPandas()

# Visualisation des résultats du clustering
plt.figure(figsize=(10, 6))
plt.scatter(df_tunisia_temp_pd['year'], df_tunisia_temp_pd['AverageTemperature'], c=df_tunisia_temp_pd['Cluster'], cmap='viridis', s=10)
plt.title('Clustering des températures en Tunisie (3 clusters)', fontsize=16)
plt.xlabel('Année', fontsize=12)
plt.ylabel('Température moyenne (°C)', fontsize=12)
plt.grid(True, linestyle='--', alpha=0.6)
plt.legend(title='Cluster')

# Sauvegarde du graphique
plt.savefig("temperature_clustering_tunisia.png")
print("Graphique sauvegardé sous le nom 'temperature_clustering_tunisia.png'.")

# Compter le nombre de valeurs dans chaque cluster
cluster_counts = df_tunisia_temp.groupBy('Cluster').count().collect()
print("Nombre de valeurs par cluster :")
for row in cluster_counts:
    print(f"Cluster {row['Cluster']}: {row['count']} occurrences")

spark.stop()
