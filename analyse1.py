from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pandas as pd
#Création de la session spark
spark = SparkSession.builder.appName("chargement_dataset").getOrCreate()

#Chargement du fichier CSV depuis hadoop fs:
df1 = spark.read.option("delimiter",",").csv("hdfs:///user/root/projet/GlobalLandTemperaturesByCity.csv", header=True, inferSchema=True)
print("Temperatures par ville :" )
print(df1.columns)

df1.show(30)

