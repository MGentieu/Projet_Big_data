from pyspark.sql import SparkSession
from pyspark.sql.functions import col

#Création de la session spark
spark = SparkSession.builder.appName("chargement_dataset").getOrCreate()

#Chargement du fichier CSV depuis hadoop fs:
df1 = spark.read.option("delimiter",",").csv("hdfs:///user/root/projet/GlobalLandTemperaturesByCity.csv", header=True, inferSchema=True)
print("Temperatures par ville :" )
print(df1.columns)

df1.show(30)
"""

df2 = spark.read.option("delimiter",",").csv("hdfs:///projet/GlobalLandTemperaturesByCountry.csv", header=True, inferSchema=True)
print("Temperatures par ville :" )
print(df2.columns)

df3 = spark.read.option("delimiter",",").csv("hdfs:///projet/GlobalLandTemperaturesByMajorCity.csv", header=True, inferSchema=True)
print("Temperatures par ville :" )
print(df3.columns)

df4 = spark.read.option("delimiter",",").csv("hdfs:///projet/GlobalLandTemperaturesByState.csv", header=True, inferSchema=True)
print("Temperatures par ville :" )
print(df4.columns)

df5 = spark.read.option("delimiter",",").csv("hdfs:///projet/GlobalTemperatures.csv", header=True, inferSchema=True)
print("Temperatures par ville :" )
print(df5.columns)

"""
