from pyspark.sql import SparkSession
from pyspark.sql.functions import col

#Création de la session spark
spark = SparkSession.builder.appName("chargement_dataset").getOrCreate()

#Chargement du fichier CSV depuis hadoop fs:
df1 = spark.read.option("delimiter",",").csv("hdfs:///user/root/projet/GLTBCi.csv", header=True, inferSchema=True)
print("Temperatures par ville :" )
print(df1.columns)
print("d1 : Villes")
df1.show(10)


df2 = spark.read.option("delimiter",",").csv("hdfs:///user/root/projet/GLTBCo.csv", header=True, inferSchema=True)
print("Temperatures par pays :" )
print(df2.columns)
print("d2 : pays")
df2.show(10)

df3 = spark.read.option("delimiter",",").csv("hdfs:///user/root/projet/GLTBMC.csv", header=True, inferSchema=True)
print("Temperatures par grande ville :" )
print(df3.columns)
print("d3 : Major city")
df3.show(10)


df4 = spark.read.option("delimiter",",").csv("hdfs:///user/root/projet/GLTBS.csv", header=True, inferSchema=True)
print("Temperatures par etat :" )
print(df4.columns)
print("d4 : State")
df4.show(10)


df5 = spark.read.option("delimiter",",").csv("hdfs:///user/root/projet/GT.csv", header=True, inferSchema=True)
print("Temperatures globales :" )
print(df5.columns)
df5.show(50)

