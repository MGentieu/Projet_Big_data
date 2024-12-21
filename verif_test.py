from pyspark.sql import SparkSession
from pyspark.sql.functions import col

#Création de la session spark
spark = SparkSession.builder.appName("chargement_dataset").getOrCreate()
"""
df2 = spark.read.option("delimiter",",").csv("hdfs:///user/root/projet/test.csv", header=True, inferSchema=True)
print("Temperatures par pays :" )
print(df2.columns)
print("d2 : pays")
df2.show(150)
"""
df3 = spark.read.option("delimiter",",").csv("hdfs:///user/root/projet/final_output.csv", header=True, inferSchema=True)
print("Temperatures par grande ville :" )
print(df3.columns)
print("d3 : Country corrige")
df3.show(150)
