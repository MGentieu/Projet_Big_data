from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType

# Initialiser une session Spark
spark = SparkSession.builder \
.appName("Handle_latitude_longitude") \
.getOrCreate()

# Charger le fichier CSV dans un DataFrame Spark
df = spark.read.csv("hdfs:///user/root/projet/GlobalLandTemperaturesByCity.csv", header=True, inferSchema=True)
print(df.columns)
# Conversion des colonnes AverageTemperature et AverageTemperatureUncertainty en Double
# Convertir le DataFrame en RDD pour un traitement partitionné
original_rdd = df.rdd

def remove_last_char(lat):
    if lat and isinstance(lat, str):  # Vérifier si lat est non-None et une chaîne de caractères
        return lat[:-1]
    return lat  # Si lat est None ou autre type, retourner tel quel

# Enregistrer l'UDF
remove_last_char_udf = udf(remove_last_char)

# Appliquer l'UDF à la colonne Latitude
df = df.withColumn("Latitude", remove_last_char_udf(df["Latitude"]))

# Afficher les premières lignes pour vérifier
df.show(50)


#input_csv_path = sys.argv[1]
#output_csv_path = sys.argv[2]

# Appeler la fonction pour traiter le fichier
fill_missing_values()


