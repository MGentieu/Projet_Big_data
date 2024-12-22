from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType
from pyspark.sql import Row
import sys
import time
import subprocess


def rename_hdfs_file(hdfs_path):

    subprocess.run(["hdfs", "dfs", "-rm", f"{hdfs_path}/_SUCCESS"], check=True)  # Supprime _SUCCESS
    subprocess.run([
        "hdfs", "dfs", "-mv",
        f"{hdfs_path}/part-00000-*",  # Part-00000 avec identifiant aléatoire
        f"{hdfs_path}/../GLTBC.csv"  # Nouveau nom de fichier
    ], check=True)
    subprocess.run(["hdfs", "dfs", "-rmdir", "projet/GLTBC_doc.csv"], check=True)
    print(f"Le fichier a été renommé en 'final_output.csv' dans le répertoire HDFS : {hdfs_path}")

def process_partition(rows):
	# On utilise une copie de référence des lignes pour ne pas affecter les calculs suivants
	rows = list(rows)
	initial_rows = rows.copy()

	for i in range(len(rows)):
		if rows[i][1] is None or rows[i][2] is None:  # Vérifie les valeurs manquantes
			# Récupérer les précédentes et suivantes valeurs valides dans la copie initiale
			prev_temp=None
			prev_uncert=None
			next_temp=None
			next_uncert=None

			# Rechercher les valeurs précédentes valides
			for j in range(i - 1, -1, -1):
				if initial_rows[j][1] is not None and initial_rows[j][2] is not None:
					prev_temp, prev_uncert = initial_rows[j][1], initial_rows[j][2]
					break

			# Rechercher les valeurs suivantes valides
			for j in range(i + 1, len(rows)):
				if initial_rows[j][1] is not None and initial_rows[j][2] is not None:
					next_temp, next_uncert = initial_rows[j][1], initial_rows[j][2]
					break

			# Calculer les moyennes uniquement si les deux valeurs sont valides
			avg_temp = prev_temp if next_temp is None else (
				next_temp if prev_temp is None else (prev_temp + next_temp) / 2
                	)
			avg_uncert = prev_uncert if next_uncert is None else (
				next_uncert if prev_uncert is None else (prev_uncert + next_uncert) / 2
			)

			# Remplir les valeurs manquantes avec ces moyennes
			rows[i] = (rows[i][0], avg_temp, avg_uncert, rows[i][3])
	"""
	print('\n')
	print('\n')
	print('Partition après traitement :')
	print('\n')
	print('\n')
	print(rows)
	"""
	return rows

def fill_missing_values(file_path, output_path):
        # Initialiser une session Spark
	spark = SparkSession.builder \
	.appName("Handle Missing Values") \
	.getOrCreate()

	# Charger le fichier CSV dans un DataFrame Spark
	df = spark.read.csv(file_path, header=True, inferSchema=True)

	# Conversion des colonnes AverageTemperature et AverageTemperatureUncertainty en Double
	df = df.withColumn("AverageTemperature", col("AverageTemperature").cast(DoubleType()))
	df = df.withColumn("AverageTemperatureUncertainty", col("AverageTemperatureUncertainty").cast(DoubleType()))
	df = df.withColumn("year", split(col("dt"), "-").getItem(0).cast(IntegerType()))
	df = df.withColumn("month", split(col("dt"), "-").getItem(1).cast(IntegerType()))
	df = df.withColumn("day", split(col("dt"), "-").getItem(2).cast(IntegerType()))
	# Supprimer la colonne "dt"
	df = df.drop("dt")
	# Convertir le DataFrame en RDD pour un traitement partitionné
	original_rdd = df.rdd

	# Algorithme pour combler les valeurs manquantes


	# Appliquer le traitement partition par partition
	filled_rdd = original_rdd.mapPartitions(lambda partition: process_partition(partition))
	print("Exemple de lignes dans le RDD traité :")
	print(filled_rdd.take(5))

	filled_df = spark.createDataFrame(filled_rdd, schema=["AverageTemperature", "AverageTemperatureUncertainty", "Country","year",
	"month","day"])
	filled_df.show(15)

	# Regrouper les partitions en une seule
	filled_df.coalesce(1).write.option("header", True).mode("overwrite").csv(output_path)

	time.sleep(1)
	
	rename_hdfs_file(output_path)


if __name__ == "__main__":
	#if len(sys.argv) != 3:
	#	print("Usage: python handle_missing_values.py <input_csv_path> <output_csv_path>")
	#	sys.exit(1)

	#input_csv_path = sys.argv[1]
	#output_csv_path = sys.argv[2]
	input_csv_path = "hdfs:///user/root/projet/GlobalLandTemperaturesByCountry.csv"
	output_csv_path = "hdfs:///user/root/projet/GLTBC_doc.csv"
	# Appeler la fonction pour traiter le fichier
	fill_missing_values(input_csv_path, output_csv_path)


