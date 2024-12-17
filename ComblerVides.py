from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType

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

    # Convertir le DataFrame en RDD pour un traitement partitionné
    original_rdd = df.rdd

    # Algorithme pour combler les valeurs manquantes
    def process_partition(rows):
        # On utilise une copie de référence des lignes pour ne pas affecter les calculs suivants
        rows = list(rows)
        initial_rows = rows.copy()

        for i in range(len(rows)):
            if rows[i][1] is None or rows[i][2] is None:  # Vérifie les valeurs manquantes
                # Récupérer les précédentes et suivantes valeurs valides dans la copie initiale
                prev_temp, prev_uncert = None, None
                next_temp, next_uncert = None, None

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

        return rows

    # Appliquer le traitement partition par partition
    filled_rdd = original_rdd.mapPartitions(lambda partition: process_partition(partition))

    # Convertir l'RDD corrigé en DataFrame
    filled_df = spark.createDataFrame(filled_rdd, schema=df.schema)

    # Sauvegarder le DataFrame traité
    filled_df.write.csv(output_path, header=True)

    print(f"Le fichier traité a été sauvegardé dans {output_path}.")

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 3:
        print("Usage: python handle_missing_values.py <input_csv_path> <output_csv_path>")
        sys.exit(1)

    input_csv_path = sys.argv[1]
    output_csv_path = sys.argv[2]

    # Appeler la fonction pour traiter le fichier
    fill_missing_values(input_csv_path, output_csv_path)


