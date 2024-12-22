from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType
import sys

def remove_last_char(colonne):
    if colonne and isinstance(colonne, str):
        if(colonne[-1] in ["S","O"]):
            return  "-"+colonne[:-1]
        return colonne[:-1]
    return colonne

def normalise_latitude_longitude(df):
    remove_last_char_udf = udf(remove_last_char)
    df = df.withColumn("Latitude", remove_last_char_udf(df["Latitude"]))
    df = df.withColumn("Longitude",remove_last_char_udf(df["Longitude"]))
    df = df.withColumn("Latitude", col("Latitude").cast(DoubleType()))
    df = df.withColumn("Longitude", col("Longitude").cast(DoubleType()))
    return df
