from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split
from pyspark.sql.types import IntegerType
from pyspark.ml.feature import StringIndexer, VectorAssembler
import xgboost as xgb
from sklearn.metrics import mean_squared_error, r2_score
import pandas as pd

spark = SparkSession.builder \
    .appName("TemperaturePrediction") \
    .getOrCreate()

file_path = "hdfs:///user/root/projet/GLTBC2.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)


df = df.select("dt", "Country", "AverageTemperature")
df = df.withColumn("year", split(col("dt"), "-").getItem(0).cast(IntegerType()))
df = df.withColumn("month", split(col("dt"), "-").getItem(1).cast(IntegerType()))

indexer = StringIndexer(inputCol="Country", outputCol="CountryIndex")
indexer_model = indexer.fit(df)
df = indexer.fit(df).transform(df)

assembler = VectorAssembler(inputCols=["CountryIndex", "year", "month"], outputCol="features")
df = assembler.transform(df)

train_data, test_data = df.randomSplit([0.8, 0.2], seed=1234)

X_train = train_data.select("CountryIndex", "year", "month").collect()
y_train = [row["AverageTemperature"] for row in train_data.collect()]
X_test = test_data.select("CountryIndex", "year", "month").collect()
y_test = [row["AverageTemperature"] for row in test_data.collect()]

X_train = pd.DataFrame(X_train, columns=["CountryIndex", "year", "month"])
X_test = pd.DataFrame(X_test, columns=["CountryIndex", "year", "month"])

# Modèle XGBoost
xgb_model = xgb.XGBRegressor(objective="reg:squarederror", eval_metric="rmse")
xgb_model.fit(X_train, y_train)

# Prédictions
xgb_predictions = xgb_model.predict(X_test)

# Évaluer les performances
xgb_rmse = mean_squared_error(y_test, xgb_predictions, squared=False)
xgb_r2 = r2_score(y_test, xgb_predictions)

print(f"XGBoost - RMSE: {xgb_rmse:.2f}, R²: {xgb_r2:.2f}")

#################################PREDICTION####################################
predictions_df = pd.DataFrame({
    "CountryIndex": X_test["CountryIndex"],
    "year": X_test["year"],
    "month": X_test["month"],
    "prediction": xgb_predictions
})

spark_predictions = spark.createDataFrame(predictions_df)
spark_predictions = spark_predictions.join(df.select("Country", "CountryIndex").distinct(), on="CountryIndex", how="left")
spark_predictions.select("Country", "year", "month", "prediction").show(10)

spark.stop()