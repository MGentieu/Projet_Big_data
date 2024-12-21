import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split
from pyspark.sql.types import IntegerType
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import PolynomialFeatures
from xgboost import XGBRegressor
import numpy as np
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.model_selection import train_test_split


# Fonction pour traiter la colonne 'dt'
def process_date_column(df):
    if "dt" in df.columns:
        df = df.withColumn("year", split(col("dt"), "-").getItem(0).cast(IntegerType()))
        df = df.withColumn("month", split(col("dt"), "-").getItem(1).cast(IntegerType()))
        df = df.withColumn("day", split(col("dt"), "-").getItem(2).cast(IntegerType()))
        df = df.drop("dt")
    else:
        print("La colonne 'dt' est absente dans le DataFrame.")
    return df

# Fonction d'évaluation du modèle
def evaluate_regression_model(model, X_test, y_test, X, y, model_name):
    y_pred = model.predict(X_test)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    r2 = r2_score(y_test, y_pred)
    relative_errors = np.abs((y_test - y_pred) / y_test) * 100
    mean_relative_error = np.mean(relative_errors)

    # Affichage des résultats
    print(f"\n--- Évaluation du Modèle ({model_name}) ---")
    print(f"RMSE : {rmse:.4f}")
    print(f"R² : {r2:.4f}")
    print(f"Pourcentage d'erreur moyenne relative : {mean_relative_error:.2f} %")

    # Visualisation
    plt.figure(figsize=(12, 6))
    plt.plot(df_pd['year'], df_pd['YearlyAverageTemperature'], color='blue', label='Température moyenne annuelle')
    plt.plot(df_pd['year'], model.predict(X), label=f'Tendance ({model_name})', linestyle='--')
    plt.title(f"Évolution de la température moyenne avec {model_name}")
    plt.xlabel("Année")
    plt.ylabel("Température moyenne (°C)")
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.legend()
    plt.tight_layout()
    plt.savefig(f"temperature_evolution_{model_name}.png")
    print(f"Graphique sauvegardé sous le nom 'temperature_evolution_{model_name}.png'.")

# Démarrage de la session Spark
spark = SparkSession.builder \
    .appName("TemperatureEvolution") \
    .getOrCreate()

# Chargement des données
df = spark.read.csv("hdfs:///user/root/projet/GlobalTemperatures.csv", header=True, inferSchema=True)
df = df.select("dt", "LandAverageTemperature")

# Traitement de la colonne 'dt'
df = process_date_column(df)

# Calcul de la température moyenne par année
df_yearly_avg = df.groupBy("year").avg("LandAverageTemperature").withColumnRenamed("avg(LandAverageTemperature)",
                                                                                   "YearlyAverageTemperature")

# Conversion en Pandas
df_pd = df_yearly_avg.toPandas()
df_pd = df_pd.sort_values(by="year")

# Préparation des données
X = df_pd['year'].values.reshape(-1, 1)  # Année
y = df_pd['YearlyAverageTemperature'].values  # Température
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

# Modèle 1 : Régression Linéaire
model_linear = LinearRegression()
model_linear.fit(X_train, y_train)
evaluate_regression_model(model_linear, X_test, y_test, X, y, "Régression Linéaire")

# Modèle 2 : Régression Polynomiale
degree = 2
model_poly = make_pipeline(PolynomialFeatures(degree), LinearRegression())
model_poly.fit(X_train, y_train)
evaluate_regression_model(model_poly, X_test, y_test, X, y, f"Régression Polynomiale (degré {degree})")

# Modèle 3 : Random Forest
model_rf = RandomForestRegressor(n_estimators=100, random_state=42)
model_rf.fit(X_train, y_train)
evaluate_regression_model(model_rf, X_test, y_test, X, y, "Forêt Aléatoire")

# Modèle 4 : Gradient Boosting
model_xgb = XGBRegressor(n_estimators=100, learning_rate=0.1, random_state=42)
model_xgb.fit(X_train, y_train)
evaluate_regression_model(model_xgb, X_test, y_test, X, y, "Gradient Boosting")

# Arrêt de Spark
spark.stop()
