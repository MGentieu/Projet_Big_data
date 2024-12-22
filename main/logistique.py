from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, when
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
import matplotlib.pyplot as plt
import pandas as pd

spark = SparkSession.builder.appName("TemperatureAnalysis").getOrCreate()

country_data = spark.read.csv('hdfs:///user/root/projet/GlobalLandTemperaturesByCountry.csv', header=True, inferSchema=True)
state_data = spark.read.csv('hdfs:///user/root/projet/GlobalLandTemperaturesByState.csv', header=True, inferSchema=True)

us_country_data = country_data.filter(col('Country') == 'United States')
us_state_data = state_data.filter(col('Country') == 'United States')
us_country_data = us_country_data.filter(col('year') >= 1811)
us_state_data = us_state_data.filter(col('year') >= 1811)

country_avg_temp = us_country_data.groupBy('year').agg(
    mean('AverageTemperature').alias('CountryAvgTemp'),
    mean('AverageTemperatureUncertainty').alias('AvgTempUncertainty')
)

state_avg_temp = us_state_data.groupBy('year').agg(
    mean('AverageTemperature').alias('StateAvgTemp')
)

# Fusionner les deux ensembles de données
merged_data = country_avg_temp.join(state_avg_temp, on='year', how='inner')
# Ajouter une colonne YearType basée sur la médiane
temperature_median = merged_data.approxQuantile('CountryAvgTemp', [0.5], 0.0)[0]
merged_data = merged_data.withColumn('YearType', when(col('CountryAvgTemp') > temperature_median, 1).otherwise(0))

# Vectoriser les données pour Spark MLlib
feature_columns = ['year', 'StateAvgTemp', 'AvgTempUncertainty']
assembler = VectorAssembler(inputCols=feature_columns, outputCol='features')
merged_data = assembler.transform(merged_data).select('features', 'YearType')


train_data, test_data = merged_data.randomSplit([0.8, 0.2], seed=42)

# Entraîner un modèle de régression logistique
lr = LogisticRegression(labelCol='YearType', featuresCol='features', maxIter=100)
model = lr.fit(train_data)

predictions = model.transform(test_data)

# Évaluation du modèle
evaluator = BinaryClassificationEvaluator(labelCol='YearType', metricName='areaUnderROC')
roc_auc = evaluator.evaluate(predictions)
print(f"AUC-ROC: {roc_auc}")

# Tracer une matrice de confusion
predictions_pd = predictions.select('YearType', 'prediction').toPandas()
conf_matrix = pd.crosstab(predictions_pd['YearType'], predictions_pd['prediction'], rownames=['Vérité'], colnames=['Prédictions'])
plt.imshow(conf_matrix, cmap='Blues')
plt.title("Matrice de confusion")
plt.colorbar()
plt.savefig('matrice_de_confusion.png')
plt.close()

# Tracer la courbe ROC
roc_data = predictions.select('YearType', 'probability').rdd.map(lambda row: (float(row['probability'][1]), float(row['YearType'])))
roc_df = pd.DataFrame(roc_data.collect(), columns=['probability', 'label'])
roc_df = roc_df.sort_values(by='probability', ascending=False)

true_positive_rate = []
false_positive_rate = []
thresholds = []
pos_count = len(roc_df[roc_df['label'] == 1])
neg_count = len(roc_df[roc_df['label'] == 0])
tp = 0
fp = 0

for _, row in roc_df.iterrows():
    if row['label'] == 1:
        tp += 1
    else:
        fp += 1
    true_positive_rate.append(tp / pos_count)
    false_positive_rate.append(fp / neg_count)
    thresholds.append(row['probability'])

plt.figure()
plt.plot(false_positive_rate, true_positive_rate, label=f"LogisticRegression (AUC = {roc_auc:.2f})")
plt.title("Courbe ROC")
plt.xlabel("False Positive Rate")
plt.ylabel("True Positive Rate")
plt.legend(loc="lower right")
plt.grid()
plt.savefig('/roc_curve.png')
plt.close()

# Ajout des prédictions par État
state_predictions = {}
states = us_state_data.select('State').distinct().rdd.flatMap(lambda x: x).collect()

future_years = spark.createDataFrame([(year,) for year in range(2024, 2051)], ['year'])

for state in states:
    state_data_filtered = us_state_data.filter(col('State') == state)
    state_avg_temp = state_data_filtered.groupBy('year').agg(
        mean('AverageTemperature').alias('StateAvgTemp')
    )

    if state_avg_temp.count() < 10:
        continue

    state_avg_temp = state_avg_temp.filter(col('StateAvgTemp').isNotNull())

    temperature_median = state_avg_temp.approxQuantile('StateAvgTemp', [0.5], 0.0)[0]
    state_avg_temp = state_avg_temp.withColumn('YearType', when(col('StateAvgTemp') > temperature_median, 1).otherwise(0))

    assembler_state = VectorAssembler(inputCols=['year'], outputCol='features')
    state_avg_temp = assembler_state.transform(state_avg_temp)

    train_data, test_data = state_avg_temp.randomSplit([0.8, 0.2], seed=42)
    lr_state = LogisticRegression(labelCol='YearType', featuresCol='features', maxIter=100)
    model_state = lr_state.fit(train_data)

    future_years_with_features = assembler_state.transform(future_years)
    future_predictions_state = model_state.transform(future_years_with_features)

    state_predictions[state] = {
        'model': model_state,
        'temperature_median': temperature_median,
        'historical_data': state_avg_temp,
        'future_predictions': future_predictions_state
    }

# Proportion des États chauds et froids en 2050
final_year = 2050
hot_states = 0
cold_states = 0
for state, data in state_predictions.items():
    future_predictions = data['future_predictions']
    prediction_2050 = future_predictions.filter(col('year') == final_year).select('prediction').collect()
    if prediction_2050:
        if prediction_2050[0][0] == 1:
            hot_states += 1
        else:
            cold_states += 1

print(f"Nombre d'États chauds en {final_year} : {hot_states}")
print(f"Nombre d'États froids en {final_year} : {cold_states}")

# Génération du diagramme camembert
labels = ['États chauds', 'États froids']
sizes = [hot_states, cold_states]
colors = ['#ff9999', '#66b3ff']

plt.figure(figsize=(8, 8))
plt.pie(sizes, labels=labels, colors=colors, autopct='%1.1f%%', startangle=90)
plt.title('Proportion des États chauds et froids en 2050')
plt.savefig('proportion_etats_chauds_froids_2050.png')
plt.close()

spark.stop()