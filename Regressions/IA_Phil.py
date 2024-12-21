import pandas as pd
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

# Chargement du dataset
df = pd.read_csv('../datasets/GlobalLandTemperaturesByCountry.csv')

# Filtrer les données pour la Tunisie
df_tunisia = df[df['Country'] == 'Tunisia']

# Convertir la colonne 'dt' en format datetime
df_tunisia['dt'] = pd.to_datetime(df_tunisia['dt'], format='%Y-%m-%d')

# Trier les données par date
df_tunisia = df_tunisia.sort_values('dt')

# Extraire les températures moyennes pour le clustering
temperatures = df_tunisia[['AverageTemperature']].dropna()

# Vérifier la longueur des données après suppression des NaN
print("Longueur des données après suppression des NaN:", len(temperatures))

# Normaliser les données (important pour le K-Means)
scaler = StandardScaler()
temperatures_scaled = scaler.fit_transform(temperatures)

# Appliquer K-Means avec 2 clusters
kmeans = KMeans(n_clusters=3, random_state=42)

# Ajouter la colonne Cluster à df_tunisia en utilisant .loc pour éviter l'avertissement
df_tunisia.loc[temperatures.index, 'Cluster'] = kmeans.fit_predict(temperatures_scaled)

# Visualisation des résultats
plt.figure(figsize=(10, 6))

# Tracer les points avec les couleurs des clusters
plt.scatter(df_tunisia['dt'], df_tunisia['AverageTemperature'], c=df_tunisia['Cluster'], cmap='viridis', s=10)

# Ajouter le titre et les labels
plt.title('Clustering des températures en Tunisie (2 clusters)', fontsize=16)
plt.xlabel('Date', fontsize=12)
plt.ylabel('Température moyenne (°C)', fontsize=12)
plt.grid(True, linestyle='--', alpha=0.6)

# Afficher la légende
plt.legend(title='Cluster')

# Sauvegarder l'image

# Afficher le graphique
plt.show()


# Compter le nombre de valeurs dans chaque cluster
cluster_counts = df_tunisia['Cluster'].value_counts()

# Afficher le nombre de valeurs par cluster
print("Nombre de valeurs par cluster :")
print(cluster_counts)
