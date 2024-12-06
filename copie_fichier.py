import os
from kaggle.api.kaggle_api_extended import KaggleApi

# Authentification via la clé API
api = KaggleApi()
api.authenticate()

# Téléchargement du dataset
dataset_name = "berkeleyearth/climate-change-earth-surface-temperature-data"
output_path = "climate_data"  # Répertoire local où les fichiers seront stockés

api.dataset_download_files(dataset_name, path=output_path, unzip=True)

print(f"Dataset téléchargé et décompressé dans : {os.path.abspath(output_path)}")

