#!/bin/bash

# Créer le dossier cible
hadoop fs -mkdir -p P

# Déplacer les fichiers
hadoop fs -rm projet/GLTBCi.csv
hadoop fs -rm projet/GLTBCo.csv
hadoop fs -rm projet/GLTBMC.csv
hadoop fs -rm projet/GLTBS.csv
hadoop fs -rm projet/GT.csv
hadoop fs -mv projet/GlobalLandTemperaturesByCity.csv P/GlobalLandTemperaturesByCity.csv
hadoop fs -mv projet/GlobalLandTemperaturesByCountry.csv P/GlobalLandTemperaturesByCountry.csv
hadoop fs -mv projet/GlobalLandTemperaturesByMajorCity.csv P/GlobalLandTemperaturesByMajorCity.csv
hadoop fs -mv projet/GlobalLandTemperaturesByState.csv P/GlobalLandTemperaturesByState.csv
hadoop fs -mv projet/GlobalTemperatures.csv P/GlobalTemperatures.csv
hadoop fs -mv projet/GLTBCi2.csv projet/GlobalLandTemperaturesByCity.csv
hadoop fs -mv projet/GLTBC2.csv projet/GlobalLandTemperaturesByCountry.csv
hadoop fs -mv projet/GLTBMC2.csv projet/GlobalLandTemperaturesByMajorCity.csv
hadoop fs -mv projet/GLTBS2.csv projet/GlobalLandTemperaturesByState.csv
hadoop fs -mv projet/GT2.csv projet/GlobalTemperatures.csv
