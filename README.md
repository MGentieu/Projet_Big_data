# Projet_Big_data

Commandes à taper dans le terminal de docker :

docker start hadoop-master hadoop-slave1 hadoop-slave2

docker exec -it hadoop-master bash

./start-hadoop.sh


**#Installons maintenant git sur notre hadoop-master :**

apt-get update

apt-get install -y git


**#Cliquez ensuite sur votre compte, allez dans "settings", puis dans "developper settings"**

**#Allez dans dédiés aux tokens classiques, et créez-en un nouveau. Attention ensuite à bien mettre les droits suffisants et à sauvegarder le token généré.**

**#Enfin, exécutez l'instruction suivante pour cloner le projet :**

git clone https://github.com/MGentieu/Projet_Big_data.git

**Rentrez ensuite votre identifiant github, et mettez le token généré précédemment à la place du mot de passe. Et voilà !**

**Afin de télécharger les datasets, on importe la bibliothèque kaggle avec l'instruction :**

pip install kaggle

**il faut à présent créer une API kaggle (instructions chatGPT) pour générer un token afin de pouvoir directement vous connecter à votre compte kaggle depuis Docker**

**Mettez le fichier "kaggle.json" dans le répertoire kaggle avec l'instruction suivante :**

docker cp "chemin_vers_fichier\kaggle.json" hadoop-master:/root/.config/kaggle/

**Maintenant, on télécharge le dataset en lançant dans le répertoire de hadoop-master le fichier python "copie_fichier.py"**

**Pour mettre le fichier dans le hadoop fs, on se place dans le répertoire contenant le dossier _climate_data_ et on exécute ensuite les instructions suivantes :**

hadoop fs -mkdir dossier_projet

hadoop fs -put climate_data /dossier_projet
