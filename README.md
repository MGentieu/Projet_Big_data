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
