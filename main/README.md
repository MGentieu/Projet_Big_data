# Comment exécuter correctement le code :

Il est nécessaire d'avoir les fichiers CSV dans le répertoire hdfs:///user/root/projet/
Allez dans hadoop-master et exécutez les commandes suivantes :

**hadoop fs -mkdir projet**

**hadoop fs -put GlobalTemperatures.csv projet**
**hadoop fs -put ... projet** #Pour les autres fichiers.

Une fois que c'est fait, placez-vous dans le dosier "main" et exécutez le fichier main.py avec la commande suivante :

**python main.py**
