import subprocess

def execute_command(command):
    try:
        # Exécuter la commande et capturer les flux stdout et stderr
        process = subprocess.Popen(
            command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True
        )
        stdout, stderr = process.communicate()
        print(f"Sortie : {stdout}")
        print(f"Erreurs : {stderr}")
        print(f"Code de retour : {process.returncode}")
    except Exception as e:
        print(f"Une erreur s'est produite : {e}")

# On transforme à chaque fois l'attribut 'date', que l'on décompose en année, mois, jour :
execute_command("spark-submit --deploy-mode client --master local[2] TransfoDate.py")

# On gère aussi les valeurs manquantes comme les températures pour chaque fichier CSV :
# On change aussi la latitude et la longitude pour donner une valeur réelle, positive si N ou E, négative si O ou S :
execute_command("spark-submit --deploy-mode client --master local[2] fill_missing_GLTBC.py") 
execute_command("spark-submit --deploy-mode client --master local[2] fill_missing_GLTBMC.py")
execute_command("spark-submit --deploy-mode client --master local[2] fill_missing_GT.py")
execute_command("spark-submit --deploy-mode client --master local[2] fill_missing_GLTBS.py") 
execute_command("spark-submit --deploy-mode client --master local[2] fill_missing_GLTBCi2.py") 

# On exécute ces commandes pour déplacer, renommer, et supprimer des fichiers afin d'obtenir à la fin les fichiers traités
# avec les mêmes noms que les fichiers orignaux, cela afin de faciliter le traitement à postériori
execute_command("chmod +x commandes_suppr_fichiers_inutiles.sh")
execute_command("./commandes_suppr_fichiers_inutiles.sh")

# Les données normalisées et nettoyées, on exécute à présent des fichiers afin d'afficher des graphiques et ainsi analyser les tendances :s
execute_command("spark-submit --deploy-mode client --master local[2] GenerationDesGraphiques.py hdfs:///user/root/projet/GlobalLandTemperaturesByCountry.csv")

#Machine learning lineaire avec Mlib
execute_command("spark-submit --deploy-mode client --master local[2] IA_regression_lineaire_par_année.py")
execute_command("spark-submit --deploy-mode client --master local[2] IA_regression_lineaire_pays_date.py")

#Machine learning logistique avec Mlib
execute_command("spark-submit --deploy-mode client --master local[2] IA_regression_logistique_tunisie.py.py")
execute_command("spark-submit --deploy-mode client --master local[2] logistique.py")