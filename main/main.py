import subprocess
#python main.py
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


# 3. Traitement des Données avec Spark
execute_command("spark-submit --deploy-mode client --master local[2] TransfoDate.py")

execute_command("spark-submit --deploy-mode client --master local[2] fill_missing_GLTBC.py") 
execute_command("spark-submit --deploy-mode client --master local[2] fill_missing_GLTBMC.py")
execute_command("spark-submit --deploy-mode client --master local[2] fill_missing_GT.py")
execute_command("spark-submit --deploy-mode client --master local[2] fill_missing_GLTBS.py") 
execute_command("spark-submit --deploy-mode client --master local[2] fill_missing_GLTBCi2.py") 

execute_command("chmod +x commandes_suppr_fichiers_inutiles.sh")
execute_command("./commandes_suppr_fichiers_inutiles.sh")

#Visualisation graphs



#Machine learning avec Mlib




#execute_command("spark-submit --deploy-mode client --master local[2] creation_datasets_exploitables.py")

#execute_command("spark-submit --deploy-mode client --master local[2] genere_graphiques.py hdfs:///user/root/projet/AvgTempByCo.csv")

