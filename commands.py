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


# Exemple d'utilisation
execute_command("spark-submit --deploy-mode client --master local[2] fill_missing_GLTBC.py")  # Remplacez par la commande que vous voulez exécuter

execute_command("spark-submit --deploy-mode client --master local[2] fill_missing_GLTBMC.py")

execute_command("spark-submit --deploy-mode client --master local[2] TransfoDate.py")

execute_command("spark-submit --deploy-mode client --master local[2] creation_datasets_exploitables.py")

execute_command("spark-submit --deploy-mode client --master local[2] genere_graphiques.py hdfs:///user/root/projet/AvgTempByCo.csv")
