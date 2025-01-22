import os
import requests
import zipfile
import io
import pandas as pd
from datetime import datetime

# URL de l'API
url = "https://transport.data.gouv.fr/api/datasets/64635525318cc75a9a8a771f"

# Requête pour obtenir les données JSON
response = requests.get(url)
if response.status_code == 200:
    data = response.json()
else:
    print(f"Erreur : {response.status_code}")
    data = None

# Créer le dossier "data/" s'il n'existe pas
output_dir = "data"
os.makedirs(output_dir, exist_ok=True)

# Vérifier que le JSON contient des données
if data:
    # Parcourir "history" pour extraire les URLs et métadonnées des fichiers
    for entry in data["history"]:
        payload = entry.get("payload", {})
        zip_metadata = payload.get("zip_metadata", [])
        resource_url = payload.get("resource_latest_url")

        print(f"Téléchargement depuis : {resource_url}")

        # Télécharger et traiter les fichiers de l'archive ZIP
        response = requests.get(resource_url)
        if response.status_code == 200:
            with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                for file_meta in zip_metadata:
                    file_name = file_meta["file_name"]
                    last_modified_datetime = file_meta["last_modified_datetime"]

                    # Convertir la date "last_modified_datetime" en format YYYY-MM-DD
                    last_modified_date = datetime.strptime(last_modified_datetime, "%Y-%m-%dT%H:%M:%S").strftime("%Y-%m-%d")

                    print(f"Extraction de {file_name} (modifié le {last_modified_date})")

                    # Charger le fichier dans un DataFrame
                    with z.open(file_name) as file:
                        try:
                            df = pd.read_csv(file) 
                            # Chemin complet du fichier dans le dossier "data/"
                            output_file_path = os.path.join(output_dir, f"{file_name.replace('.txt', '')}_{last_modified_date}.csv")

                            # Sauvegarder le fichier CSV avec la date incluse
                            df.to_csv(output_file_path, index=False)
                            print(f"{file_name} sauvegardé sous {output_file_path}")
                        except Exception as e:
                            print(f"Erreur lors de la lecture de {file_name}: {e}")
        else:
            print(f"Erreur lors du téléchargement : {response.status_code}")

else:
    print("Aucune donnée disponible.")
