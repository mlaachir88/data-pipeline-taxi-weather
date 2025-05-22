import os
import requests
from minio import Minio
from dotenv import load_dotenv

# Charger les variables d’environnement depuis .env
load_dotenv()

# --- CONFIGURATION ---
MONTHS = [f"{m:02d}" for m in range(1, 13)]  # ["01", "02", ..., "12"]
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
MINIO_BUCKET = "taxi-weather"

# --- Client MinIO ---
minio_client = Minio(
    endpoint=os.getenv("MINIO_ENDPOINT", "localhost:9000"),
    access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
    secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
    secure=False
)

# --- Créer le dossier local si absent ---
os.makedirs("./data", exist_ok=True)

# --- Fonction de traitement d’un mois ---
def process_month(month):
    filename = f"yellow_tripdata_2023-{month}.parquet"
    local_path = f"./data/{filename}"
    url = f"{BASE_URL}/{filename}"
    minio_path = f"taxi/{filename}"

    # Étape 1 : Téléchargement
    if not os.path.exists(local_path):
        print(f" Téléchargement de {filename}...")
        response = requests.get(url)
        if response.status_code == 200:
            with open(local_path, "wb") as f:
                f.write(response.content)
            print(f" Fichier téléchargé : {filename}")
        else:
            print(f" Échec du téléchargement pour {filename} (code {response.status_code})")
            return
    else:
        print(f" Déjà présent localement : {filename}")

    # Étape 2 : Upload vers MinIO
    if not minio_client.bucket_exists(MINIO_BUCKET):
        minio_client.make_bucket(MINIO_BUCKET)
        print(f" Bucket créé : {MINIO_BUCKET}")

    print(f" Upload vers MinIO : {minio_path} ...")
    minio_client.fput_object(
        MINIO_BUCKET,
        minio_path,
        local_path,
        content_type="application/octet-stream"
    )
    print(f" Upload terminé pour : {filename}")

# --- Boucle principale ---
if __name__ == "__main__":
    for month in MONTHS:
        process_month(month)
