import os
import json
from datetime import datetime, timedelta
from random import uniform, randint, choice
from minio import Minio
from dotenv import load_dotenv

# Charger les variables d’environnement depuis .env
load_dotenv()

# --- CONFIGURATION ---
MINIO_BUCKET = "taxi-weather"
CITY = "New York"
LAT, LON = 40.7128, -74.0060
MINIO_CLIENT = Minio(
    endpoint=os.getenv("MINIO_ENDPOINT", "localhost:9000"),
    access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
    secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
    secure=False
)

# Créer les dossiers
os.makedirs("./data/weather", exist_ok=True)

# Liste de conditions météo aléatoires
CONDITIONS = ["Clear", "Clouds", "Rain", "Snow", "Fog", "Thunderstorm"]

# Génère un faux échantillon météo
def generate_fake_weather(ts: datetime) -> dict:
    return {
        "datetime": ts.strftime("%Y-%m-%dT%H:%M:%S"),
        "weather_id": ts.strftime("%Y-%m-%d_%H"),
        "city": CITY,
        "location": {"lat": LAT, "lon": LON},
        "temperature": round(uniform(-5, 35), 2),
        "feels_like": round(uniform(-7, 33), 2),
        "humidity": randint(20, 100),
        "wind_speed": round(uniform(0, 20), 2),
        "condition": choice(CONDITIONS),
        "pressure": randint(980, 1040),
        "precipitation": round(uniform(0, 10), 2),
        "visibility": randint(5000, 10000)
    }

# Upload vers MinIO
def upload_weather(ts: datetime):
    data = generate_fake_weather(ts)
    filename = f"{ts.strftime('%Y-%m-%d_%H')}.json"
    local_path = f"./data/weather/{filename}"
    minio_path = f"weather/{filename}"

    # Sauvegarde locale JSON
    with open(local_path, "w") as f:
        json.dump(data, f, indent=2)
    print(f" Fichier généré : {local_path}")

    # Upload dans le bucket
    if not MINIO_CLIENT.bucket_exists(MINIO_BUCKET):
        MINIO_CLIENT.make_bucket(MINIO_BUCKET)
        print(f" Bucket créé : {MINIO_BUCKET}")

    MINIO_CLIENT.fput_object(
        MINIO_BUCKET,
        minio_path,
        local_path,
        content_type="application/json"
    )
    print(f" Upload terminé sur MinIO : {minio_path}")

# Boucle principale : simulation 1 par jour à 13h (mois 2023)
if __name__ == "__main__":
    for month in range(1, 13):
        ts = datetime(2023, month, 15, 13, 0, 0)
        upload_weather(ts)
