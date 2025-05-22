import os
import pandas as pd
import glob

# Créer le dossier de sortie
os.makedirs("data/weather_parquet", exist_ok=True)

# Pour chaque fichier JSON
for file in glob.glob("data/weather/*.json"):
    df = pd.read_json(file)
    filename = os.path.basename(file).replace(".json", ".parquet")
    df.to_parquet(f"data/weather_parquet/{filename}")
    print(f" Converti : {filename}")
