import sys
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Ajouter le chemin vers le dossier scripts
sys.path.append('/opt/airflow/scripts')

# Importer la fonction depuis le script météo
from ingestion_weather import upload_weather

# Définir les arguments par défaut
default_args = {
    'owner': 'Mohamed',
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

# Créer le DAG
with DAG(
    dag_id='weather_data_pipeline',
    default_args=default_args,
    description='Ingestion des données météo simulées dans MinIO',
    schedule_interval=None,  # À déclencher manuellement pour chaque test
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['weather', 'minio'],
) as dag:

    # Créer une tâche par mois (heure fixe à 13h pour chaque 15 du mois)
    for month in range(1, 13):
        task = PythonOperator(
            task_id=f'ingest_weather_data_2023_{month:02d}',
            python_callable=upload_weather,
            op_args=[datetime(2023, month, 15, 13, 0, 0)],
        )