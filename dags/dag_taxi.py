import sys
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# Ajouter le chemin vers le dossier scripts
sys.path.append('/opt/airflow/scripts')

# Importer la fonction process_month
from ingestion_taxi import process_month

# Définition des arguments par défaut
default_args = {
    'owner': 'Mohamed',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Définir le DAG
with DAG(
    dag_id='taxi_data_pipeline',
    default_args=default_args,
    description='Ingestion des données taxi vers MinIO',
    schedule_interval=None,  # ou crontab style
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['taxi', 'minio'],
) as dag:

    # Créer une tâche par mois
    for month in [f"{m:02d}" for m in range(1, 13)]:
        PythonOperator(
            task_id=f'ingest_taxi_data_{month}',
            python_callable=process_month,
            op_args=[month],
        )
