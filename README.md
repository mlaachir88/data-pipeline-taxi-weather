🚀 Data Pipeline - Yellow Taxi & Weather Analytics

Bienvenue dans ce projet de pipeline de données qui combine des trajets de taxis Yellow Cab à New York et des données météo pour produire des analyses avancées.

🌐 Objectif

Mettre en place un pipeline complet pour :

Ingestion batch (Yellow Taxi)

Ingestion streaming (Weather API)

Transformations via PySpark & Spark Streaming

Modélisation et analyse avec dbt

Visualisation automatique des résultats avec Python

🧱 Stack Technique

Airflow → orchestration

PySpark / Spark Streaming → transformation

DuckDB → entrepôt analytique

DBT → modélisation

Python / Matplotlib / Seaborn → visualisation

📂 Structure du projet

.
├── dags/                      # DAGs Airflow
├── data/                     # Données brut (parquet, json)
├── dbt_project/              # Projet DBT complet
├── scripts/                  # Scripts Python (ingestion, transformation)
├── outputs/graphs/           # Graphiques auto-générés
├── notebooks/                # (optionnel) Analyse Jupyter
├── generate_graphs.py        # Script Python principal d'analyse

📊 Modèles DBT Clés

fact_taxi_trips_parquet : base transformée des trajets taxi

dim_weather_parquet : dimension météo avec enrichissement

high_value_by_hour : nombre de clients à haute valeur par heure

tip_by_weather : pourboire moyen selon la météo

behaviour_by_weather : comportement des trajets selon météo

 Analyse des Résultats

1. Nombre de clients à haute valeur par heure



Les clients à haute valeur sont les plus nombreux entre 17h et 20h. C'est le moment idéal pour maximiser les revenus.

2. Pourboire moyen (%) par condition météo



Les pourcentages de pourboires sont similaires entre les conditions. Légère augmentation par temps orageux ou nuageux.

3. Nombre de trajets par condition météo



La neige entraîne le plus grand nombre de trajets, probablement à cause de l'inconfort à pied.

 Lancer l'analyse automatiquement

python generate_graphs.py

Tous les graphiques sont sauvegardés dans outputs/graphs/

🎓 Auteur

Mohamed LaachirMaster 1 Data EngineeringSUPINFO / 4DATADEV 2025

© Projet réalisé dans le cadre de l'évaluation finale du module Data Engineering.