import duckdb

# Crée la base DuckDB (fichier pipeline.duckdb)
con = duckdb.connect("pipeline.duckdb")

# Table fact_taxi_trips à partir des fichiers Parquet taxi
con.execute("""
    CREATE OR REPLACE TABLE fact_taxi_trips AS
    SELECT * FROM parquet_scan('data/yellow_tripdata_2023-*.parquet');
""")
print(" Table fact_taxi_trips créée avec succès.")

# Table dim_weather à partir des fichiers Parquet météo
con.execute("""
    CREATE OR REPLACE TABLE dim_weather AS
    SELECT * FROM parquet_scan('data/weather_parquet/*.parquet');
""")
print(" Table dim_weather créée avec succès.")

# Fermer la connexion proprement
con.close()
print(" Base DuckDB initialisée (pipeline.duckdb)")
