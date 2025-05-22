import os
import glob
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, hour, dayofweek, unix_timestamp
from pyspark.sql.types import LongType

# Spécifier l’interpréteur Python si besoin (utile sur Windows en local)
os.environ["PYSPARK_PYTHON"] = r"venv/Scripts/python.exe"

# Créer SparkSession avec le driver PostgreSQL
spark = SparkSession.builder \
    .appName("Taxi Transformation") \
    .config("spark.jars", "./jars/postgresql-42.7.1.jar") \
    .getOrCreate()

# Lire tous les fichiers parquet
parquet_files = glob.glob("./data/yellow_tripdata_2023-*.parquet")
dfs = []

for file in parquet_files:
    df = spark.read.parquet(file)
    if "VendorID" in df.columns:
        df = df.withColumn("VendorID", col("VendorID").cast(LongType()))
    dfs.append(df)

df = dfs[0]
for other_df in dfs[1:]:
    df = df.unionByName(other_df, allowMissingColumns=True)

# Transformations
df_transformed = df \
    .withColumn("trip_duration_min", (unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 60) \
    .withColumn("distance_category", when(col("trip_distance") <= 2, "0-2km")
                                  .when(col("trip_distance") <= 5, "2-5km")
                                  .otherwise(">5km")) \
    .withColumn("tip_percentage", (col("tip_amount") / col("fare_amount")) * 100) \
    .withColumn("pickup_hour", hour(col("tpep_pickup_datetime"))) \
    .withColumn("pickup_weekday", dayofweek(col("tpep_pickup_datetime")))

df_final = df_transformed.select(
    "VendorID", "passenger_count", "trip_distance", "distance_category",
    "tip_amount", "fare_amount", "tip_percentage", "payment_type",
    "pickup_hour", "pickup_weekday", "trip_duration_min",
    "tpep_pickup_datetime", "tpep_dropoff_datetime"
)

#  Connexion PostgreSQL
POSTGRES_URL = "jdbc:postgresql://localhost:5433/airflow"
POSTGRES_PROPERTIES = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver"
}

# Sauvegarde dans PostgreSQL
df_final.write.jdbc(
    url=POSTGRES_URL,
    table="fact_taxi_trips",
    mode="overwrite",
    properties=POSTGRES_PROPERTIES
)

print(" Données taxi enregistrées dans PostgreSQL.")
