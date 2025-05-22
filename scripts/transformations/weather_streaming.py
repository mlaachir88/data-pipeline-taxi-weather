import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

# Configuration PySpark
os.environ["PYSPARK_PYTHON"] = "venv/Scripts/python.exe"

spark = SparkSession.builder \
    .appName("Weather Streaming") \
    .config("spark.jars", "./jars/postgresql-42.7.1.jar") \
    .getOrCreate()

# Schéma attendu (adapter si nécessaire)
schema = """
    datetime STRING,
    weather_id STRING,
    city STRING,
    location STRUCT<lat: DOUBLE, lon: DOUBLE>,
    temperature DOUBLE,
    feels_like DOUBLE,
    humidity INT,
    wind_speed DOUBLE,
    condition STRING,
    pressure INT,
    precipitation DOUBLE,
    visibility INT
"""

# Lecture en streaming (micro-batch) des fichiers JSON
df = spark.readStream \
    .schema(schema) \
    .json("data/weather/")

# Nettoyage + enrichissement
df_cleaned = df.select(
    to_timestamp("datetime").alias("datetime"),
    col("weather_id"),
    col("city"),
    col("location.lat").alias("latitude"),
    col("location.lon").alias("longitude"),
    "temperature",
    "feels_like",
    "humidity",
    "wind_speed",
    "condition",
    "pressure",
    "precipitation",
    "visibility"
)

# Fonction d’envoi vers PostgreSQL
def write_to_postgres(batch_df, batch_id):
    batch_df.write.jdbc(
        url="jdbc:postgresql://localhost:5433/taxi_weather_dwh",
        table="dim_weather",
        mode="append",
        properties={
            "user": "postgres",
            "password": "password123",
            "driver": "org.postgresql.Driver"
        }
    )

# Lancement du stream avec trigger 30 secondes
query = df_cleaned.writeStream \
    .foreachBatch(write_to_postgres) \
    .trigger(processingTime="30 seconds") \
    .outputMode("append") \
    .start()

query.awaitTermination()