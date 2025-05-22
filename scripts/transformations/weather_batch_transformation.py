from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, year, month, dayofmonth
import os

# Initialisation Spark
spark = SparkSession.builder \
    .appName("Weather Batch Processing") \
    .config("spark.jars", "./jars/postgresql-42.7.1.jar") \
    .getOrCreate()

# Répertoire des fichiers JSON
data_dir = "data/weather"
all_files = [os.path.join(data_dir, f) for f in os.listdir(data_dir) if f.endswith(".json")]

# Lire les fichiers un par un et unionner
df_final = None
for file in all_files:
    df_temp = spark.read.option("multiline", "true").json(file)
    if df_final is None:
        df_final = df_temp
    else:
        df_final = df_final.unionByName(df_temp)

# Enrichissement
df_final = df_final.withColumn("datetime", to_timestamp("datetime")) \
    .withColumn("year", year("datetime")) \
    .withColumn("month", month("datetime")) \
    .withColumn("day", dayofmonth("datetime"))

# Envoi vers PostgreSQL
df_final.write \
  .format("jdbc") \
  .option("url", "jdbc:postgresql://localhost:5433/taxi_weather_dwh") \
  .option("dbtable", "dim_weather") \
  .option("user", "postgres") \
  .option("password", "password123") \
  .option("driver", "org.postgresql.Driver") \
  .mode("overwrite") \
  .save()
