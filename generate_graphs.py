import duckdb
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

# Créer le dossier de sortie s'il n'existe pas
os.makedirs("outputs/graphs", exist_ok=True)

# Connexion à DuckDB
con = duckdb.connect("pipeline.duckdb")

# === 1. Graph: High Value Clients by Hour ===
df1 = con.execute("SELECT * FROM main.high_value_by_hour ORDER BY pickup_hour").fetchdf()

plt.figure(figsize=(10,6))
plt.bar(df1['pickup_hour'], df1['potential_high_value_clients'], color='skyblue')
plt.title("Nombre de clients à haute valeur par heure")
plt.xlabel("Heure")
plt.ylabel("Nombre de clients")
plt.xticks(range(0, 24))
plt.grid(axis='y')
plt.tight_layout()
plt.savefig("outputs/graphs/high_value_by_hour.png")
plt.close()

# === 2. Graph: Tip % by Weather Condition (PIE CHART - Soft Boys Colors) ===
df2 = con.execute("SELECT * FROM main.tip_by_weather").fetchdf()

soft_boy_colors = [
    "#A2D2FF", "#B5EAD7", "#C6DEF1", "#D0F4DE", "#F6F6F6", "#AED9E0", "#C1FBA4"
]

plt.figure(figsize=(8, 8))
plt.pie(
    df2["avg_tip_percentage"],
    labels=df2["weather_condition"],
    autopct='%1.1f%%',
    colors=soft_boy_colors[:len(df2)],
    startangle=140
)
plt.title("Répartition du pourboire moyen par condition météo", fontsize=14)
plt.axis("equal")
plt.tight_layout()
plt.savefig("outputs/graphs/tip_by_weather_pie.png")
plt.close()


# === 3. Graph: Trip Behavior by Weather ===
df3 = con.execute("SELECT * FROM main.behaviour_by_weather").fetchdf()

plt.figure(figsize=(10,6))
sns.barplot(x="weather_condition", y="total_trips", data=df3)
plt.title("Nombre de trajets par condition météo")
plt.xlabel("Condition météo")
plt.ylabel("Nombre de trajets")
plt.xticks(rotation=30)
plt.tight_layout()
plt.savefig("outputs/graphs/trips_by_weather.png")
plt.close()

print(" Tous les graphiques ont été générés dans outputs/graphs/")