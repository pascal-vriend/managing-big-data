from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, row_number
from pyspark.sql.window import Window
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

spark = SparkSession.builder \
    .appName("Top 5 Gemeentes per Provincie") \
    .getOrCreate()

hdfs_path = "/user/s2739046/MBD/ongevallen.csv"
df = spark.read.csv(hdfs_path, header=True, inferSchema=True)

df = df.filter(col("JAAR_VKL") == 2023)

provincie_gemeente_counts = df.groupBy("PVE_NAAM", "GME_NAAM") \
    .agg(count("GME_NAAM").alias("aantal_ongelukken"))

window_spec = Window.partitionBy("PVE_NAAM").orderBy(col("aantal_ongelukken").desc())
provincie_gemeente_ranking = provincie_gemeente_counts \
    .withColumn("rank", row_number().over(window_spec)) \
    .filter(col("rank") <= 5)

provincie_gemeente_pd = provincie_gemeente_ranking.toPandas()

provincie_gemeente_pd = provincie_gemeente_pd.sort_values(by=["PVE_NAAM", "rank"])

totale_ongelukken_per_provincie = provincie_gemeente_pd.groupby("PVE_NAAM")["aantal_ongelukken"].sum()

provincies = totale_ongelukken_per_provincie.sort_values(ascending=False).index.tolist()

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(18, 8), gridspec_kw={'width_ratios': [2, 1]})

# --- BAR CHART ---
x_labels = []
x_positions = []
colors = plt.cm.tab20(np.linspace(0, 1, len(provincies)))
current_position = 0

for i, provincie in enumerate(provincies):
    provincie_data = provincie_gemeente_pd[provincie_gemeente_pd["PVE_NAAM"] == provincie]
    gemeenten = provincie_data["GME_NAAM"].tolist()
    counts = provincie_data["aantal_ongelukken"].tolist()

    positions = range(current_position, current_position + len(gemeenten))
    ax1.bar(positions, counts, color=colors[i], label=provincie)

    for pos, count, gemeente in zip(positions, counts, gemeenten):
        ax1.text(pos, count + 1, gemeente, ha='center', va='bottom', fontsize=9, rotation=90)

    mid_position = np.mean(positions)
    x_labels.append(provincie)
    x_positions.append(mid_position)

    current_position += len(gemeenten) + 1  # Extra ruimte tussen provincies

ax1.set_xticks(x_positions)
ax1.set_xticklabels(x_labels, rotation=45, ha='right', fontsize=10)

ax1.set_title("Top 5 Municipalities With Most Accidents Per Province (2023)", fontsize=16)
ax1.set_ylabel("Accident Count", fontsize=12)
ax1.set_xlabel("Provinces", fontsize=12)
y_max = provincie_gemeente_pd["aantal_ongelukken"].max()
ax1.set_ylim(0, y_max * 1.2)  # Increase the upper limit by 20%

# --- PIE CHART ---
ax2.pie(
    totale_ongelukken_per_provincie.sort_values(ascending=False),
    labels=totale_ongelukken_per_provincie.sort_values(ascending=False).index,
    rotatelabels=True,
    autopct='%1.1f%%',
    colors=colors,
    startangle=90,
    textprops={'fontsize': 10},
    labeldistance=1
)
ax2.set_title(" Total Accident Count Per Province (2023)", fontsize=14, pad=20)

plt.tight_layout()
output_path = "top5_gemeentes_met_cirkeldiagram_gesorteerd.png"
plt.savefig(output_path, dpi=300)
plt.show()

print(f"Grafiek opgeslagen als: {output_path}")
