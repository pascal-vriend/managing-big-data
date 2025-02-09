from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import numpy as np

spark = SparkSession.builder.getOrCreate()
df = spark.read.csv("/user/s2739046/MBD/ongevallen.csv", header=True, inferSchema=True)

grouped_df = df.groupBy("JAAR_VKL", "AP3_CODE").count()

grouped_dict = {}
for row in grouped_df.collect():
    year, ap3_code, count = row["JAAR_VKL"], row["AP3_CODE"], row["count"]
    if year not in grouped_dict:
        grouped_dict[year] = {"DOD": 0, "LET": 0, "UMS": 0}
    grouped_dict[year][ap3_code] = count

sorted_years = sorted(grouped_dict.keys())
dods = [grouped_dict[year]["DOD"] for year in sorted_years]
lets = [grouped_dict[year]["LET"] for year in sorted_years]
umss = [grouped_dict[year]["UMS"] for year in sorted_years]

x = np.arange(len(sorted_years))
bar_width = 0.2

plt.figure(figsize=(14, 8))

plt.bar(x - bar_width, dods, bar_width, label="Deadly", color='red')
plt.bar(x, lets, bar_width, label="Injury", color='orange')
plt.bar(x + bar_width, umss, bar_width, label="Material Damage", color='green')

plt.xlabel("Year", fontsize=12)
plt.ylabel("Amount", fontsize=12)
plt.title("Accidents Per Year With Outcome", fontsize=16)
plt.xticks(x, sorted_years, rotation=45)
plt.legend(title="Outcome", fontsize=10)

plt.tight_layout()
plt.savefig("accidents_per_year_with_outcome_bar_chart.png", bbox_inches='tight')
