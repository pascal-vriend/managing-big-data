"""
This Python file creates a heatmap that combines the light situation and the outcome of the car accident
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col
import seaborn as sns
import matplotlib.pyplot as plt

spark = SparkSession.builder.appName("LightSightOutcomeAnalysis").getOrCreate()

path = "/user/s2121182/tree_dataset"
df = spark.read.csv(path, header=True, inferSchema=True)
df_filtered = df.filter(col("LGD_ID").isNotNull() & col("AP3_CODE").isNotNull())

light_situation_mapping = {
    1: "Daylight",
    2: "Night",
    3: "Dusk"
}

severity_outcome_mapping = {
    "DOD": "Deadly",
    "LET": "Injury",
    "UMS": "Material damage only"
}

df_filtered = df_filtered.withColumn(
    "light_situation",
    F.when(col("LGD_ID") == 1, "Daylight")
     .when(col("LGD_ID") == 2, "Night")
     .when(col("LGD_ID") == 3, "Dusk")
     .otherwise("Unknown")
)

df_filtered = df_filtered.withColumn(
    "severity",
    F.when(col("AP3_CODE") == "DOD", "Deadly")
     .when(col("AP3_CODE") == "LET", "Injury")
     .when(col("AP3_CODE") == "UMS", "Material damage only")
     .otherwise("Unknown")
)

df_count = df_filtered.groupBy("light_situation", "severity").agg(F.count("*").alias("count"))

total_by_severity = df_filtered.groupBy("severity").agg(F.count("*").alias("total_severity_count"))

df_with_totals = df_count.join(total_by_severity, on="severity")

df_with_totals = df_with_totals.withColumn(
    "percentage",
    (F.col("count") / F.col("total_severity_count")) * 100
)

pandas_df = df_with_totals.toPandas()

heatmap_data = pandas_df.pivot(index="light_situation", columns="severity", values="percentage")

plt.figure(figsize=(8, 6))
sns.heatmap(heatmap_data, annot=True, cmap="YlGnBu", fmt=".2f", cbar_kws={'label': 'Percentage (%)'})
plt.title("Percentage of Severity Cases by Light Situation")
plt.xlabel("Severity")
plt.ylabel("Light Situation")
plt.tight_layout()


nfs_path = "light_situation.png"
plt.savefig(nfs_path)

print(f"Heatmap saved to {nfs_path}")
