"""
This Python file analyses the middenberm breedte against the outcome of the accident
"""
from pyspark.sql import SparkSession
import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd

spark = SparkSession.builder.appName("MiddenbermAnalysis").getOrCreate()

path = "/user/s2121182/middenberm_dataset"
df_spark = spark.read.csv(path, header=True, inferSchema=True)

chunks = df_spark.randomSplit([0.2] * 5 , seed=42)
chunks_pd = [chunk.toPandas() for chunk in chunks]
df = pd.concat(chunks_pd, ignore_index=True)

middenberm_columns = [
    "middenberm_breedte_SOORT", "middenberm_breedte_BREEDTE"
]

ap3_ames = [
    "Injury", "Material damage only", "Deadly"
]


df["AP3_CODE"] = df["AP3_CODE"].astype(str)

plt.figure(figsize=(8, 6))
sns.boxplot(
    data=df,
    x="AP3_CODE",  # x-axis: AP3_CODE
    y="middenberm_breedte_BREEDTE",  # y-axis: middenberm_breedte_BREEDTE
)
plt.title("Box Plot of Middenberm Breedte BREEDTE by AP3 Code")
plt.xlabel("AP3 Code")
plt.ylabel("Middenberm Breedte BREEDTE")
plt.grid(True)
plt.savefig("middenberm_boxplot.png")
plt.show()

plt.savefig("middenberm_boxplot.png")

print(f"Heatmap saved to middenberm_boxplot.png")

accidents_per_soort = df['middenberm_breedte_SOORT'].value_counts().reset_index()
accidents_per_soort.columns = ['middenberm_breedte_SOORT', 'Number of Accidents']

plt.figure(figsize=(8, 6))
sns.barplot(
    x='middenberm_breedte_SOORT',
    y='Number of Accidents',
    data=accidents_per_soort,
    palette='viridis'
)

plt.title('Number of Accidents per Middenberm Breedte SOORT')
plt.xlabel('Middenberm Breedte SOORT')
plt.ylabel('Number of Accidents')
plt.grid(True)
plt.savefig("middenberm_counts.png")

print(f"Heatmap saved to middenberm_counts.png")
