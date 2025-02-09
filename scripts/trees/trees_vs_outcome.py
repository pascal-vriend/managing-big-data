"""
Running on Spark takes approximately 1 minute
This file analyses and plots the trees dataset against the outcome of the car accidents
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import seaborn as sns
import matplotlib.pyplot as plt

spark = SparkSession.builder.appName("TreesAnalysis").getOrCreate()

path = "/user/s2121182/tree_dataset"
df = spark.read.csv(path, header=True, inferSchema=True)


tree_columns = [
    "bomen_O_AF_KL_1", "bomen_O_AF_KL_2", "bomen_O_AF_KL_3",
    "bomen_O_AF_KL_4", "bomen_O_AF_KL_5", "bomen_O_AF_KL_6", "bomen_O_AF_KL_7"
]

aggregated_df = df.groupBy("AP3_CODE").agg(*[F.avg(col).alias(col) for col in tree_columns])

for col_name in tree_columns:
    aggregated_df = aggregated_df.withColumnRenamed(f"sum({col_name})", col_name)

pandas_df = aggregated_df.toPandas()

heatmap_data = pandas_df.set_index("AP3_CODE")[tree_columns]

column_names = [
    "0-0.8m", "0.8-1.5m", "1.5-2.5m", "2.5-4.5m", "4.5-6m", "6-7.55m", "7.55-10m"
]
row_names = [
    "Injury", "Material damage only", "Deadly"
]

heatmap_data.columns = column_names
heatmap_data.index = row_names

plt.figure(figsize=(12, 8))
sns.heatmap(
    heatmap_data,
    cmap="YlGnBu",
    annot=True,
    fmt="g",
    cbar_kws={'label': 'Average number of Trees'}
)
plt.title("Heatmap of trees by distance and outcome of a traffic accident")
plt.xlabel("Average number of trees within a certain distance")
plt.ylabel("Outcome of a traffic accident")
plt.xticks(rotation=45)
plt.tight_layout()

nfs_path = "heatmap_trees.png"

plt.savefig(nfs_path)
