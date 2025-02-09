"""
This file analyses and plots trees vs outcome.
"""
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
from pyspark.sql import functions as F
import numpy as np

spark = SparkSession.builder.appName("LightSightOutcomeAnalysis").getOrCreate()

path = "/user/s2121182/tree_dataset"
df = spark.read.csv(path, header=True, inferSchema=True)

df_filtered = df.filter(F.col("LGD_ID").isNotNull() & F.col("AP3_CODE").isNotNull())

tree_columns = [
    "bomen_O_AF_KL_1", "bomen_O_AF_KL_2", "bomen_O_AF_KL_3",
    "bomen_O_AF_KL_4", "bomen_O_AF_KL_5", "bomen_O_AF_KL_6", "bomen_O_AF_KL_7"
]

aggregated_df = df_filtered.groupBy("LGD_ID", "AP3_CODE").agg(
    *[F.avg(col).alias(col) for col in tree_columns]
)

pandas_df = aggregated_df.toPandas()

aggregated_df.show()

heatmap_data = pandas_df.set_index(["LGD_ID", "AP3_CODE"])[tree_columns]

Z = heatmap_data.values
X = np.arange(Z.shape[0])
Y = np.arange(Z.shape[1])

X, Y = np.meshgrid(X, Y)

x_flat = X.flatten()
y_flat = Y.flatten()
z_flat = Z.flatten()

fig = plt.figure(figsize=(10, 8))
ax = fig.add_subplot(111, projection='3d')
sc = ax.scatter(x_flat, y_flat, z_flat, c=z_flat, cmap='YlGnBu', marker='o')
ax.set_xlabel('Light Situation')
ax.set_ylabel('Severity')
ax.set_zlabel('Average Number of Trees')

light_situation_labels = pandas_df["LGD_ID"].unique()
severity_labels = pandas_df["AP3_CODE"].unique()

ax.set_xticks(np.arange(len(light_situation_labels)))
ax.set_xticklabels(light_situation_labels)

ax.set_yticks(np.arange(len(severity_labels)))
ax.set_yticklabels(severity_labels)

cbar = fig.colorbar(sc)
cbar.set_label('Average Number of Trees')

plt.title("3D Heatmap of Trees, Light Situation, and Severity")

nfs_path = "trees_light_severity.png"
plt.savefig(nfs_path)


