from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc
import matplotlib.pyplot as plt
import numpy as np

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Big Data Methods - Trees and Accidents") \
    .getOrCreate()

# File paths
ongevallen_path = "/user/s2739046/MBD/ongevallen.csv"
bomen_path = "/user/s2563363/FilesProject/bomen.csv"

# Load data
df_ongevallen = spark.read.csv(ongevallen_path, header=True, inferSchema=True)
df_bomen = spark.read.csv(bomen_path, header=True, inferSchema=True, sep=";")

# Filter accidents for the year 2023 and group by WVK_ID
df_ongevallen_2023 = (
    df_ongevallen.filter(col("JAAR_VKL") == 2023)
    .select("WVK_ID", "VKL_NUMMER")
    .groupBy("WVK_ID")
    .agg(count("*").alias("aantal_ongelukken"))
)

# Select relevant columns from bomen dataset and rename for consistency
df_bomen = df_bomen.select(col("WEGVAK_ID").alias("WVK_ID"), "AANT_BOMEN")

# Join dataframes on WVK_ID
df_join = df_bomen.join(df_ongevallen_2023, "WVK_ID")

# Sample a fraction of the data (e.g., 10% of the dataset)
sample_fraction = 0.1 # Adjust this fraction as needed
df_sampled = df_join.sample(withReplacement=False, fraction=sample_fraction)

# Collect the sampled data for plotting
sampled_data = df_sampled.collect()

# Prepare data for plotting
x_points = [row["AANT_BOMEN"] for row in sampled_data]
y_points = [row["aantal_ongelukken"] for row in sampled_data]

# Scatter plot with sampled data
plt.scatter(x_points, y_points, alpha=0.8)  # Add transparency for better visibility
plt.title("Trees and Accidents")
plt.xlabel("Number of Trees")
plt.ylabel("Number of Accidents")
plt.grid(True)
output_path = "bomen_en_ongelukken_sampled.png"
plt.savefig(output_path, dpi=300)
print(f"Grafiek opgeslagen als: {output_path}")

# Stop SparkSession
spark.stop()
