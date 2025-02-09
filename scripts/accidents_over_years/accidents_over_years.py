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

# Load data
df_ongevallen = spark.read.csv(ongevallen_path, header=True, inferSchema=True)

# Select only necessary columns
df_ongevallen_filtered = df_ongevallen.select("WVK_ID", "JAAR_VKL", "VKL_NUMMER")

# Group by WVK_ID and JAAR_VKL, and count the number of accidents
df_accidents_per_year = (df_ongevallen_filtered.groupBy("WVK_ID", "JAAR_VKL").agg(count("*").alias("aantal_ongelukken")))

# Get the top 10 WVK_IDs with the most accidents in 2023
df_ongevallen_2023 = df_ongevallen.filter(col("JAAR_VKL") == 2023)

df_top_wvk_2023 = (
    df_ongevallen_2023.groupBy("WVK_ID")
    .agg(count("*").alias("aantal_ongelukken"))
    .orderBy(desc("aantal_ongelukken"))
    .limit(8)
)

# Collect the top 10 WVK_IDs
top_10_wvk_ids = [row["WVK_ID"] for row in df_top_wvk_2023.collect()]

# Filter data for the top 10 WVK_IDs across all years
df_selected = df_accidents_per_year.filter(col("WVK_ID").isin(top_10_wvk_ids))

# Collect the filtered data for plotting
data = df_selected.collect()

plot_data = {}
for row in data:
    wvk_id = row["WVK_ID"]
    jaar = row["JAAR_VKL"]
    aantal = row["aantal_ongelukken"]
    if wvk_id not in plot_data:
        plot_data[wvk_id] = {"years": [], "accidents": []}
    plot_data[wvk_id]["years"].append(jaar)
    plot_data[wvk_id]["accidents"].append(aantal)

# Sort years and corresponding accidents for each WVK_ID
for wvk_id, values in plot_data.items():
    sorted_indices = sorted(range(len(values["years"])), key=lambda i: values["years"][i])
    plot_data[wvk_id]["years"] = [values["years"][i] for i in sorted_indices]
    plot_data[wvk_id]["accidents"] = [values["accidents"][i] for i in sorted_indices]

# Plot the data
plt.figure(figsize=(12, 8))
for wvk_id, values in plot_data.items():
    plt.plot(
        values["years"],
        values["accidents"],
        marker="o",
        label=f"WVK_ID {wvk_id}"
    )

# Customize the plot
plt.title("Number of Accidents per Year for Top 10 WVK_IDs in 2023")
plt.xlabel("Year")
plt.ylabel("Number of Accidents")
plt.xticks(sorted(set(row["JAAR_VKL"] for row in data)))  # Ensure only full years are shown
plt.legend(title="WVK_ID")
plt.grid(True)

# Save and display the plot
output_path = "top_10_wvk_accidents_per_year_corrected.png"
plt.savefig(output_path, dpi=300)
print(f"Grafiek opgeslagen als: {output_path}")
