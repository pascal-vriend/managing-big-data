# Ran using normal python to convert geojson object to be compatible with python gepandas:

import json

with open("/home/s2692759/MBD/gemeentes.geojson", "r") as f:
    geojson_data = json.load(f)

for feature in geojson_data["features"]:
    properties = feature["properties"]

    if isinstance(properties.get("prov_code"), list):
        properties["prov_code"] = properties["prov_code"][0]
    if isinstance(properties.get("prov_name"), list):
        properties["prov_name"] = properties["prov_name"][0]
    if isinstance(properties.get("gem_code"), list):
        properties["gem_code"] = properties["gem_code"][0]
    if isinstance(properties.get("gem_name"), list):
        properties["gem_name"] = properties["gem_name"][0]
    if isinstance(properties.get("gem_cbs_code"), list):
        properties["gem_cbs_code"] = properties["gem_cbs_code"][0]

with open("/home/s2692759/MBD/gemeentes_gefixt.geojson", "w") as f:
    json.dump(geojson_data, f)


# Script to retrieve accidents per municipality in map of the Netherlands

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower
import matplotlib.pyplot as plt
import pandas as pd
import geopandas as gpd
import contextily as ctx

spark = SparkSession.builder.getOrCreate()

df1 = spark.read.csv("/user/s2739046/MBD/ongevallen.csv", header=True, inferSchema=True)
df2 = spark.read.csv("/user/s2692759/MBD/bevolkingsdichtheid.csv", header=True, inferSchema=True)

counts_df = df1.groupBy("GME_NAAM").count()

counts_df = counts_df.join(df2, lower(df2["gemeente"]).contains(lower(counts_df["GME_NAAM"])),"left")

counts_df = counts_df.withColumn("genormaliseerd", col("count") / col("Inwonersperkm"))

counts_dict = {row["GME_NAAM"]: row["genormaliseerd"] for row in counts_df.collect()}

geo_df = gpd.read_file("/home/s2692759/MBD/gemeentes_gefixt.geojson")

geo_df["ongevallen"] = geo_df["gem_name"].map(counts_dict)
geo_df["ongevallen"] = geo_df["ongevallen"].fillna(0)

geo_df["ongevallen_klasse"] = pd.qcut(geo_df["ongevallen"], 6, labels=False)

fig, ax = plt.subplots(1, 1, figsize=(12, 8))

geo_df.plot(column="ongevallen_klasse", cmap="Reds", legend=False, ax=ax, edgecolor="k")

# ctx.add_basemap(ax, crs=geo_df.crs.to_string(), source=ctx.providers.OpenStreetMap.Mapnik)

ax.set_title("Car Accidents per Municipality in the Netherlands (Normalized)", fontsize=16)
ax.axis("off")

plt.savefig("genormaliseerd_ongevallen_per_gemeente_nederland_map.png")



