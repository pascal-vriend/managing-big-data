import geopandas as gpd
import os

# Path to the extracted shapefile
shapefile_dir = "/home/s2563363/Snelheden/01-11-2024"
shapefile_path = os.path.join(shapefile_dir, "Snelheden.shp")
output_path = os.path.join(shapefile_dir, "Snelheden.parquet")

# Read the shapefile
gdf = gpd.read_file(shapefile_path)

# Convert to Parquet
gdf.to_parquet(output_path)

print("Conversion complete. Parquet file saved at:", output_path)
