from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc, rank, when
from pyspark.sql.window import Window


# Initialize SparkSession
spark = SparkSession.builder.appName("Top Roads by Accident Type").getOrCreate()

# Load accident dataset
hdfs_path = "/user/s2739046/MBD/ongevallen.csv"
df = spark.read.csv(hdfs_path, header=True, inferSchema=True)

# Map AOL_ID to descriptive categories
df = df.withColumn(
    "AOL_OMS",
    when(col("AOL_ID") == 3, "Animal")
    .when(col("AOL_ID") == 9, "Onesided")
    .when(col("AOL_ID") == 7, "Sideways")
    .when(col("AOL_ID") == 6, "Frontal")
    .when(col("AOL_ID") == 2, "Parked vehicle")
    .when(col("AOL_ID") == 8, "Head/Tail")
    .when(col("AOL_ID") == 5, "Loose object")
    .when(col("AOL_ID") == 0, "Unknown")
    .when(col("AOL_ID") == 4, "Solid object")
    .when(col("AOL_ID") == 1, "Pedestrian")
    .otherwise("Unknown"),
)

# Filter out null road IDs
df = df.filter(col("WVK_ID").isNotNull())

# Calculate total accidents by category for all roads
category_totals = (
    df.groupBy("AOL_OMS")
    .agg(count("*").alias("total_category_accidents"))
)

# Calculate total accidents per road across all categories
road_totals = (
    df.groupBy("WVK_ID")
    .agg(count("*").alias("total_road_accidents"))
)

# Calculate accidents per road and category
accidents_by_road_and_category = (
    df.groupBy("WVK_ID", "AOL_OMS")
    .agg(count("*").alias("road_accident_count"))
    .join(category_totals, "AOL_OMS")
    .join(road_totals, "WVK_ID")
)

# Add percentage for this road in the category (percentage within the category)
accidents_by_road_and_category = accidents_by_road_and_category.withColumn(
    "category_percentage", (col("road_accident_count") / col("total_category_accidents")) * 100
)

# Add percentage of accidents on the road for this category (percentage on road)
accidents_by_road_and_category = accidents_by_road_and_category.withColumn(
    "road_percentage", (col("road_accident_count") / col("total_road_accidents")) * 100
)

# Rank the rows to get the top-ranked road for each category
window_spec = Window.partitionBy("AOL_OMS").orderBy(
    desc("category_percentage"), desc("road_percentage"), desc("road_accident_count")
)
accidents_by_road_and_category = accidents_by_road_and_category.withColumn(
    "rank", rank().over(window_spec)
)

# Filter to get only the top-ranked road per category
top_roads_per_category = accidents_by_road_and_category.filter(col("rank") == 1)

# Select and display relevant columns
result = top_roads_per_category.select(
    "AOL_OMS",
    "WVK_ID",
    "road_accident_count",
    "total_road_accidents",
    "category_percentage",
    "road_percentage",
)

# Sort by road_percentage in descending order
result_sorted = result.orderBy(desc("road_percentage"))

result_sorted.show(truncate=False)