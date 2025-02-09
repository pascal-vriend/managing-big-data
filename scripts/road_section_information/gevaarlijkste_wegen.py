from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc, first


spark = SparkSession.builder.appName("Top 10 road sections").getOrCreate()
hdfs_path = "/user/s2739046/MBD/ongevallen.csv"
df = spark.read.csv(hdfs_path, header=True, inferSchema=True)

top_wegsegmenten = (
    df.groupBy("WVK_ID")
    .agg(count("VKL_NUMMER").alias("aantal_ongelukken"), first("FK_VELD5").alias("FK_VELD5"))
    .filter(col("WVK_ID").isNotNull())
    .orderBy(desc("aantal_ongelukken"))
    .limit(10)
)

# Laad de dataset met puntlocaties
puntlocaties_path = "/user/s2739046/MBD/puntlocaties.txt"
puntlocaties_df = spark.read.csv(puntlocaties_path, header=True, inferSchema=True)

# Voeg coördinaten toe aan de top 10 wegsegmenten
df_with_coords = top_wegsegmenten.join(
    puntlocaties_df, top_wegsegmenten["FK_VELD5"] == puntlocaties_df["FK_VELD5"], "inner"
)

# Selecteer unieke coördinaten en wegsegmenten
locaties = df_with_coords.select("WVK_ID", "X_COORD", "Y_COORD", "aantal_ongelukken").orderBy(desc("aantal_ongelukken"))

locaties.show()
