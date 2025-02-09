from pyspark.sql import SparkSession
import matplotlib.pyplot as plt

spark = SparkSession.builder.getOrCreate()

df1 = spark.read.csv("/user/s2739046/MBD/ongevallen.csv", header=True, inferSchema=True)
df2 = spark.read.csv("/user/s2692759/MBD/wegvakken.csv", header=True, inferSchema=True)
df3 = df1.join(df2, on="WVK_ID", how="left")

counts_df = df3.groupBy("stt_naam").count()
counts_dict = {row["stt_naam"]: row["count"] for row in counts_df.collect()}
counts_dict.pop(None)
counts_dict.pop('Rykswg') # Not filled in
counts_dict.pop('Rijksweg') # Not filled in
counts_dict.pop('Provincialeweg') # Not filled in
counts_dict.pop('Prov Wg') # Not filled in

sorted_counts = sorted(counts_dict.items(), key=lambda x: x[1], reverse=True)[:20]

labels, values = zip(*sorted_counts)

plt.figure(figsize=(10, 6))
plt.bar(labels, values, color='darkcyan')

plt.title("Accidents Per Street Name (Top 20)", fontsize=16)
plt.xlabel("Street Name", fontsize=12)
plt.ylabel("Count", fontsize=12)
plt.xticks(rotation=45)

plt.savefig("street_name_accidents.png", bbox_inches='tight')
