from pyspark.sql import SparkSession
import matplotlib.pyplot as plt

spark = SparkSession.builder.getOrCreate()

df = spark.read.csv("/user/s2739046/MBD/ongevallen.csv", header=True, inferSchema=True)

counts_df = df.groupBy("ANTL_PTJ").count()
counts_dict = {row["ANTL_PTJ"]: row["count"] for row in counts_df.collect()}

# Result of dict: {31: 1, 12: 29, 1: 215722, 13: 16, 6: 1521, 16: 8, 3: 100288, 20: 1, 5: 4822, 19: 2, 15: 5, 9: 176, 17: 5, 4: 19285, 8: 323, 7: 653, 10: 75, 11: 45, 14: 13, 2: 581389, 0: 269098, 24: 1, 36: 1, 45: 1, 25: 2, 22: 1, 18: 1}

sorted_counts = sorted(counts_dict.items(), key=lambda x: x[0])[:10]

labels, values = zip(*sorted_counts)

plt.figure(figsize=(10, 6))
plt.bar(labels, values, color='darkcyan')
plt.title("Amount Of Parties Per Accident", fontsize=16)
plt.xlabel("Amount Of Parties", fontsize=12)
plt.ylabel("Amount Of Accidents", fontsize=12)
plt.xticks(rotation=45)

plt.savefig("amount_of_parties_per_accident_bar_chart.png", bbox_inches='tight')
