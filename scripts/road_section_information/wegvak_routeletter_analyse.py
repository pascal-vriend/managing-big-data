from pyspark.sql import SparkSession
import matplotlib.pyplot as plt

spark = SparkSession.builder.getOrCreate()

df = spark.read.csv("/user/s2692759/MBD/ongevallen_wegvakken.csv", header=True, inferSchema=True)

counts_df = df.groupBy("routeltr").count()
counts_dict = {row["routeltr"]: row["count"] for row in counts_df.collect()}
counts_dict.pop(None)

# Result of dict: {'E': 2591, 'A': 178607, 'N': 63092, 'S': 181, 'U': 1}

sorted_counts = sorted(counts_dict.items(), key=lambda x: x[1], reverse=True)

labels, values = zip(*sorted_counts)

plt.figure(figsize=(10, 6))
plt.bar(labels, values, color='steelblue')

plt.title("Route Letters", fontsize=16)
plt.xlabel("Route Letter", fontsize=12)
plt.ylabel("Count", fontsize=12)
plt.xticks(rotation=45)

plt.savefig("route_letters_bar_chart.png", bbox_inches='tight')
