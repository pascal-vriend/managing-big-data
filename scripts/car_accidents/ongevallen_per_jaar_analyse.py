from pyspark.sql import SparkSession
import matplotlib.pyplot as plt

spark = SparkSession.builder.getOrCreate()

df = spark.read.csv("/user/s2739046/MBD/ongevallen.csv", header=True, inferSchema=True)

counts_df = df.groupBy("JAAR_VKL").count()
counts_dict = {row["JAAR_VKL"]: row["count"] for row in counts_df.collect()}

# Result of dict: {2015: 113289, 2022: 122036, 2014: 94593, 2016: 124992, 2017: 123930, 2021: 114219, 2018: 128225, 2019: 134617, 2020: 103578, 2023: 134005}

sorted_counts = sorted(counts_dict.items(), key=lambda x: x[0])

labels, values = zip(*sorted_counts)

plt.figure(figsize=(10, 6))
bars = plt.bar(labels, values, color='#f06530')

ax = plt.gca()
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
ax.spines['left'].set_visible(False)
ax.spines['bottom'].set_visible(False)
ax.tick_params(left=False, bottom=False)

for bar in bars:
    yval = bar.get_height()
    plt.text(bar.get_x() + bar.get_width() / 2, yval + 2000, str(yval), ha='center', fontsize=9)

plt.title("Accidents Per Year", fontsize=16)
plt.xlabel("Year", fontsize=12)
plt.ylabel("Amount", fontsize=12)
plt.xticks(rotation=45)

plt.savefig("accidents_per_year_bar_chart.png", bbox_inches='tight')
