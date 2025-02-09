from pyspark.sql import SparkSession
import matplotlib.pyplot as plt

spark = SparkSession.builder.getOrCreate()

df = spark.read.csv("/user/s2739046/MBD/ongevallen.csv", header=True, inferSchema=True)

counts_df = df.groupBy("AOL_ID").count()
counts_dict = {row["AOL_ID"]: row["count"] for row in counts_df.collect()}

# Result of dict: {1: 16190, 6: 35808, 3: 5715, 5: 10934, 9: 106390, 4: 79151, 8: 136867, 7: 226801, 2: 16377, 0: 559251}

key_mapping = {
    3: "Animal",
    9: "Single-vehicle",
    7: "Flank",
    6: "Frontal",
    2: "Parked vehicle",
    8: "Rear-end",
    5: "Loose object",
    4: "Fixed object",
    0: "Unknown",
    1: "Pedestrian"
}

named_counts_dict = {key_mapping[key]: value for key, value in counts_dict.items() if key_mapping[key] != "Unknown"}

sorted_counts = sorted(named_counts_dict.items(), key=lambda x: x[1], reverse=True)

labels, values = zip(*sorted_counts)

plt.figure(figsize=(10, 6))
bars = plt.bar(labels, values, color='#2e8ada')

ax = plt.gca()
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
ax.spines['left'].set_visible(False)
ax.spines['bottom'].set_visible(False)
ax.tick_params(left=False, bottom=False)

for bar in bars:
    yval = bar.get_height()
    plt.text(bar.get_x() + bar.get_width() / 2, yval + 2000, str(yval), ha='center', fontsize=9)

plt.title("Nature Of Accident Frequencies", fontsize=16)
plt.xlabel("Nature Of Accident", fontsize=12)
plt.ylabel("Amount", fontsize=12)
plt.xticks(rotation=45)

plt.savefig("nature_of_accident_bar_chart.png", bbox_inches='tight')
