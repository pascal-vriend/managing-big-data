from pyspark.sql import SparkSession
import matplotlib.pyplot as plt

spark = SparkSession.builder.getOrCreate()

df = spark.read.csv("/user/s2692759/MBD/ongevallen_wegvakken.csv", header=True, inferSchema=True)

alias_dict = {
    "AFR": "Exit",
    "VDA": "Car Ferry",
    "BU": "Bus Lane",
    "BUS": "Bus Lane",
    "CADO": "Emergency Passage",
    "DST": "Shortcut",
    "ERF": "Residential Area",
    "FP": "Bike Path",
    "VDF": "Bike Ferry",
    "GRB": "Large Roundabout Lane",
    "HR": "Main Roadway",
    "MRB": "Mini Roundabout Lane",
    "NRB": "Roundabout Lane",
    "OVB": "Public Transport Lane",
    "OPR": "On-Ramp",
    "YYY": "Other Lane Type (Unspecified)",
    "PAR": "Service Road",
    "PP": "Parking Area",
    "PR": "Park & Ride",
    "PKB": "Parking Area with Gas Station",
    "PC": "Carpool Parking Area",
    "PST": "Chevron",
    "RB": "Roadway",
    "RP": "Bridleway",
    "TRB": "Turbo Roundabout Lane",
    "TN": "Intermediate Lane",
    "VD": "Ferry Service",
    "VWG": "Frontage Road",
    "VBW": "Connecting Road (Other)",
    "VBD": "Direct Connecting Road",
    "VBI": "Indirect Connecting Road",
    "VBK": "Short Connecting Road",
    "VBR": "Shunting yard",
    "VBS": "Semi-Connecting Road",
    "BST": "Service Road to/from Gas Station",
    "BVP": "Service Road to/from Fuel Outlet",
    "PKP": "Service Road to/from Parking Area",
    "VV": "Air Traffic",
    "VZ": "Pedestrian Zone",
    "VP": "Footpath",
    "VDV": "Pedestrian Ferry",
    "WIS": "Switching Lane"
}

counts_df = df.groupBy("bst_code").count()
counts_dict = {row["bst_code"]: row["count"] for row in counts_df.collect()}

# Result of dict: {'PC': 14, 'VP': 3282, 'GRB': 1008, 'TRB': 1165, 'ERF': 3411, 'VBR': 7809, 'BUS': 1135, 'PKP': 186, 'HR': 237134, 'VZ': 3035, 'VBK': 385, 'PAR': 9394, 'RB': 263284, 'OVB': 28, 'VBD': 3035, 'NRB': 5298, 'VBS': 3624, 'AFR': 6084, 'TN': 18, 'OPR': 4669, 'DST': 883, 'BVP': 482, 'FP': 10422, 'PST': 3153, 'PKB': 1179, 'VBI': 1092, 'VBW': 693, 'PP': 861, 'CADO': 4, 'YYY': 5, 'VDV': 1, 'VDF': 3, 'PR': 3, 'VDA': 2}

sorted_counts = sorted(counts_dict.items(), key=lambda x: x[1], reverse=True)[:10]

labels, values = zip(*sorted_counts)

aliased_labels = [alias_dict.get(label, label) for label in labels]

plt.figure(figsize=(10, 6))
plt.bar(aliased_labels, values, color='#00284F')
ax = plt.gca()
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
ax.spines['left'].set_visible(False)
ax.spines['bottom'].set_visible(False)
ax.tick_params(left=False, bottom=False)
plt.xticks(fontsize=9)

plt.title("Top 10 road types", fontsize=16)
plt.xlabel("Road type", fontsize=12)
plt.ylabel("Count", fontsize=12)
plt.xticks(rotation=45)

plt.savefig("top_10_bst_code_bar_chart.png", bbox_inches='tight')

sorted_counts_2 = sorted(counts_dict.items(), key=lambda x: x[1], reverse=True)[2:12]

labels_2, values_2 = zip(*sorted_counts_2)

aliased_labels_2 = [alias_dict.get(label, label) for label in labels_2]

plt.figure(figsize=(10, 6))
plt.bar(aliased_labels_2, values_2, color='#00284F')
ax = plt.gca()
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
ax.spines['left'].set_visible(False)
ax.spines['bottom'].set_visible(False)
ax.tick_params(left=False, bottom=False)
plt.xticks(fontsize=9)

plt.title("Top 10 road types; without the top 2", fontsize=16)
plt.xlabel("Road type", fontsize=12)
plt.ylabel("Count", fontsize=12)
plt.xticks(rotation=45)

plt.savefig("top_2_to_12_bst_code_bar_chart.png", bbox_inches='tight')
