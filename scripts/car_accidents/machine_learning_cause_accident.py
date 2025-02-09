"""

Feature importances for all AOL_ID_OUTPUT:
+----------------+------------+
|    Feature     | Importance |
+----------------+------------+
|  AP3_CODE_NUM  |   11.71%   |
|    ANTL_PTJ    |   32.08%   |
|   MAXSNELHD    |   11.66%   |
|     WVL_ID     |   2.72%    |
|     WVG_ID     |   12.13%   |
|   LGD_ID_NUM   |   9.63%    |
|   WDK_ID_NUM   |   0.26%    |
|   BEBKOM_NUM   |   2.32%    |
| WGD_CODE_1_NUM |   0.03%    |
|     WSE_ID     |   17.47%   |
+----------------+------------+


Feature importances changes when leaving out cause = Pedestrian:
+----------------+---------------------+-----------------+-----------------+
|    Feature     | Filtered Importance | Absolute Change | Relative Change |
+----------------+---------------------+-----------------+-----------------+
|  AP3_CODE_NUM  |        6.90%        |      -4.81%     |     -41.09%     |
|    ANTL_PTJ    |        48.46%       |     +16.38%     |     +51.06%     |
|   MAXSNELHD    |        6.18%        |      -5.48%     |     -47.00%     |
|     WVL_ID     |        1.95%        |      -0.77%     |     -28.18%     |
|     WVG_ID     |        9.70%        |      -2.43%     |     -20.01%     |
|   LGD_ID_NUM   |        9.13%        |      -0.50%     |      -5.16%     |
|   WDK_ID_NUM   |        0.24%        |      -0.02%     |      -7.54%     |
|   BEBKOM_NUM   |        0.72%        |      -1.60%     |     -69.07%     |
| WGD_CODE_1_NUM |        0.06%        |      +0.04%     |     +123.05%    |
|     WSE_ID     |        16.65%       |      -0.81%     |      -4.66%     |
+----------------+---------------------+-----------------+-----------------+
Leaving out pedestrian accidents decreases the importance of the following features, indicating they are more relevant for predicting pedestrian accidents:
- Maximum speed
- Outcome
- Urban area

Leaving out pedestrian accidents increases the importance of the following features, indicating they are less relevant for predicting pedestrian accidents:
- Amount of parties


Feature importances changes when leaving out cause = Frontal:
+----------------+---------------------+-----------------+-----------------+
|    Feature     | Filtered Importance | Absolute Change | Relative Change |
+----------------+---------------------+-----------------+-----------------+
|  AP3_CODE_NUM  |        9.88%        |      -1.83%     |     -15.64%     |
|    ANTL_PTJ    |        32.68%       |      +0.60%     |      +1.88%     |
|   MAXSNELHD    |        10.89%       |      -0.76%     |      -6.53%     |
|     WVL_ID     |        3.13%        |      +0.41%     |     +14.99%     |
|     WVG_ID     |        12.84%       |      +0.71%     |      +5.85%     |
|   LGD_ID_NUM   |        9.72%        |      +0.09%     |      +0.97%     |
|   WDK_ID_NUM   |        0.24%        |      -0.03%     |      -9.81%     |
|   BEBKOM_NUM   |        3.06%        |      +0.74%     |     +31.88%     |
| WGD_CODE_1_NUM |        0.01%        |      -0.02%     |     -73.60%     |
|     WSE_ID     |        17.55%       |      +0.09%     |      +0.50%     |
+----------------+---------------------+-----------------+-----------------+
Leaving out frontal accidents decreases the importance of the following features, indicating they are more relevant for predicting frontal accidents:
- No big differences

Leaving out frontal accidents increases the importance of the following features, indicating they are less relevant for predicting frontal accidents:
- No big differences


Feature importances changes when leaving out cause = Animal:
+----------------+---------------------+-----------------+-----------------+
|    Feature     | Filtered Importance | Absolute Change | Relative Change |
+----------------+---------------------+-----------------+-----------------+
|  AP3_CODE_NUM  |        12.89%       |      +1.18%     |     +10.05%     |
|    ANTL_PTJ    |        31.89%       |      -0.19%     |      -0.59%     |
|   MAXSNELHD    |        12.71%       |      +1.05%     |      +9.03%     |
|     WVL_ID     |        2.11%        |      -0.61%     |     -22.53%     |
|     WVG_ID     |        13.28%       |      +1.15%     |      +9.46%     |
|   LGD_ID_NUM   |        6.87%        |      -2.75%     |     -28.61%     |
|   WDK_ID_NUM   |        0.40%        |      +0.14%     |     +51.83%     |
|   BEBKOM_NUM   |        2.99%        |      +0.67%     |     +28.76%     |
| WGD_CODE_1_NUM |        0.05%        |      +0.02%     |     +78.94%     |
|     WSE_ID     |        16.82%       |      -0.65%     |      -3.71%     |
+----------------+---------------------+-----------------+-----------------+
Leaving out animal accidents decreases the importance of the following features, indicating they are more relevant for predicting animal accidents:
- Daylight/darkness/dusk

Leaving out animal accidents increases the importance of the following features, indicating they are less relevant for predicting animal accidents:
- No big differences


Feature importances changes when leaving out cause = Loose object:
+----------------+---------------------+-----------------+-----------------+
|    Feature     | Filtered Importance | Absolute Change | Relative Change |
+----------------+---------------------+-----------------+-----------------+
|  AP3_CODE_NUM  |        12.07%       |      +0.36%     |      +3.07%     |
|    ANTL_PTJ    |        31.64%       |      -0.44%     |      -1.37%     |
|   MAXSNELHD    |        11.37%       |      -0.29%     |      -2.46%     |
|     WVL_ID     |        3.03%        |      +0.31%     |     +11.37%     |
|     WVG_ID     |        12.80%       |      +0.67%     |      +5.54%     |
|   LGD_ID_NUM   |        9.38%        |      -0.24%     |      -2.50%     |
|   WDK_ID_NUM   |        0.24%        |      -0.02%     |      -8.64%     |
|   BEBKOM_NUM   |        2.47%        |      +0.15%     |      +6.38%     |
| WGD_CODE_1_NUM |        0.06%        |      +0.03%     |     +99.27%     |
|     WSE_ID     |        16.94%       |      -0.53%     |      -3.01%     |
+----------------+---------------------+-----------------+-----------------+
Leaving out loose object accidents decreases the importance of the following features, indicating they are more relevant for predicting loose object accidents:
- No big differences

Leaving out loose object accidents increases the importance of the following features, indicating they are less relevant for predicting loose object accidents:
- No big differences


Feature importances changes when leaving out cause = Single-vehicle:
+----------------+---------------------+-----------------+-----------------+
|    Feature     | Filtered Importance | Absolute Change | Relative Change |
+----------------+---------------------+-----------------+-----------------+
|  AP3_CODE_NUM  |        7.08%        |      -4.63%     |     -39.57%     |
|    ANTL_PTJ    |        2.22%        |     -29.86%     |     -93.08%     |
|   MAXSNELHD    |        20.42%       |      +8.76%     |     +75.17%     |
|     WVL_ID     |        3.91%        |      +1.19%     |     +43.65%     |
|     WVG_ID     |        17.71%       |      +5.58%     |     +45.97%     |
|   LGD_ID_NUM   |        13.28%       |      +3.66%     |     +38.01%     |
|   WDK_ID_NUM   |        0.35%        |      +0.09%     |     +33.98%     |
|   BEBKOM_NUM   |        0.28%        |      -2.04%     |     -88.09%     |
| WGD_CODE_1_NUM |        0.04%        |      +0.01%     |     +34.08%     |
|     WSE_ID     |        34.72%       |     +17.25%     |     +98.78%     |
+----------------+---------------------+-----------------+-----------------+
Leaving out single-vehicle accidents decreases the importance of the following features, indicating they are more relevant for predicting single-vehicle accidents:
- Amount of parties
- Outcome
- Urban area

Leaving out single-vehicle accidents increases the importance of the following features, indicating they are less relevant for predicting single-vehicle accidents:
- Type of road
- Maximum speed


Feature importances changes when leaving out cause = Fixed object:
+----------------+---------------------+-----------------+-----------------+
|    Feature     | Filtered Importance | Absolute Change | Relative Change |
+----------------+---------------------+-----------------+-----------------+
|  AP3_CODE_NUM  |        15.93%       |      +4.23%     |     +36.09%     |
|    ANTL_PTJ    |        24.48%       |      -7.60%     |     -23.68%     |
|   MAXSNELHD    |        19.32%       |      +7.67%     |     +65.80%     |
|     WVL_ID     |        1.16%        |      -1.56%     |     -57.36%     |
|     WVG_ID     |        18.41%       |      +6.28%     |     +51.81%     |
|   LGD_ID_NUM   |        1.97%        |      -7.66%     |     -79.54%     |
|   WDK_ID_NUM   |        0.34%        |      +0.08%     |     +28.66%     |
|   BEBKOM_NUM   |        7.92%        |      +5.60%     |     +241.54%    |
| WGD_CODE_1_NUM |        0.02%        |      -0.01%     |     -21.86%     |
|     WSE_ID     |        10.43%       |      -7.03%     |     -40.27%     |
+----------------+---------------------+-----------------+-----------------+
Leaving out fixed object accidents decreases the importance of the following features, indicating they are more relevant for predicting fixed object accidents:
- Daylight/darkness/dusk
- Amount of parties
- Type of road

Leaving out fixed object accidents increases the importance of the following features, indicating they are less relevant for predicting fixed object accidents:
- Maximum speed
- Road hardness
- Urban area


Feature importances changes when leaving out cause = Rear-end:
+----------------+---------------------+-----------------+-----------------+
|    Feature     | Filtered Importance | Absolute Change | Relative Change |
+----------------+---------------------+-----------------+-----------------+
|  AP3_CODE_NUM  |        7.99%        |      -3.72%     |     -31.78%     |
|    ANTL_PTJ    |        45.22%       |     +13.14%     |     +40.96%     |
|   MAXSNELHD    |        3.12%        |      -8.53%     |     -73.23%     |
|     WVL_ID     |        1.13%        |      -1.60%     |     -58.61%     |
|     WVG_ID     |        3.59%        |      -8.54%     |     -70.42%     |
|   LGD_ID_NUM   |        4.20%        |      -5.42%     |     -56.34%     |
|   WDK_ID_NUM   |        0.32%        |      +0.06%     |     +22.10%     |
|   BEBKOM_NUM   |        2.23%        |      -0.09%     |      -3.70%     |
| WGD_CODE_1_NUM |        0.04%        |      +0.01%     |     +24.71%     |
|     WSE_ID     |        32.16%       |     +14.70%     |     +84.14%     |
+----------------+---------------------+-----------------+-----------------+
Leaving out rear-end accidents decreases the importance of the following features, indicating they are more relevant for predicting rear-end accidents:
- Maximum speed
- Daylight/darkness/dusk

Leaving out rear-end accidents increases the importance of the following features, indicating they are less relevant for predicting rear-end accidents:
- Type of road
- Amount of parties


Feature importances changes when leaving out cause = Flank:
+----------------+---------------------+-----------------+-----------------+
|    Feature     | Filtered Importance | Absolute Change | Relative Change |
+----------------+---------------------+-----------------+-----------------+
|  AP3_CODE_NUM  |        9.86%        |      -1.85%     |     -15.83%     |
|    ANTL_PTJ    |        37.89%       |      +5.81%     |     +18.11%     |
|   MAXSNELHD    |        16.89%       |      +5.23%     |     +44.90%     |
|     WVL_ID     |        2.00%        |      -0.72%     |     -26.44%     |
|     WVG_ID     |        14.17%       |      +2.04%     |     +16.85%     |
|   LGD_ID_NUM   |        8.97%        |      -0.66%     |      -6.86%     |
|   WDK_ID_NUM   |        0.27%        |      +0.00%     |      +0.12%     |
|   BEBKOM_NUM   |        4.63%        |      +2.31%     |     +99.62%     |
| WGD_CODE_1_NUM |        0.02%        |      -0.01%     |     -37.40%     |
|     WSE_ID     |        5.32%        |     -12.15%     |     -69.57%     |
+----------------+---------------------+-----------------+-----------------+
Leaving out flank accidents decreases the importance of the following features, indicating they are more relevant for predicting flank accidents:
- Type of road

Leaving out flank accidents increases the importance of the following features, indicating they are less relevant for predicting flank accidents:
- Amount of parties
- Maximum speed
- Urban area


Feature importances changes when leaving out cause = Parked vehicle:
+----------------+---------------------+-----------------+-----------------+
|    Feature     | Filtered Importance | Absolute Change | Relative Change |
+----------------+---------------------+-----------------+-----------------+
|  AP3_CODE_NUM  |        12.55%       |      +0.84%     |      +7.20%     |
|    ANTL_PTJ    |        40.48%       |      +8.40%     |     +26.19%     |
|   MAXSNELHD    |        8.39%        |      -3.27%     |     -28.03%     |
|     WVL_ID     |        5.77%        |      +3.04%     |     +111.83%    |
|     WVG_ID     |        3.35%        |      -8.78%     |     -72.42%     |
|   LGD_ID_NUM   |        11.59%       |      +1.97%     |     +20.42%     |
|   WDK_ID_NUM   |        0.27%        |      +0.01%     |      +2.62%     |
|   BEBKOM_NUM   |        3.11%        |      +0.79%     |     +34.21%     |
| WGD_CODE_1_NUM |        0.17%        |      +0.14%     |     +480.08%    |
|     WSE_ID     |        14.33%       |      -3.14%     |     -17.98%     |
+----------------+---------------------+-----------------+-----------------+
Leaving out parked vehicle accidents decreases the importance of the following features, indicating they are more relevant for predicting parked vehicle accidents:
- Road hardness
- Maximum speed
- Road type

Leaving out parked vehicle accidents increases the importance of the following features, indicating they are less relevant for predicting parked vehicle accidents:
- Amount of parties
- Lights next to road

"""








from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StringIndexer
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml import Pipeline
import pyspark.sql.functions as F
from prettytable import PrettyTable

spark = SparkSession.builder.getOrCreate()

df = spark.read.csv("/user/s2739046/MBD/ongevallen.csv", header=True, inferSchema=True)

# Cause of accident (AOL_ID)
# 3: "Animal"
# 9: "Single-vehicle"
# 7: "Flank"
# 6: "Frontal"
# 2: "Parked vehicle"
# 8: "Rear-end"
# 5: "Loose object"
# 4: "Fixed object"
# 0: "Unknown"
# 1: "Pedestrian"

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

df = df.withColumn("AOL_ID_OUTPUT", F.when(F.col("AOL_ID") == 0, None).otherwise(F.col("AOL_ID")))
df = df.filter(F.col("AOL_ID_OUTPUT").isNotNull())

categorical_columns = ["AP3_CODE", "LGD_ID", "WDK_ID", "BEBKOM", "WGD_CODE_1"]
indexers = [
    StringIndexer(inputCol=col, outputCol=col + "_NUM", handleInvalid="skip") for col in categorical_columns
]

features_list = ["AP3_CODE_NUM", "ANTL_PTJ", "MAXSNELHD", "WVL_ID", "WVG_ID", "LGD_ID_NUM", "WDK_ID_NUM", "BEBKOM_NUM", "WGD_CODE_1_NUM", "WSE_ID"]

assembler = VectorAssembler(
    inputCols=features_list,
    outputCol="features"
)

rf = RandomForestRegressor(featuresCol="features", labelCol="AOL_ID_OUTPUT")
pipeline = Pipeline(stages=indexers + [assembler, rf])

df = df.dropna(subset=["AOL_ID_OUTPUT", "ANTL_PTJ", "MAXSNELHD", "WVL_ID", "WVG_ID", "LGD_ID", "WDK_ID", "BEBKOM", "WGD_CODE_1", "WSE_ID"])

model = pipeline.fit(df)
importances = model.stages[-1].featureImportances
global_importances_percentage = importances.toArray() * 100
print("Feature importances for all AOL_ID_OUTPUT:")
table = PrettyTable()
table.field_names = ["Feature", "Importance"]
for name, importance in zip(features_list, global_importances_percentage):
    table.add_row([name, f"{importance:.2f}%"])
print(table)

unique_labels = df.select("AOL_ID_OUTPUT").distinct().collect()
for label in unique_labels:
    label_value = label["AOL_ID_OUTPUT"]
    df_filtered = df.filter(F.col("AOL_ID_OUTPUT") != label_value)
    model = pipeline.fit(df_filtered)
    importances = model.stages[-1].featureImportances
    importances_percentage = importances.toArray() * 100
    print(f"\nFeature importances changes when leaving out cause = {key_mapping[label_value]}:")
    table = PrettyTable()
    table.field_names = ["Feature", "Filtered Importance", "Absolute Change", "Relative Change"]
    for name, global_imp, filtered_imp in zip(features_list, global_importances_percentage, importances_percentage):
        absolute_change = filtered_imp - global_imp
        relative_change = ((filtered_imp - global_imp) / global_imp) * 100 if global_imp != 0 else 0
        table.add_row([name, f"{filtered_imp:.2f}%", f"{absolute_change:+.2f}%", f"{relative_change:+.2f}%"])
    print(table)
