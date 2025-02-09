# OUTPUT OF THIS CODE:
#
# Correlation between maximum speed and outcome: -0.19669129322067053
# Correlation between weather and outcome: -0.034407955525223036
# Correlation between speed and weather: 0.0639699554952489
# Correlation between lighting and outcome: -0.029850411459865122
# Correlation between urban area and outcome: -0.06633748111278195


from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.stat import Correlation
import pyspark.sql.functions as F

spark = SparkSession.builder.getOrCreate()

df = spark.read.csv("/user/s2739046/MBD/ongevallen.csv", header=True, inferSchema=True)

def calculate_correlation(df, col1, col2):
    assembler = VectorAssembler(inputCols=[col1, col2], outputCol="features")
    vector_df = assembler.transform(df.select(col1, col2).dropna())
    correlation = Correlation.corr(vector_df, "features").head()[0].toArray()
    return correlation[0][1]

# Severity Mapping
# DOD: death              3
# LET: injury             2
# UMS: material damage    1

df = df.withColumn("AP3_CODE_NUM", F.when(F.col("AP3_CODE") == "DOD", 3)
                   .when(F.col("AP3_CODE") == "LET", 2)
                   .when(F.col("AP3_CODE") == "UMS", 1)
                   .otherwise(None))

correlation_speed_outcome = calculate_correlation(df, "MAXSNELHD", "AP3_CODE_NUM")
print(f"Correlation between maximum speed and outcome: {correlation_speed_outcome}")

# Weather Mapping
# D: dry            0
# R: rain           1
# M: fog            2
# S: snow           3
# H: strong wind    4

df = df.withColumn("WGD_CODE_NUM",
                   F.when(F.col("WGD_CODE_1") == "D", 0)
                   .when(F.col("WGD_CODE_1") == "R", 1)
                   .when(F.col("WGD_CODE_1") == "M", 2)
                   .when(F.col("WGD_CODE_1") == "S", 3)
                   .when(F.col("WGD_CODE_1") == "H", 4)
                   .otherwise(None))

correlation_weather_outcome = calculate_correlation(df, "WGD_CODE_NUM", "AP3_CODE_NUM")
print(f"Correlation between weather and outcome: {correlation_weather_outcome}")

correlation_speed_weather = calculate_correlation(df, "MAXSNELHD", "WGD_CODE_NUM")
print(f"Correlation between speed and weather: {correlation_speed_weather}")

correlation_lighting_severity = calculate_correlation(df, "LGD_ID", "AP3_CODE_NUM")
print(f"Correlation between lighting and outcome: {correlation_lighting_severity}")

# Urban Mapping
# BI: within urban area     0
# BU: outside urban area    1

df = df.withColumn("BEBKOM_NUM",
                   F.when(F.col("BEBKOM") == "BI", 0)
                   .when(F.col("BEBKOM") == "BU", 1)
                   .otherwise(None))

correlation_urban_outcome = calculate_correlation(df, "BEBKOM_NUM", "AP3_CODE_NUM")
print(f"Correlation between urban area and outcome: {correlation_urban_outcome}")
