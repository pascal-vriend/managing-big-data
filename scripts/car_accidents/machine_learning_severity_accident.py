# OUTPUT OF THIS CODE:
#
# Feature Importances:
# MAXSNELHD: 0.6376
# LGD_ID_NUM: 0.0201
# WDK_ID_NUM: 0.0609
# BEBKOM_NUM: 0.1454
# WGD_CODE_1_NUM: 0.0302
# WSE_ID_NUM: 0.1058


from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StringIndexer
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml import Pipeline
import pyspark.sql.functions as F

spark = SparkSession.builder.getOrCreate()

df = spark.read.csv("/user/s2739046/MBD/ongevallen.csv", header=True, inferSchema=True)

# Severity Mapping
# DOD: death              3
# LET: injury             2
# UMS: material damage    1

df = df.withColumn("AP3_CODE_NUM", F.when(F.col("AP3_CODE") == "DOD", 3)
                   .when(F.col("AP3_CODE") == "LET", 2)
                   .when(F.col("AP3_CODE") == "UMS", 1)
                   .otherwise(None))

categorical_columns = ["LGD_ID", "WDK_ID", "BEBKOM", "WGD_CODE_1", "WSE_ID"]
indexers = [
    StringIndexer(inputCol=col, outputCol=col + "_NUM", handleInvalid="skip") for col in categorical_columns
]

features_list = ["MAXSNELHD", "LGD_ID_NUM", "WDK_ID_NUM", "BEBKOM_NUM", "WGD_CODE_1_NUM", "WSE_ID_NUM"]

assembler = VectorAssembler(
    inputCols=features_list,
    outputCol="features"
)

rf = RandomForestRegressor(featuresCol="features", labelCol="AP3_CODE_NUM")
pipeline = Pipeline(stages=indexers + [assembler, rf])

df = df.dropna(subset=["AP3_CODE_NUM", "MAXSNELHD"])

model = pipeline.fit(df)
importances = model.stages[-1].featureImportances

print("Feature Importances:")
for name, importance in zip(features_list, model.stages[-1].featureImportances.toArray()):
    print(f"{name}: {importance:.4f}")

