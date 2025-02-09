"""
Running on Spark takes approximately 7,5 minutes (with using 2> /dev/nul)
This file preprocess the data by removing all columns with only Null values
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("PreprocessData").getOrCreate()

path = "/user/s2121182/temp_path"
df = spark.read.csv(path, header=True, inferSchema=True)

print(f"Initial Columns: {len(df.columns)}")
print(f"Initial Rows: {df.count()}")
print(f"Number of columns before cleaning: {len(df.columns)}")

non_null_columns = []

for index, col_name in enumerate(df.columns):
    non_null_count = df.filter(col(col_name).isNotNull()).limit(1).count()

    if non_null_count > 0:
        non_null_columns.append(col_name)

    print(f"--------------------------------------------------------------------- Finished column: {col_name} with index {index} -----------------------------------------------")

df_cleaned = df.select(non_null_columns)

num_columns_after = len(df_cleaned.columns)
print(f"Number of columns after cleaning: {num_columns_after}")

df_cleaned.write.csv("preprocessed", mode="overwrite", header=True)
df_cleaned.printSchema()
df_cleaned.show(5)

