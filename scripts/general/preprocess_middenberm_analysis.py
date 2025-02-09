"""
This file preprocess the data further to include the necessary data for the middenberm breedte analysis.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from functools import reduce

spark = SparkSession.builder.appName("MiddenbermProcessing").getOrCreate()

path = "/user/s2121182/preprocessed"
df = spark.read.csv(path, header=True, inferSchema=True)

columns_to_select_and_filter = ["AP3_CODE", "middenberm_breedte_SOORT", "middenberm_breedte_BREEDTE"]

columns_to_select = ["VKL_NUMMER", "WVK_ID"]

all_columns = columns_to_select_and_filter + columns_to_select

df_selected = df.select(*all_columns)

filter_condition = reduce(
    lambda acc, column: acc | ((col(column).isNotNull()) & (col(column) != 0)),
    columns_to_select_and_filter,
    None
)

df_filtered = df_selected.filter(filter_condition)

df_ap3 = df_filtered.filter(col("AP3_CODE").isNotNull())

df_ap3.write.csv("middenberm_dataset", mode="overwrite", header=True)
