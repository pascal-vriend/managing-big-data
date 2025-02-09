"""
This file preprocess the data further to include the necessary data for the trees vs outcome analysis.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from functools import reduce

spark = SparkSession.builder.appName("TreesProcessing").getOrCreate()

path = "/user/s2121182/preprocessed"
df = spark.read.csv(path, header=True, inferSchema=True)

columns_to_select_and_filter = ["PVOPGEM", "JAAR_VKL", "AP3_CODE", "ANTL_PTJ", "AOL_ID", "NIVEAUKOP",
                                "WSE_ID", "BEBKOM", "MAXSNELHD", "WVL_ID", "WVG_AN", "WDK_ID", "WDK_AN", "LGD_ID",
                                "ZAD_ID", "WGD_CODE_1", "WGD_CODE_2", "BZD_ID_VM1", "BZD_ID_VM2", "BZD_ID_VM3",
                                "BZD_VM_AN", "BZD_ID_IF1", "BZD_IF_AN", "BZD_ID_TA1", "BZD_ID_TA2", "BZD_TA_AN",
                                "bomen_O_AF_KL_1", "bomen_O_AF_KL_2", "bomen_O_AF_KL_3", "bomen_O_AF_KL_4",
                                "bomen_O_AF_KL_5", "bomen_O_AF_KL_6", "bomen_O_AF_KL_7"]

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

df_ap3.write.csv("tree_dataset", mode="overwrite", header=True)
