from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, row_number

spark = SparkSession.builder.getOrCreate()

df1 = spark.read.csv("/user/s2739046/MBD/ongevallen.csv", header=True, inferSchema=True)
df2 = spark.read.csv("/user/s2692759/MBD/wegvakken.csv", header=True, inferSchema=True)

df2 = df2.drop('dienstcode')
df2 = df2.drop('dienstnaam')
df2 = df2.drop('distrcode')
df2 = df2.drop('distrnaam')
df2 = df2.drop('gme_id')
df2 = df2.drop('gme_naam')

df3 = df1.join(df2, on="WVK_ID", how="inner")
df3.coalesce(1).write.csv("/user/s2692759/MBD/ongevallen_wegvakken.csv", header=True, mode="overwrite")
