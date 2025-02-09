# The following commands ran in pyspark terminal

from pyspark.sql.functions import col

df1 = spark.read.csv("/user/s2739046/MBD/ongevallen.csv", header=True, inferSchema=True)

counts_list = {column: df1.filter(col(column).isNotNull()).count() for column in df1.columns}

columns_to_drop = [column for column, count in counts_list.items() if count == 0]
df2 = df1.drop(*columns_to_drop)

df2.write.csv("/user/s2692759/MBD/ongevallen_gefilterd.csv", header=True, mode="overwrite")




df3 = spark.read.csv("/user/s2739046/MBD/partijen.csv", header=True, inferSchema=True)

counts_list = {column: df3.filter(col(column).isNotNull()).count() for column in df3.columns}

columns_to_drop = [column for column, count in counts_list.items() if count == 0]

df4 = df3.drop(*columns_to_drop)

df3.write.csv("/user/s2692759/MBD/partijen_gefilterd.csv", header=True, mode="overwrite")
