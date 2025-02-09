"""
Running on Spark takes approximately 7,5 minutes (without using 2> /dev/nul)
This file merges all the separate datasets into one dataset for analysis
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MergeData").getOrCreate()

def rename_columns(df, prefix):
    for col_name in df.columns:
        df = df.withColumnRenamed(col_name, f"{prefix}_{col_name}")
    return df


# Use accidents as the main file and LEFT-join all other files with this file
accidents = spark.read.csv("/user/s2739046/MBD/ongevallen.csv", header=True, inferSchema=True)
parties = spark.read.csv("/user/s2739046/MBD/partijen.csv", header=True, inferSchema=True)
parties = rename_columns(parties, 'partijen')

wegvak_id_csv_files = [
    "/user/s2563363/FilesProject/geleiderails_middenberm.csv",
    "/user/s2563363/FilesProject/middenberm_breedte.csv",
    "/user/s2563363/FilesProject/oversteekplaatsen.csv",
    "/user/s2563363/FilesProject/bomen.csv",
    "/user/s2563363/FilesProject/inritten.csv"
]

wvk_id_csv_files = [
    "/user/s2563363/FilesProject/wegbreedte.csv",
    "/user/s2563363/FilesProject/schoolzones.csv",
    "/user/s2563363/FilesProject/wegversmallingen.csv"
]

wvk_id_parquet_files = [
    "/user/s2563363/FilesProject/Snelheden.parquet"
]
main_df = accidents.join(parties, accidents["VKL_NUMMER"] == parties["partijen_VKL_NUMMER"], how="left")


def join_files(df, file_paths, column_mapping, file_type, join_type="left"):
    for file_path in file_paths:
        if file_type == 'csv':
            other_df = spark.read.csv(file_path, sep=";", header=True, inferSchema=True)
        elif file_type == 'parquet':
            other_df = spark.read.parquet(file_path)
        else:
            raise Exception("Not a valid file type")

        prefix = file_path.split('/')[-1].split('.')[0]

        other_df = rename_columns(other_df, prefix)

        df = df.join(other_df, main_df["WVK_ID"] == other_df[f"{prefix}_{column_mapping}"], how=join_type)

    return df


main_df = join_files(main_df, wegvak_id_csv_files, column_mapping="WEGVAK_ID", file_type="csv")
main_df = join_files(main_df, wvk_id_csv_files, column_mapping="WVK_ID", file_type="csv")
main_df = join_files(main_df, wvk_id_parquet_files, column_mapping="WVK_ID", file_type="parquet")

non_binary_columns = [col for col, dtype in main_df.dtypes if dtype != "binary"]
main_df_filtered = main_df.select(non_binary_columns)

main_df_filtered.write.csv("temp_path", mode="overwrite", header=True)
