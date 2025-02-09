from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, round, lit, when, avg
import pandas as pd
spark = SparkSession.builder.appName("Accident Baselines").getOrCreate()
hdfs_path = "/user/s2739046/MBD/ongevallen.csv"
df = spark.read.csv(hdfs_path, header=True, inferSchema=True)

total_accidents = df.count()

def calculate_average(group_col):
    return (
        df.groupBy(group_col)
        .agg(
            count("*").alias("total_count"),
            round((count("*") / total_accidents) * 100, 2).alias("percentage")
        )
    )

##---- AP3_code: outcome of accident----
df = df.withColumn(
    "AP3_OMS",
    when(col("AP3_CODE") == "DOD", "Deadly")
    .when(col("AP3_CODE") == "LET", "Injury")
    .when(col("AP3_CODE") == "UMS", "Material Damage Only")
    .otherwise(None)
)

##---- avarge amount of parties involved----
avg_pty = df.agg(round(avg(col("ANTL_PTJ")), 2).alias("average_parties_involved"))

##---- average cause of accident---
df = df.withColumn(
    "AOL_OMS",
    when(col("AOL_ID") == 3, "Animal")
    .when(col("AOL_ID") == 9, "Onesided")
    .when(col("AOL_ID") == 7, "Sideways")
    .when(col("AOL_ID") == 6, "Frontal")
    .when(col("AOL_ID") == 2, "Parked vehicle")
    .when(col("AOL_ID") == 8, "Head/Tail")
    .when(col("AOL_ID") == 5, "Loose object")
    .when(col("AOL_ID") == 0, "Unkown")
    .when(col("AOL_ID") == 4, "Solid object")
    .when(col("AOL_ID") == 1, "pedestrian")
    .otherwise("Unknown")
)

##---- wegsituatie
df = df.withColumn(
    "WSE_OMS",
    when(col("WSE_ID") == 1, "Straight road")
    .when(col("WSE_ID") == 2, "Curve")
    .when(col("WSE_ID") == 3, "Roundabout")
    .when(col("WSE_ID") == 4, "Intersection with 3 branches")
    .when(col("WSE_ID") == 5, "Intersection with 4 branches")
    .when(col("WSE_ID") == 6, "Straight road with separated lanes")
    .when(col("WSE_ID") == 7, "On-ramp to (motor)way")
    .when(col("WSE_ID") == 8, "Off-ramp from (motor)way")
    .otherwise("Unknown")
)

df = df.withColumn(
    "BZD_OMS",
    when(col("BZD_ID_VM1") == 100, "One-way street")
    .when(col("BZD_ID_VM1") == 110, "Pedestrian crossing")
    .when(col("BZD_ID_VM1") == 120, "Other crossing")
    .when(col("BZD_ID_VM1") == 130, "Turning lanes")
    .when(col("BZD_ID_VM1") == 140, "Priority intersection/road")
    .when(col("BZD_ID_VM1") == 150, "Overtaking ban")
    .when(col("BZD_ID_VM1") == 160, "Traffic light working")
    .when(col("BZD_ID_VM1") == 170, "Traffic light flashing")
    .when(col("BZD_ID_VM1") == 180, "Traffic light not working")
    .when(col("BZD_ID_VM1") == 200, "Bridge/viaduct")
    .when(col("BZD_ID_VM1") == 210, "Bump/plateau")
    .when(col("BZD_ID_VM1") == 220, "Gas station")
    .when(col("BZD_ID_VM1") == 230, "Weaving lane")
    .when(col("BZD_ID_VM1") == 240, "Ventilation/parallel road")
    .when(col("BZD_ID_VM1") == 250, "Bus/Tram stop")
    .when(col("BZD_ID_VM1") == 260, "Parking facility")
    .when(col("BZD_ID_VM1") == 270, "Railway crossing")
    .when(col("BZD_ID_VM1") == 280, "Tunnel")
    .when(col("BZD_ID_VM1") == 290, "Narrowing")
    .when(col("BZD_ID_VM1") == 300, "Other accident")
    .when(col("BZD_ID_VM1") == 310, "Traffic jam")
    .when(col("BZD_ID_VM1") == 320, "Temporary road closure (partial/full of lanes/cycle path/sidewalk)")
    .when(col("BZD_ID_VM1") == 330, "Work in progress")
    .otherwise("Unknown")
)

##---- max speed ------
df = df.withColumn(
    "MAXSNELHD_OMS",
    when(col("MAXSNELHD") == 15, "15 km/h")
    .when(col("MAXSNELHD") == 30, "30 km/h")
    .when(col("MAXSNELHD") == 50, "50 km/h")
    .when(col("MAXSNELHD") == 60, "60 km/h")
    .when(col("MAXSNELHD") == 70, "70 km/h")
    .when(col("MAXSNELHD") == 80, "80 km/h")
    .when(col("MAXSNELHD") == 90, "90 km/h")
    .when(col("MAXSNELHD") == 100, "100 km/h")
    .when(col("MAXSNELHD") == 120, "120 km/h")
    .when(col("MAXSNELHD") == 130, "130 km/h")
    .otherwise("Unknown")
)

df = df.withColumn(
    "WVL_OMS",
    when(col("WVL_ID") == 1, "Burning")
    .when(col("WVL_ID") == 2, "Not Burning")
    .when(col("WVL_ID") == 3, "Not Present")
    .otherwise("Unknown")
)

df = df.withColumn(
    "WVG_OMS",
    when(col("WVG_ID") == 1, "ZOAB")
    .when(col("WVG_ID") == 2, "Other Asphalt")
    .when(col("WVG_ID") == 3, "Concrete")
    .when(col("WVG_ID") == 4, "Cobblestones")
    .when(col("WVG_ID") == 5, "Unpaved")
    .when(col("WVG_ID") == 6, "Gravel Tiles")
    .when(col("WVG_ID") == 7, "Wood Tiles")
    .otherwise("Unknown")
)

df = df.withColumn(
    "WDK_OMS",
    when(col("WDK_ID") == 1, "Dry")
    .when(col("WDK_ID") == 2, "Wet")
    .when(col("WDK_ID") == 3, "Snow/Ice")
    .otherwise("Unknown")
)

df = df.withColumn(
    "LGD_OMS",
    when(col("LGD_ID") == 1, "Daylight")
    .when(col("LGD_ID") == 2, "Darkness")
    .when(col("LGD_ID") == 3, "Dusk")
    .otherwise("Unknown")
)

df = df.withColumn(
    "Wgd_OMS",
    when(col("Wgd_code_1") == "D", "Dry")
    .when(col("Wgd_code_1") == "R", "Rain")
    .when(col("Wgd_code_1") == "M", "Fog")
    .when(col("Wgd_code_1") == "S", "Snow/Hail")
    .when(col("Wgd_code_1") == "H", "Strong Winds")
    .when(col("Wgd_code_1") == "O", "Unknown")
    .otherwise("Unknown")
)

# avarage outcome of accident(AP3_OMS)
avg_AP3 = calculate_average("AP3_OMS")

# Average of speed limits (MAXSNELHD_OMS)
avg_speed = calculate_average("MAXSNELHD_OMS")

# Average for accident cause (AOL_OMS)
avg_aol = calculate_average("AOL_OMS")

# Average for road situation (WSE_OMS)
avg_wse = calculate_average("WSE_OMS")

# Average for BZD description (BZD_OMS)
avg_bzd = calculate_average("BZD_OMS")

# Average for weather condition (Wgd_OMS)
avg_wgd = calculate_average("Wgd_OMS")

# Average for road surface type (WVG_OMS)
avg_wvg = calculate_average("WVG_OMS")

# Average for weather (WDK_OMS)
avg_wdk = calculate_average("WDK_OMS")

# Average for visibility (LGD_OMS)
avg_lgd = calculate_average("LGD_OMS")

# Average for surface condition (WVL_OMS)
avg_wvl = calculate_average("WVL_OMS")

# Show the results
avg_AP3.show()
avg_speed.show()
avg_aol.show()
avg_wse.show()
avg_bzd.show()
avg_wgd.show()
avg_wvg.show()
avg_wdk.show()
avg_lgd.show()
avg_wvl.show()
avg_pty.show()



