# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "cde85ba7-0260-4a72-bb87-692ca5c9285e",
# META       "default_lakehouse_name": "HoopLakehouse",
# META       "default_lakehouse_workspace_id": "b3c40514-dfa5-4755-9f0f-a6bb86c8e076",
# META       "known_lakehouses": [
# META         {
# META           "id": "cde85ba7-0260-4a72-bb87-692ca5c9285e"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "known_warehouses": []
# META     }
# META   }
# META }

# CELL ********************

df = spark.sql("SELECT * FROM HoopLakehouse.hoop_data.bronze_player_box")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = df.dropDuplicates([
    "game_id",
    "athlete_id",
    "team_id"
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = df.dropna(
    subset=[
        "game_id",
        "season",
        "athlete_id",
        "team_id"
    ]
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import coalesce, lit, col

stat_columns = [
    "points",
    "rebounds",
    "assists",
    "steals",
    "blocks",
    "turnovers",
    "minutes",
    "field_goals_made",
    "field_goals_attempted",
    "free_throws_made",
    "free_throws_attempted"
]

for c in stat_columns:
    df = df.withColumn(c, coalesce(col(c), lit(0)))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import upper

df = df.withColumn(
    "home_away",
    upper("home_away")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col

df = df.withColumn(
    "plus_minus",
    col("plus_minus").cast("int")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import when

df = df.withColumn(
    "fg_pct",
    when(
        col("field_goals_attempted") > 0,
        col("field_goals_made") /
        col("field_goals_attempted")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import expr
from pyspark.sql.functions import when, col

df = df.withColumn(
    "double_double",
    (
        when(col("points") >= 10, 1).otherwise(0) +
        when(col("rebounds") >= 10, 1).otherwise(0) +
        when(col("assists") >= 10, 1).otherwise(0) +
        when(col("steals") >= 10, 1).otherwise(0) +
        when(col("blocks") >= 10, 1).otherwise(0)
    ) >= 2
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import current_timestamp

df = df.withColumn(
    "silver_load_timestamp",
    current_timestamp()
)

(
    df.write
      .mode("overwrite")
      .option("mergeSchema", "true")
      .format("delta")
      .saveAsTable("hoop_data.silver_player_box")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
