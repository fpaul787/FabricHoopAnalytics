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
# META     }
# META   }
# META }

# CELL ********************

# Read data
player_box_df = spark.read.parquet("Files/hoopr-nba-storage/player_box/parquet/*")

# Write to bronze
player_box_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("hoop_data.bronze_player_box")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read data
player_box_df = spark.read.parquet("Files/hoopr-nba-storage/schedules/parquet")

# Write to bronze
player_box_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("hoop_data.bronze_schedules")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read data
player_box_df = spark.read.parquet("Files/hoopr-nba-storage/team_box/parquet/*")

# Write to bronze
player_box_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("hoop_data.bronze_team_box")

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
