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

# MARKDOWN ********************

# # Gold Game Summary
# 
# Creates `hoop_data.gold_game_summary` from `hoop_data.silver_schedules`.
# 
# **Grain:** One row per game.


# CELL ********************

from pyspark.sql import functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

games_df = spark.table("HoopLakehouse.hoop_data.silver_schedules")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gold_df = (
    games_df
    .withColumn(
        "winning_team",
        F.when(F.col("home_winner"), F.col("home_display_name"))
         .otherwise(F.col("away_display_name"))
    )
    .withColumn(
        "winning_team_abbreviation",
        F.when(F.col("home_winner"), F.col("home_abbreviation"))
         .otherwise(F.col("away_abbreviation"))
    )
    .withColumn(
        "losing_team",
        F.when(F.col("home_winner"), F.col("away_display_name"))
         .otherwise(F.col("home_display_name"))
    )
    .withColumn(
        "losing_team_abbreviation",
        F.when(F.col("home_winner"), F.col("away_abbreviation"))
         .otherwise(F.col("home_abbreviation"))
    )
    .withColumn(
        "winner_score",
        F.when(F.col("home_winner"), F.col("home_score"))
         .otherwise(F.col("away_score"))
    )
    .withColumn(
        "loser_score",
        F.when(F.col("home_winner"), F.col("away_score"))
         .otherwise(F.col("home_score"))
    )
    .withColumn(
        "sellout_game",
        F.when(
            F.col("attendance_pct_capacity").isNotNull(),
            F.col("attendance_pct_capacity") >= 0.95
        ).otherwise(False)
    )
    .withColumn(
        "high_scoring_game",
        F.col("total_score") >= 240
    )
    .withColumn(
        "close_game",
        F.col("point_differential") <= 5
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gold_df = gold_df.select(
    "game_id",
    "season",
    "season_type",
    "season_type_label",
    "game_date",
    "game_date_time",
    "home_display_name",
    "home_abbreviation",
    "away_display_name",
    "away_abbreviation",
    "winning_team",
    "winning_team_abbreviation",
    "losing_team",
    "losing_team_abbreviation",
    "winner_score",
    "loser_score",
    "home_score",
    "away_score",
    "total_score",
    "point_differential",
    "venue_full_name",
    "venue_address_city",
    "venue_address_state",
    "venue_capacity",
    "attendance",
    "attendance_pct_capacity",
    "sellout_game",
    "neutral_site",
    "is_conference_game",
    "went_to_overtime",
    "overtime_periods",
    "high_scoring_game",
    "close_game",
    "data_complete"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    gold_df.orderBy(
        F.desc("game_date")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gold_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("HoopLakehouse.hoop_data.gold_game_summary")

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
