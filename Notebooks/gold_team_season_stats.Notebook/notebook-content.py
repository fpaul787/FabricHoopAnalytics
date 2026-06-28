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

# # Gold Team Season Stats
# 
# Creates `hoop_data.gold_team_season_stats` from `hoop_data.silver_team_box`.
# 
# **Grain:** One row per team per season.


# CELL ********************

from pyspark.sql import functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

team_df = spark.table("hoop_data.silver_team_box")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gold_df = (
    team_df
    .groupBy(
        "season",
        "season_type",
        "season_type_label",
        "team_id",
        "team_display_name",
        "team_name",
        "team_location",
        "team_abbreviation"
    )
    .agg(
        F.countDistinct("game_id").alias("games_played"),
        F.sum(F.when(F.col("team_winner"),1).otherwise(0)).alias("wins"),
        F.sum(F.when(~F.col("team_winner"),1).otherwise(0)).alias("losses"),
        F.sum(F.when(F.col("is_home"),1).otherwise(0)).alias("home_games"),
        F.sum(F.when(~F.col("is_home"),1).otherwise(0)).alias("away_games"),

        F.sum("team_score").alias("total_points"),
        F.avg("team_score").alias("points_per_game"),
        F.avg("opponent_team_score").alias("opponent_points_per_game"),
        F.avg("point_differential").alias("avg_point_differential"),

        F.avg("assists").alias("assists_per_game"),
        F.avg("total_rebounds").alias("rebounds_per_game"),
        F.avg("steals").alias("steals_per_game"),
        F.avg("blocks").alias("blocks_per_game"),
        F.avg("turnovers").alias("turnovers_per_game"),

        F.sum("field_goals_made").alias("fgm"),
        F.sum("field_goals_attempted").alias("fga"),
        F.sum("three_point_field_goals_made").alias("three_pm"),
        F.sum("three_point_field_goals_attempted").alias("three_pa"),
        F.sum("free_throws_made").alias("ftm"),
        F.sum("free_throws_attempted").alias("fta"),

        F.avg("effective_fg_pct").alias("effective_fg_pct"),
        F.avg("true_shooting_pct").alias("true_shooting_pct"),
        F.avg("assist_to_turnover_ratio").alias("assist_to_turnover_ratio"),
        F.avg("three_point_rate").alias("three_point_rate"),
        F.avg("free_throw_rate").alias("free_throw_rate"),

        F.avg("fast_break_points").alias("fast_break_points_per_game"),
        F.avg("points_in_paint").alias("paint_points_per_game"),
        F.avg("largest_lead").alias("average_largest_lead")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gold_df = (
    gold_df
    .withColumn("win_pct", F.round(F.col("wins")/F.col("games_played"),3))
    .withColumn("fg_pct",
        F.round(F.when(F.col("fga")>0,F.col("fgm")/F.col("fga")),3))
    .withColumn("three_pt_pct",
        F.round(F.when(F.col("three_pa")>0,F.col("three_pm")/F.col("three_pa")),3))
    .withColumn("ft_pct",
        F.round(F.when(F.col("fta")>0,F.col("ftm")/F.col("fta")),3))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gold_df = gold_df.select(
    "season","season_type","season_type_label",
    "team_id","team_display_name","team_name","team_location","team_abbreviation",
    "games_played","wins","losses","win_pct",
    "home_games","away_games",
    F.round("points_per_game",1).alias("points_per_game"),
    F.round("opponent_points_per_game",1).alias("opponent_points_per_game"),
    F.round("avg_point_differential",1).alias("avg_point_differential"),
    F.round("assists_per_game",1).alias("assists_per_game"),
    F.round("rebounds_per_game",1).alias("rebounds_per_game"),
    F.round("steals_per_game",1).alias("steals_per_game"),
    F.round("blocks_per_game",1).alias("blocks_per_game"),
    F.round("turnovers_per_game",1).alias("turnovers_per_game"),
    "fg_pct","three_pt_pct","ft_pct",
    F.round("effective_fg_pct",3).alias("effective_fg_pct"),
    F.round("true_shooting_pct",3).alias("true_shooting_pct"),
    F.round("assist_to_turnover_ratio",2).alias("assist_to_turnover_ratio"),
    F.round("three_point_rate",3).alias("three_point_rate"),
    F.round("free_throw_rate",3).alias("free_throw_rate"),
    F.round("fast_break_points_per_game",1).alias("fast_break_points_per_game"),
    F.round("paint_points_per_game",1).alias("paint_points_per_game"),
    F.round("average_largest_lead",1).alias("average_largest_lead"),
    "total_points"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(gold_df.orderBy(F.desc("win_pct"), F.desc("points_per_game")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gold_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("hoop_data.gold_team_season_stats")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
