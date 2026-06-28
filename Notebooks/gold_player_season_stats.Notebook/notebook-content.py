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

# # Gold Player Season Stats
# 
# Creates an analytics-ready player season table from `hoop_data.silver_player_box`.
# 
# **Grain:** One row per player per season.

# CELL ********************

from pyspark.sql import functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

player_df = (
    spark.table("hoop_data.silver_player_box")
         .filter(~F.col("did_not_play"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gold_df = (
    player_df
    .groupBy(
        "season",
        "athlete_id",
        "athlete_display_name",
        "athlete_short_name",
        "athlete_position_abbreviation",
        "team_id",
        "team_display_name",
        "team_abbreviation",
        "athlete_headshot_href"
    )
    .agg(
        F.countDistinct("game_id").alias("games_played"),
        F.sum(F.when(F.col("starter"),1).otherwise(0)).alias("games_started"),

        F.sum(F.when(F.col("team_winner"),1).otherwise(0)).alias("wins"),
        F.sum(F.when(~F.col("team_winner"),1).otherwise(0)).alias("losses"),

        F.sum(F.when(F.lower(F.col("home_away")) == "home", 1).otherwise(0)).alias("home_games"),
        F.sum(F.when(F.lower(F.col("home_away")) == "away", 1).otherwise(0)).alias("away_games"),
        F.sum("minutes").alias("total_minutes"),
        F.sum("points").alias("total_points"),
        F.sum("rebounds").alias("total_rebounds"),
        F.sum("assists").alias("total_assists"),
        F.sum("steals").alias("total_steals"),
        F.sum("blocks").alias("total_blocks"),
        F.sum("turnovers").alias("total_turnovers"),
        F.sum("fouls").alias("total_fouls"),

        F.avg("plus_minus").alias("average_plus_minus"),

        F.sum("field_goals_made").alias("fgm"),
        F.sum("field_goals_attempted").alias("fga"),
        F.sum("three_point_field_goals_made").alias("three_pm"),
        F.sum("three_point_field_goals_attempted").alias("three_pa"),
        F.sum("free_throws_made").alias("ftm"),
        F.sum("free_throws_attempted").alias("fta"),

        F.sum(F.when(F.col("double_double"),1).otherwise(0)).alias("double_doubles"),

        F.avg("points").alias("points_per_game"),
        F.avg("rebounds").alias("rebounds_per_game"),
        F.avg("assists").alias("assists_per_game"),
        F.avg("steals").alias("steals_per_game"),
        F.avg("blocks").alias("blocks_per_game"),
        F.avg("turnovers").alias("turnovers_per_game"),
        F.avg("fouls").alias("fouls_per_game"),
        F.avg("minutes").alias("minutes_per_game"),

        F.avg(F.when(F.lower(F.col("home_away")) == "home", F.col("points"))).alias("home_ppg"),
        F.avg(F.when(F.lower(F.col("home_away")) == "away", F.col("points"))).alias("away_ppg")
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
    "season","athlete_id","athlete_display_name","athlete_short_name",
    "athlete_position_abbreviation","team_id","team_display_name",
    "team_abbreviation","athlete_headshot_href",
    "games_played","games_started","wins","losses","win_pct",
    "home_games","away_games",
    F.round("minutes_per_game",1).alias("minutes_per_game"),
    F.round("points_per_game",1).alias("points_per_game"),
    F.round("rebounds_per_game",1).alias("rebounds_per_game"),
    F.round("assists_per_game",1).alias("assists_per_game"),
    F.round("steals_per_game",1).alias("steals_per_game"),
    F.round("blocks_per_game",1).alias("blocks_per_game"),
    F.round("turnovers_per_game",1).alias("turnovers_per_game"),
    F.round("fouls_per_game",1).alias("fouls_per_game"),
    F.round("home_ppg",1).alias("home_ppg"),
    F.round("away_ppg",1).alias("away_ppg"),
    F.round("average_plus_minus",1).alias("average_plus_minus"),
    "fg_pct","three_pt_pct","ft_pct",
    "double_doubles",
    "total_points","total_rebounds","total_assists","total_steals","total_blocks"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(gold_df.select("athlete_display_name", "points_per_game", "season", "team_display_name").orderBy(F.desc("points_per_game")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gold_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("HoopLakehouse.hoop_data.gold_player_season_stats")

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
