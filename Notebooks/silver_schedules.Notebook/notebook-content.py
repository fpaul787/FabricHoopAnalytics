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

# # Silver Layer — `silver_schedules`
# 
# Transforms `bronze_schedules` into a clean, analytics-ready table.
# 
# **Steps**
# 1. Load bronze source
# 2. Cast mis-typed columns
# 3. Parse date strings
# 4. Normalize nulls & codes
# 5. Drop cosmetic / redundant columns
# 6. Deduplicate
# 7. Add derived columns
# 8. Write to Delta as `silver_schedules`
# 9. Validation

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, TimestampType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 1. Load Bronze

# CELL ********************

df = spark.sql("SELECT * FROM HoopLakehouse.hoop_data.bronze_schedules")
print(f"Bronze row count: {df.count():,}")
df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Cast Mis-Typed Columns
# 
# `attendance`, `venue_capacity`, `status_clock`, `status_period`, and `format_regulation_periods`
# were ingested as `double` but represent whole-number values.

# CELL ********************

double_to_int_cols = [
    "attendance",
    "venue_capacity",
    "status_clock",
    "status_period",
    "format_regulation_periods",
]

for col in double_to_int_cols:
    df = df.withColumn(col, F.col(col).cast(IntegerType()))

null_counts = (
    df.agg(*[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in double_to_int_cols])
      .collect()[0]
      .asDict()
)
print("Null counts after cast:", null_counts)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Parse Date Strings
# 
# `date` and `start_date` are ISO-8601 strings (e.g. `"2005-06-24T00:00Z"`). 
# Parse them to `timestamp`. `game_date` and `game_date_time` are already correctly typed and kept as-is.

# CELL ********************

df = (
    df
    .withColumn("date",       F.to_timestamp(F.col("date"),       "yyyy-MM-dd'T'HH:mmX"))
    .withColumn("start_date", F.to_timestamp(F.col("start_date"), "yyyy-MM-dd'T'HH:mmX"))
)

# Verify parse success
bad_dates = df.filter(F.col("date").isNull() | F.col("start_date").isNull()).count()
print(f"Rows with unparseable date/start_date: {bad_dates}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Normalize Nulls & Codes

# CELL ********************

# Empty strings in notes columns -> null (not every game has notes)
df = (
    df
    .withColumn("notes_type",     F.nullif(F.trim(F.col("notes_type")),     F.lit("")))
    .withColumn("notes_headline", F.nullif(F.trim(F.col("notes_headline")), F.lit("")))
)

# season_type integer code -> human-readable label (consistent with silver_team_box)
df = df.withColumn(
    "season_type_label",
    F.when(F.col("season_type") == 1, "Preseason")
     .when(F.col("season_type") == 2, "Regular Season")
     .when(F.col("season_type") == 3, "Playoffs")
     .otherwise("Unknown")
)

# Rename conference_competition for clarity
df = df.withColumnRenamed("conference_competition", "is_conference_game")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Drop Cosmetic / Redundant Columns
# 
# Removed:
# - `uid`, `home_uid`, `away_uid` — internal ESPN identifiers
# - `home_color`, `home_alternate_color`, `home_logo`, `home_short_display_name` — UI assets
# - `away_color`, `away_alternate_color`, `away_logo`, `away_short_display_name` — UI assets
# - `game_json_url` — raw data pipeline artifact, not analytical
# - `game_json` — boolean flag for pipeline use only
# - `type_abbreviation` — redundant with `season_type_label`
# - `status_display_clock` — string representation of `status_clock` (e.g. `"0.0"`), redundant
# - `status_type_name` — verbose form of `status_type_state` (e.g. STATUS_FINAL vs post)
# - `recent` — pipeline-relative boolean, not stable for analysis

# CELL ********************

drop_cols = [
    "uid", "home_uid", "away_uid",
    "home_color", "home_alternate_color", "home_logo", "home_short_display_name",
    "away_color", "away_alternate_color", "away_logo", "away_short_display_name",
    "game_json_url", "game_json",
    "type_abbreviation",
    "status_display_clock",
    "status_type_name",
    "recent",
]

df = df.drop(*drop_cols)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Deduplicate
# 
# Primary key is `game_id` — one row per game.

# CELL ********************

before = df.count()
df = df.dropDuplicates(["game_id"])
after = df.count()
print(f"Rows before dedup: {before:,} | after: {after:,} | dropped: {before - after:,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7. Derived Columns

# CELL ********************

# ── Scoring ───────────────────────────────────────────────────────────────────

# Total combined score
df = df.withColumn(
    "total_score",
    F.col("home_score") + F.col("away_score")
)

# Point differential from home team's perspective (positive = home win)
df = df.withColumn(
    "point_differential",
    F.col("home_score") - F.col("away_score")
)

# ── Game Context ──────────────────────────────────────────────────────────────

# Did the game go to overtime? (periods played > regulation periods)
df = df.withColumn(
    "went_to_overtime",
    F.when(
        F.col("status_period").isNotNull() & F.col("format_regulation_periods").isNotNull(),
        F.col("status_period") > F.col("format_regulation_periods")
    ).otherwise(False)
)

# Number of overtime periods played (0 if regulation)
df = df.withColumn(
    "overtime_periods",
    F.when(
        F.col("went_to_overtime"),
        F.col("status_period") - F.col("format_regulation_periods")
    ).otherwise(0)
)

# ── Venue ─────────────────────────────────────────────────────────────────────

# Attendance as a percentage of venue capacity
df = df.withColumn(
    "attendance_pct_capacity",
    F.when(
        F.col("venue_capacity") > 0,
        F.round(F.col("attendance") / F.col("venue_capacity"), 4)
    ).otherwise(None)
)

# ── Data Completeness ─────────────────────────────────────────────────────────

# Flag rows where all three supplementary data sources are available.
# Useful for filtering in downstream joins to silver_team_box / player_box.
df = df.withColumn(
    "data_complete",
    F.col("PBP") & F.col("team_box") & F.col("player_box")
)

print("Derived columns added.")
df.select(
    "game_id", "home_abbreviation", "away_abbreviation",
    "home_score", "away_score", "total_score", "point_differential",
    "went_to_overtime", "overtime_periods",
    "attendance", "venue_capacity", "attendance_pct_capacity",
    "data_complete"
).show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 8. Final Schema Review

# CELL ********************

df.printSchema()
print(f"Final row count: {df.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 9. Write to Silver
# 
# Partitioned by `season` and `season_type` — consistent with `silver_team_box`
# for efficient partition-pruning on joins between the two tables.

# CELL ********************

(
    df.write
      .format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .partitionBy("season", "season_type")
      .saveAsTable("HoopLakehouse.hoop_data.silver_schedules")
)

print("silver_schedules written successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 10. Validation

# CELL ********************

silver = spark.sql("SELECT * FROM HoopLakehouse.hoop_data.silver_schedules")

print(f"Row count: {silver.count():,}")

# Duplicate check
dup_count = silver.groupBy("game_id").count().filter(F.col("count") > 1).count()
print(f"Duplicate game_id rows: {dup_count}")

# Score integrity: home_winner should align with point_differential
winner_mismatch = silver.filter(
    F.col("status_type_completed") == True
).filter(
    (F.col("home_winner") == True)  & (F.col("point_differential") <= 0) |
    (F.col("home_winner") == False) & (F.col("point_differential") >= 0)
).count()
print(f"home_winner / point_differential mismatches (completed games): {winner_mismatch}")

# OT distribution
print("\nOvertime breakdown:")
silver.groupBy("overtime_periods").count().orderBy("overtime_periods").show()

# Data completeness summary
print("Data completeness (PBP + team_box + player_box):")
silver.groupBy("data_complete").count().show()

# Season / season_type distribution
silver.groupBy("season", "season_type", "season_type_label") \
      .count() \
      .orderBy("season", "season_type") \
      .show(50, truncate=False)

display(silver)

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
