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

# # Silver Layer — `silver_team_box`
# 
# Transforms `bronze_team_box` into a clean, analytics-ready table.
# 
# **Steps**
# 1. Load bronze source
# 2. Cast mis-typed columns
# 3. Normalize flags & codes
# 4. Drop cosmetic / redundant columns
# 5. Deduplicate
# 6. Add derived / efficiency metrics
# 7. Write to Delta as `silver_team_box`

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 1. Load Bronze

# CELL ********************

df = spark.sql("SELECT * FROM HoopLakehouse.hoop_data.bronze_team_box")
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
# The following columns contain numeric values but were ingested as `string` in bronze.
# Cast them to `IntegerType` and flag any values that cannot be parsed.

# CELL ********************

string_to_int_cols = [
    "fast_break_points",
    "points_in_paint",
    "turnover_points",
    "largest_lead",
]

for col in string_to_int_cols:
    df = df.withColumn(col, F.col(col).cast(IntegerType()))

# Sanity check: count nulls introduced by bad casts
null_counts = {c: df.filter(F.col(c).isNull()).count() for c in string_to_int_cols}
print("Null counts after cast:", null_counts)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Normalize Flags & Codes

# CELL ********************

# team_home_away -> boolean is_home
df = df.withColumn(
    "is_home",
    F.when(F.lower(F.col("team_home_away")) == "home", True)
     .when(F.lower(F.col("team_home_away")) == "away", False)
     .otherwise(None)
).drop("team_home_away")

# season_type integer code -> human-readable label
# ESPN codes: 1 = Preseason, 2 = Regular Season, 3 = Playoffs
df = df.withColumn(
    "season_type_label",
    F.when(F.col("season_type") == 1, "Preseason")
     .when(F.col("season_type") == 2, "Regular Season")
     .when(F.col("season_type") == 3, "Playoffs")
     .otherwise("Unknown")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Drop Cosmetic / Redundant Columns
# 
# The following columns are display assets or are redundant with other fields:
# - `team_uid` / `opponent_team_uid` — internal ESPN identifiers, not useful for analytics
# - `team_slug` / `opponent_team_slug` — redundant with `team_abbreviation`
# - `team_color`, `team_alternate_color`, `team_logo` — UI assets
# - `opponent_team_color`, `opponent_team_alternate_color`, `opponent_team_logo` — UI assets
# - `team_short_display_name` / `opponent_team_short_display_name` — redundant with `team_name`

# CELL ********************

drop_cols = [
    "team_uid", "team_slug", "team_color", "team_alternate_color",
    "team_logo", "team_short_display_name",
    "opponent_team_uid", "opponent_team_slug", "opponent_team_color",
    "opponent_team_alternate_color", "opponent_team_logo",
    "opponent_team_short_display_name",
]

df = df.drop(*drop_cols)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Deduplicate
# 
# Primary key is `(game_id, team_id)` — one row per team per game.

# CELL ********************

before = df.count()
df = df.dropDuplicates(["game_id", "team_id"])
after = df.count()
print(f"Rows before dedup: {before:,} | after: {after:,} | dropped: {before - after:,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Derived & Efficiency Columns

# CELL ********************

# ── Scoring ──────────────────────────────────────────────────────────────────

# Point differential (positive = won, negative = lost)
df = df.withColumn(
    "point_differential",
    F.col("team_score") - F.col("opponent_team_score")
)

# Categorised margin: useful for filtering without writing CASE every time
df = df.withColumn(
    "score_margin_category",
    F.when(F.abs(F.col("point_differential")) <= 3,  "OT-Range (1-3)")
     .when(F.abs(F.col("point_differential")) <= 8,  "Close (4-8)")
     .when(F.abs(F.col("point_differential")) <= 15, "Comfortable (9-15)")
     .otherwise("Blowout (16+)")
)

# ── Shooting Efficiency ───────────────────────────────────────────────────────

# Effective FG% = (FGM + 0.5 * 3PM) / FGA  — weights 3-pointers properly
df = df.withColumn(
    "effective_fg_pct",
    F.when(
        F.col("field_goals_attempted") > 0,
        F.round(
            (F.col("field_goals_made") + 0.5 * F.col("three_point_field_goals_made"))
            / F.col("field_goals_attempted"),
            4
        )
    ).otherwise(None)
)

# True Shooting % = PTS / (2 * (FGA + 0.44 * FTA))  — accounts for FTs
df = df.withColumn(
    "true_shooting_pct",
    F.when(
        (F.col("field_goals_attempted") + F.col("free_throws_attempted")) > 0,
        F.round(
            F.col("team_score")
            / (2 * (F.col("field_goals_attempted") + 0.44 * F.col("free_throws_attempted"))),
            4
        )
    ).otherwise(None)
)

# 3-Point Rate = 3PA / FGA  — share of shots from deep
df = df.withColumn(
    "three_point_rate",
    F.when(
        F.col("field_goals_attempted") > 0,
        F.round(
            F.col("three_point_field_goals_attempted") / F.col("field_goals_attempted"),
            4
        )
    ).otherwise(None)
)

# Free Throw Rate = FTA / FGA  — ability to get to the line
df = df.withColumn(
    "free_throw_rate",
    F.when(
        F.col("field_goals_attempted") > 0,
        F.round(
            F.col("free_throws_attempted") / F.col("field_goals_attempted"),
            4
        )
    ).otherwise(None)
)

# ── Ball Control ──────────────────────────────────────────────────────────────

# Assist-to-Turnover Ratio
df = df.withColumn(
    "assist_to_turnover_ratio",
    F.when(
        F.col("turnovers") > 0,
        F.round(F.col("assists") / F.col("turnovers"), 2)
    ).otherwise(None)
)

# ── Rebounding ────────────────────────────────────────────────────────────────

# Data quality flag: offensive + defensive should equal total rebounds
df = df.withColumn(
    "rebound_count_mismatch",
    (F.col("offensive_rebounds") + F.col("defensive_rebounds")) != F.col("total_rebounds")
)

print("Derived columns added.")
df.select(
    "game_id", "team_abbreviation", "team_score", "opponent_team_score",
    "point_differential", "score_margin_category",
    "effective_fg_pct", "true_shooting_pct",
    "three_point_rate", "free_throw_rate",
    "assist_to_turnover_ratio", "rebound_count_mismatch"
).show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7. Final Schema Review

# CELL ********************

df.printSchema()
print(f"Final row count: {df.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 8. Write to Silver
# 
# Partition by `season` and `season_type` for efficient downstream filtering.
# Using `mergeSchema=True` so future bronze additions can flow through without breaking the write.

# CELL ********************

(
    df.write
      .format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .partitionBy("season", "season_type")
      .saveAsTable("HoopLakehouse.hoop_data.silver_team_box")
)

print("silver_team_box written successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 9. Validation

# CELL ********************

silver = spark.sql("SELECT * FROM HoopLakehouse.hoop_data.silver_team_box")

print(f"Row count: {silver.count():,}")

# Duplicate check
dup_count = silver.groupBy("game_id", "team_id").count().filter(F.col("count") > 1).count()
print(f"Duplicate (game_id, team_id) pairs: {dup_count}")

# Rebound mismatch summary
mismatch = silver.filter(F.col("rebound_count_mismatch") == True).count()
print(f"Rows with rebound count mismatch: {mismatch}")

# Season / season_type distribution
silver.groupBy("season", "season_type", "season_type_label") \
      .count() \
      .orderBy("season", "season_type") \
      .show(truncate=False)

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
