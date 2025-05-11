# Databricks notebook source
# MAGIC %md
# MAGIC # Sleeper Information

# COMMAND ----------

import dlt
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType, MapType, DoubleType, NullType, FloatType, BooleanType, TimestampType, LongType
from pyspark.sql.functions import col, expr, explode, current_timestamp, sum, when, lit, array_contains

# COMMAND ----------

# MAGIC %md
# MAGIC ##bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ### bronze_rosters

# COMMAND ----------

@dlt.table(
  comment="The raw rosters from the Sleeper API",
  table_properties={
    "myCompanyPipeline.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def bronze_rosters():
  rosters_schema = StructType([
    StructField("co_owners", ArrayType(StringType()), True),  # Assuming it could be a list of co-owner IDs (strings)
    StructField("keepers", ArrayType(StringType()), True),  # Assuming it could be a list of keeper player IDs (strings)
    StructField("league_id", StringType(), True),
    StructField("metadata", MapType(StringType(), StringType()), True),  # metadata as a map of strings
    StructField("owner_id", StringType(), True),
    StructField("player_map", MapType(StringType(), StringType()), True),  # Generic map for player info
    StructField("players", ArrayType(StringType()), True),  # Players array of strings (player IDs)
    StructField("reserve", ArrayType(StringType()), True),  # Assuming it could be a list of player IDs in reserve
    StructField("roster_id", IntegerType(), True),
    StructField("settings", StructType([  # Nested struct for settings
        StructField("fpts", IntegerType(), True),
        StructField("fpts_against", IntegerType(), True),
        StructField("fpts_against_decimal", IntegerType(), True),
        StructField("fpts_decimal", IntegerType(), True),
        StructField("losses", IntegerType(), True),
        StructField("ppts", IntegerType(), True),
        StructField("ppts_decimal", IntegerType(), True),
        StructField("ties", IntegerType(), True),
        StructField("total_moves", IntegerType(), True),
        StructField("waiver_budget_used", IntegerType(), True),
        StructField("waiver_position", IntegerType(), True),
        StructField("wins", IntegerType(), True),
    ]), True),
    StructField("starters", ArrayType(StringType()), True),  # Starters array of strings (player IDs)
    StructField("taxi", ArrayType(StringType()), True),  # Assuming it could be a list of taxi squad player IDs
    StructField("_year", IntegerType(), True),
    StructField("_league_id", StringType(), True),
    StructField("_matchup_week", IntegerType(), True),
    StructField("_ingested_ts", TimestampType(), True)
  ])

  return spark.readStream.schema(rosters_schema).json('/mnt/databricks/sleeper/stg/rosters/*')

# COMMAND ----------

# MAGIC %md
# MAGIC ###bronze_matchups

# COMMAND ----------

@dlt.table(
  comment="The raw matchups from the Sleeper API",
  table_properties={
    "myCompanyPipeline.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def bronze_matchups():
    matchups_schema = StructType([
        StructField("points", DoubleType(), True),  # Points as a double (floating point number)
        StructField("players", ArrayType(StringType()), True),  # Array of player IDs (strings)
        StructField("roster_id", IntegerType(), True),  # Roster ID as an integer
        StructField("custom_points", ArrayType(StringType()), True),  # Nullable field for custom points (could later be a float)
        StructField("matchup_id", IntegerType(), True),  # Matchup ID as an integer
        StructField("starters", ArrayType(StringType()), True),  # Array of starters (player IDs as strings)
        StructField("starters_points", ArrayType(FloatType()), True),  # Array of points corresponding to the starters
        StructField("players_points", MapType(StringType(), FloatType()), True),  # Map of player IDs to their points
        StructField("_year", IntegerType(), True),
        StructField("_league_id", StringType(), True),
        StructField("_matchup_week", IntegerType(), True),
        StructField("_ingested_ts", TimestampType(), True)
    ])

    return spark.readStream.schema(matchups_schema).json('/mnt/databricks/sleeper/stg/matchups/*')

# COMMAND ----------

# MAGIC %md
# MAGIC ### bronze_users

# COMMAND ----------

@dlt.table(
  comment="The raw users from the Sleeper API",
  table_properties={
    "myCompanyPipeline.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def bronze_users():
    users_schema = StructType([
    StructField("avatar", StringType(), True),
    StructField("display_name", StringType(), True),
    StructField("is_bot", BooleanType(), True),
    StructField("is_owner", BooleanType(), True),
    StructField("league_id", StringType(), True),
    StructField("metadata", StructType([
        StructField("allow_pn", StringType(), True),
        StructField("archived", StringType(), True),
        StructField("avatar", StringType(), True),
        StructField("mascot_item_type_id_leg_1", StringType(), True),
        StructField("mascot_message_emotion_leg_1", StringType(), True),
        StructField("mascot_message_leg_3", StringType(), True),
        StructField("mention_pn", StringType(), True),
        StructField("team_name", StringType(), True)
    ]), True),
    StructField("settings", ArrayType(StringType()), True),  # Corrected line
    StructField("user_id", StringType(), True),
    StructField("_year", IntegerType(), True),
    StructField("_league_id", StringType(), True),
    StructField("_matchup_week", IntegerType(), True),
    StructField("_ingested_ts", TimestampType(), True)
])

    return spark.readStream.schema(users_schema).json('/mnt/databricks/sleeper/stg/users/*')

# COMMAND ----------

# MAGIC %md
# MAGIC ### bronze drafts

# COMMAND ----------

@dlt.table(
  comment="The raw drafts from the Sleeper API",
  table_properties={
    "myCompanyPipeline.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def bronze_drafts():
  drafts_schema = StructType([
    StructField("created", LongType(), True),
    StructField("creators", ArrayType(StringType()), True),
    StructField("draft_id", StringType(), True),
    StructField("draft_order", MapType(StringType(), IntegerType()), True),
    StructField("last_message_id", StringType(), True),
    StructField("last_message_time", LongType(), True),
    StructField("last_picked", LongType(), True),
    StructField("league_id", StringType(), True),
    StructField("metadata", StructType([
        StructField("description", StringType(), True),
        StructField("name", StringType(), True),
        StructField("scoring_type", StringType(), True)
    ]), True),
    StructField("season", StringType(), True),
    StructField("season_type", StringType(), True),
    StructField("settings", StructType([
        StructField("alpha_sort", IntegerType(), True),
        StructField("autopause_enabled", IntegerType(), True),
        StructField("autopause_end_time", IntegerType(), True),
        StructField("autopause_start_time", IntegerType(), True),
        StructField("autostart", IntegerType(), True),
        StructField("cpu_autopick", IntegerType(), True),
        StructField("enforce_position_limits", IntegerType(), True),
        StructField("nomination_timer", IntegerType(), True),
        StructField("pick_timer", IntegerType(), True),
        StructField("player_type", IntegerType(), True),
        StructField("reversal_round", IntegerType(), True),
        StructField("rounds", IntegerType(), True),
        StructField("slots_bn", IntegerType(), True),
        StructField("slots_def", IntegerType(), True),
        StructField("slots_flex", IntegerType(), True),
        StructField("slots_k", IntegerType(), True),
        StructField("slots_qb", IntegerType(), True),
        StructField("slots_rb", IntegerType(), True),
        StructField("slots_te", IntegerType(), True),
        StructField("slots_wr", IntegerType(), True),
        StructField("teams", IntegerType(), True)
    ]), True),
    StructField("sport", StringType(), True),
    StructField("start_time", LongType(), True),
    StructField("status", StringType(), True),
    StructField("type", StringType(), True),
    StructField("_year", IntegerType(), True),
    StructField("_league_id", StringType(), True),
    StructField("_matchup_week", IntegerType(), True),
    StructField("_ingested_ts", TimestampType(), True)
  ])
  return spark.readStream.schema(drafts_schema).json('/mnt/databricks/sleeper/stg/drafts/*')

# COMMAND ----------

# MAGIC %md
# MAGIC ### bronze draft picks

# COMMAND ----------

@dlt.table(
  comment="The raw draft picks from the Sleeper API",
  table_properties={
    "myCompanyPipeline.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def bronze_drafts_picks():
    draft_picks_schema = StructType([
        StructField("draft_id", StringType(), True),
        StructField("draft_slot", IntegerType(), True),
        StructField("is_keeper", StringType(), True),  # Assuming null can be represented as StringType (use NullType in PySpark 3+ if needed)
        StructField("metadata", StructType([
            StructField("first_name", StringType(), True),
            StructField("injury_status", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("news_updated", StringType(), True),  # Assuming timestamp is represented as a string
            StructField("number", StringType(), True),
            StructField("player_id", StringType(), True),
            StructField("position", StringType(), True),
            StructField("sport", StringType(), True),
            StructField("status", StringType(), True),
            StructField("team", StringType(), True),
            StructField("team_abbr", StringType(), True),
            StructField("team_changed_at", StringType(), True),
            StructField("years_exp", StringType(), True)
        ]), True),
        StructField("pick_no", IntegerType(), True),
        StructField("picked_by", StringType(), True),
        StructField("player_id", StringType(), True),
        StructField("reactions", StringType(), True),  # Assuming null can be StringType or MapType if later defined
        StructField("roster_id", IntegerType(), True),
        StructField("round", IntegerType(), True),
        StructField("_year", IntegerType(), True),
        StructField("_league_id", StringType(), True),
        StructField("_matchup_week", IntegerType(), True),
        StructField("_ingested_ts", TimestampType(), True)
    ])
    return spark.readStream.schema(draft_picks_schema).json('/mnt/databricks/sleeper/stg/drafts_picks/*')

# COMMAND ----------

# MAGIC %md
# MAGIC ### bronze_players

# COMMAND ----------

@dlt.table(
  comment="The raw players from the Sleeper API",
  table_properties={
    "myCompanyPipeline.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def bronze_players():
    players_schema = StructType([
    StructField("birth_state", StringType(), True),
    StructField("opta_id", StringType(), True),
    StructField("pandascore_id", StringType(), True),
    StructField("injury_status", StringType(), True),
    StructField("espn_id", IntegerType(), True),  # Integer now
    StructField("position", StringType(), True),  # WR, String
    StructField("news_updated", LongType(), True),  # Timestamp in ms
    StructField("sport", StringType(), True),
    StructField("fantasy_data_id", IntegerType(), True),
    StructField("sportradar_id", StringType(), True),
    StructField("team", StringType(), True),
    StructField("birth_date", StringType(), True),  # String but represents a date
    StructField("team_abbr", StringType(), True),
    StructField("gsis_id", StringType(), True),  # gsis_id as String
    StructField("rotoworld_id", IntegerType(), True),
    StructField("injury_start_date", StringType(), True),
    StructField("depth_chart_position", StringType(), True),
    StructField("search_rank", IntegerType(), True),
    StructField("practice_participation", StringType(), True),
    StructField("player_id", StringType(), True),
    StructField("yahoo_id", IntegerType(), True),  # Integer now
    StructField("stats_id", IntegerType(), True),  # Integer now
    StructField("search_full_name", StringType(), True),
    StructField("search_first_name", StringType(), True),
    StructField("practice_description", StringType(), True),
    StructField("number", IntegerType(), True),  # Integer
    StructField("injury_body_part", StringType(), True),
    StructField("oddsjam_id", StringType(), True),
    StructField("depth_chart_order", StringType(), True),
    StructField("swish_id", IntegerType(), True),  # Integer now
    StructField("first_name", StringType(), True),
    StructField("age", IntegerType(), True),  # Age as Integer
    StructField("status", StringType(), True),
    StructField("height", StringType(), True),  # Keep as String for now
    StructField("high_school", StringType(), True),
    StructField("birth_country", StringType(), True),
    StructField("metadata", StructType([              # Struct for metadata
        StructField("rookie_year", StringType(), True),
        StructField("channel_id", StringType(), True)
    ]), True),
    StructField("weight", StringType(), True),  # Keep as String for now
    StructField("years_exp", IntegerType(), True),  # Integer for experience
    StructField("search_last_name", StringType(), True),
    StructField("rotowire_id", IntegerType(), True),
    StructField("full_name", StringType(), True),
    StructField("injury_notes", StringType(), True),
    StructField("active", BooleanType(), True),  # Boolean
    StructField("hashtag", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("college", StringType(), True),
    StructField("fantasy_positions", ArrayType(StringType()), True),  # Array of Strings
    StructField("competitions", ArrayType(StringType()), True),  # Empty array
    StructField("birth_city", StringType(), True),
    StructField("_year", IntegerType(), True),
    StructField("_league_id", StringType(), True),
    StructField("_matchup_week", IntegerType(), True),
    StructField("_ingested_ts", TimestampType(), True)
    ])
    return spark.readStream.schema(players_schema).json('/mnt/databricks/sleeper/stg/players/*')

# COMMAND ----------

# MAGIC %md
# MAGIC ##silver

# COMMAND ----------

# MAGIC %md
# MAGIC ### silver players dim

# COMMAND ----------

dlt.create_streaming_table(name="silver_players_dim", comment="SCD2 on player master data")
dlt.apply_changes(
    target           = "silver_players_dim",
    source           = "bronze_players",
    keys             = ["player_id"],
    sequence_by      = col("_ingested_ts"),      # your bronze ingestion timestamp
    except_column_list = ["_ingested_ts"],       # drop ts from final dim
    stored_as_scd_type = "2"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### silver_rosters_players_snapshot

# COMMAND ----------

@dlt.table(
  name="silver_rosters_players_snapshot",
  comment="Weekly snapshot of the players in each roster, their starter status, and their nickname metadata",
  partition_cols=["_year", "_matchup_week"]
)
def silver_rosters_players_snapshot():
    return (
        dlt.read_stream("bronze_rosters")
        .withColumn("player_id", explode("players"))
        .withColumn("is_starter", expr("array_contains(starters, player_id)"))
        .withColumn("player_nickname", expr("metadata['p_nick_' || player_id]"))
        .select(
            "owner_id",
            "roster_id",
            "player_id",
            "is_starter",
            "player_nickname",
            "_league_id",
            "_matchup_week",
            "_year",
            "_ingested_ts"
        )
        .withColumn("_snapshot_ts", current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC #### silver_stg_rosters

# COMMAND ----------

@dlt.view(name="silver_stg_rosters", comment="Flattened & cleansed roster stats")
def silver_stg_rosters():
    return (
        dlt.read_stream("bronze_rosters")
        .withColumn("streak", expr("metadata['streak']"))
        .withColumn("record", expr("metadata['record']"))
        .withColumn("wins", expr("settings['wins']"))
        .withColumn("losses", expr("settings['losses']"))
        .withColumn("ties", expr("settings['ties']"))
        .withColumn("fpts", expr("settings['fpts'] + settings['fpts_decimal'] / 100"))
        .withColumn("fpts_against", expr("settings['fpts_against'] + settings['fpts_against_decimal'] / 100"))
        .withColumn("total_moves", expr("settings['total_moves']"))
        .withColumn("waiver_budget_used", expr("settings['waiver_budget_used']"))
        .withColumn("waiver_position", expr("settings['waiver_position']"))
        .select(
            "owner_id",
            "roster_id",
            "streak",
            "record",
            "wins",
            "losses",
            "ties",
            "fpts",
            "fpts_against",
            "total_moves",
            "waiver_budget_used",
            "waiver_position",
            "_league_id",
            "_matchup_week",
            "_year",
            "_ingested_ts",
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### silver_rosters_dim

# COMMAND ----------

dlt.create_streaming_table(name="silver_rosters_dim", comment="SCD2 on cleansed rosters")
dlt.apply_changes(
    target           = "silver_rosters_dim",
    source           = "silver_stg_rosters",
    keys             = ["_league_id", "owner_id", "roster_id"],
    sequence_by      = col("_ingested_ts"),      # your bronze ingestion timestamp
    except_column_list = ["_ingested_ts"],       # drop ts from final dim
    stored_as_scd_type = "2"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### silver_matchups_fact

# COMMAND ----------

@dlt.table(
  name="silver_matchups_fact",
  comment="Flattened matchup outcomes",
  partition_cols=["_league_id", "_matchup_week",]
)
def silver_matchups_fact():
    return dlt.read_stream('bronze_matchups')\
    .select(
        "matchup_id",
        "roster_id",
        "points",
        "_league_id",
        "_matchup_week",
        "_year",
        "_ingested_ts"
    ).withColumn("_snapshot_ts", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ### silver_stg_matchups_players

# COMMAND ----------

@dlt.view(name="silver_stg_matchups_players", comment="Flattened & cleansed matchups players stats")
def silver_stg_matchups_players():
    return dlt.read_stream('bronze_matchups') \
        .withColumn("player_id", explode(col("players"))) \
        .withColumn("is_starter", array_contains(col("starters"), col("player_id"))) \
        .withColumn("player_points", col("players_points")[col("player_id")]) \
        .select(
            "roster_id",
            "matchup_id",
            "player_id",
            "player_points",
            "is_starter",
            "_league_id",
            "_matchup_week",
            "_year",
            "_ingested_ts"
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ### silver_matchups_players_dim

# COMMAND ----------

dlt.create_streaming_table(name="silver_matchups_players_dim", comment="SCD2 on cleansed matchups players")
dlt.apply_changes(
    target           = "silver_matchups_players_dim",
    source           = "silver_stg_matchups_players",
    keys             = ["_league_id", "player_id"],
    sequence_by      = col("_ingested_ts"),      # your bronze ingestion timestamp
    except_column_list = ["_ingested_ts"],       # drop ts from final dim
    stored_as_scd_type = "2"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### silver_stg_users

# COMMAND ----------

@dlt.view(name="silver_stg_users", comment="Cleansed users and teams")
def silver_stg_users():
    return (
        dlt.read_stream('bronze_users')
        .withColumnRenamed("display_name", "owner_name")
        .withColumnRenamed("user_id", "owner_id")
        .withColumnRenamed("is_owner", "is_commissioner")
        .withColumn("team_name", col("metadata.team_name"))
        .select(
            "owner_id",
            "owner_name",
            "is_bot",
            "is_commissioner",
            "team_name",
            "_league_id",
            "_matchup_week",
            "_year",
            "_ingested_ts"
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### silver_users_dim

# COMMAND ----------

dlt.create_streaming_table(name="silver_users_dim", comment="SCD2 on cleansed users")
dlt.apply_changes(
    target           = "silver_users_dim",
    source           = "silver_stg_users",
    keys             = ["owner_id"],
    sequence_by      = col("_ingested_ts"),      # your bronze ingestion timestamp
    except_column_list = ["_ingested_ts"],       # drop ts from final dim
    stored_as_scd_type = "2"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## gold

# COMMAND ----------

# MAGIC %md
# MAGIC ### gold_weekly_performance_fact

# COMMAND ----------

# def fact_weekly_performance():
#     snap = dlt.read("silver_roster_snapshot")
#     # aggregate starter vs total points
#     agg = snap.groupBy("roster_id","week").agg(
#       sum("player_points").alias("total_points"),
#       sum(when(col("is_starter"), col("player_points")).otherwise(0)).alias("starters_points")
#     ).withColumn(
#       "bench_points", col("total_points") - col("starters_points")
#     ).withColumn(
#       "bench_efficiency", col("bench_points") / col("total_points") * 100
#     )

#     # wins/losses from matchup fact
#     match = dlt.read("silver_fact_matchup") \
#       .select(
#         "roster_id","week",
#         when(col("winner_id")==col("roster_id"), 1).otherwise(0).alias("wins"),
#         when(col("winner_id")!=col("roster_id"), 1).otherwise(0).alias("losses")
#       )
#     wl = match.groupBy("roster_id","week").agg(
#       sum("wins").alias("wins"),
#       sum("losses").alias("losses")
#     )

#     perf = agg.join(wl, ["roster_id","week"], "left")

#     return perf.select(
#       "roster_id","week","total_points","starters_points",
#       "bench_points","bench_efficiency","wins","losses",
#       # placeholders – replace with your actual logic / joins
#       lit(None).cast("double").alias("roster_strength"),
#       lit(None).cast("double").alias("management_score")
#     )
