# Databricks notebook source
# MAGIC %md
# MAGIC # Sleeper Information

# COMMAND ----------

import dlt
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType, MapType, DoubleType, NullType, FloatType, BooleanType, TimestampType, LongType

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
    StructField("birth_city", StringType(), True)
    ])
    return spark.readStream.schema(players_schema).json('/mnt/databricks/sleeper/stg/players/*')
