# Databricks notebook source
import dlt
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType, MapType, DoubleType, NullType, FloatType, BooleanType, TimestampType

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
        StructField("round", IntegerType(), True)
    ])
  return spark.readStream.schema(draft_picks_schema).json('/mnt/databricks/sleeper/stg/drafts_picks/*')
