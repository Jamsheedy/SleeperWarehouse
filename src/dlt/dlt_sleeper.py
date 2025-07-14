# Databricks notebook source
# MAGIC %md
# MAGIC # Sleeper Information

# COMMAND ----------

import dlt
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType, MapType, DoubleType, NullType, FloatType, BooleanType, TimestampType, LongType
from pyspark.sql.functions import col, expr, explode, current_timestamp, sum, when, lit, array_contains, coalesce, concat_ws
from pyspark.sql.window import Window
import pyspark.sql.functions as F

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
    "SleeperWarehouse.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  },
  partition_cols=["_year", "_matchup_week"]
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

  return spark.readStream.schema(rosters_schema).json('/mnt/databricks/sleeper/stg/rosters/')

# COMMAND ----------

# MAGIC %md
# MAGIC ### bronze_users

# COMMAND ----------

@dlt.table(
  comment="The raw users from the Sleeper API",
  table_properties={
    "SleeperWarehouse.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  },
  partition_cols=["_year", "_matchup_week"]
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

    return spark.readStream.schema(users_schema).json('/mnt/databricks/sleeper/stg/users/')

# COMMAND ----------

# MAGIC %md
# MAGIC ### bronze_matchups

# COMMAND ----------

@dlt.table(
  comment="The raw matchups from the Sleeper API",
  table_properties={
    "SleeperWarehouse.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  },
  partition_cols=["_year", "_matchup_week"]
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

    return spark.readStream.schema(matchups_schema).json('/mnt/databricks/sleeper/stg/matchups/')

# COMMAND ----------

# MAGIC %md
# MAGIC ### bronze_losers_bracket

# COMMAND ----------

@dlt.table(
  comment="The raw playoffs loser bracket from the Sleeper API",
  table_properties={
    "SleeperWarehouse.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  },
  partition_cols=["_year", "_matchup_week"]
)
def bronze_losers_bracket():
  bracket_schema = StructType([
    StructField("p", IntegerType(), True),
    StructField("m", IntegerType(), True),
    StructField("r", IntegerType(), True),
    StructField("l", IntegerType(), True),
    StructField("w", IntegerType(), True),
    StructField("t1", IntegerType(), True),
    StructField("t2", IntegerType(), True),
    StructField("t2_from", StructType([
      StructField("l", IntegerType(), True)
    ]), True),
    StructField("t1_from", StructType([
      StructField("l", IntegerType(), True)
    ]), True),
    StructField("_year", IntegerType(), True),
    StructField("_league_id", StringType(), True),
    StructField("_matchup_week", IntegerType(), True),
    StructField("_ingested_ts", TimestampType(), True)
  ])
  return spark.readStream.schema(bracket_schema).json('/mnt/databricks/sleeper/stg/losers_bracket/')

# COMMAND ----------

# MAGIC %md
# MAGIC ### bronze_winners_bracket

# COMMAND ----------

@dlt.table(
  comment="The raw playoffs loser bracket from the Sleeper API",
  table_properties={
    "SleeperWarehouse.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  },
  partition_cols=["_year", "_matchup_week"]
)
def bronze_winners_bracket():
  bracket_schema = StructType([
    StructField("p", IntegerType(), True),
    StructField("m", IntegerType(), True),
    StructField("r", IntegerType(), True),
    StructField("l", IntegerType(), True),
    StructField("w", IntegerType(), True),
    StructField("t1", IntegerType(), True),
    StructField("t2", IntegerType(), True),
    StructField("t2_from", StructType([
      StructField("l", IntegerType(), True)
    ]), True),
    StructField("t1_from", StructType([
      StructField("l", IntegerType(), True)
    ]), True),
    StructField("_year", IntegerType(), True),
    StructField("_league_id", StringType(), True),
    StructField("_matchup_week", IntegerType(), True),
    StructField("_ingested_ts", TimestampType(), True)
  ])
  return spark.readStream.schema(bracket_schema).json('/mnt/databricks/sleeper/stg/winners_bracket/')

# COMMAND ----------

# MAGIC %md
# MAGIC ### bronze drafts

# COMMAND ----------

@dlt.table(
  comment="The raw drafts from the Sleeper API",
  table_properties={
    "SleeperWarehouse.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  },
  partition_cols=["_year", "_matchup_week"]
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
  return spark.readStream.schema(drafts_schema).json('/mnt/databricks/sleeper/stg/drafts/')

# COMMAND ----------

# MAGIC %md
# MAGIC ### bronze draft picks

# COMMAND ----------

@dlt.table(
  comment="The raw draft picks from the Sleeper API",
  table_properties={
    "SleeperWarehouse.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  },
  partition_cols=["_year", "_matchup_week"]
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
    return spark.readStream.schema(draft_picks_schema).json('/mnt/databricks/sleeper/stg/drafts_picks/')

# COMMAND ----------

# MAGIC %md
# MAGIC ### bronze_players

# COMMAND ----------

@dlt.table(
  comment="The raw players from the Sleeper API",
  table_properties={
    "SleeperWarehouse.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  },
  partition_cols=["_year", "_matchup_week"]
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
    StructField("_matchup_week", IntegerType(), True),
    StructField("_ingested_ts", TimestampType(), True)
    ])
    return spark.readStream.schema(players_schema).json('/mnt/databricks/sleeper/stg/players/')

# COMMAND ----------

# MAGIC %md
# MAGIC ### bronze_players_trend_add

# COMMAND ----------

@dlt.table(
  comment="The raw players from the Sleeper API",
  table_properties={
    "SleeperWarehouse.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  },
  partition_cols=["_year", "_matchup_week"]
)
def bronze_players_trend_add():
    players_added_schema = StructType([
      StructField("count", IntegerType(), True),
      StructField("player_id", StringType(), True),
      StructField("_year", IntegerType(), True),
      StructField("_matchup_week", IntegerType(), True),
      StructField("_ingested_ts", TimestampType(), True)
    ])

    return spark.readStream.schema(players_added_schema).json('/mnt/databricks/sleeper/stg/players_trend_add/')

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ### bronze_players_trend_drop

# COMMAND ----------

@dlt.table(
  comment="The raw players from the Sleeper API",
  table_properties={
    "SleeperWarehouse.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  },
  partition_cols=["_year", "_matchup_week"]
)
def bronze_players_trend_drop():
    players_drop_schema = StructType([
      StructField("count", IntegerType(), True),
      StructField("player_id", StringType(), True),
      StructField("_year", IntegerType(), True),
      StructField("_matchup_week", IntegerType(), True),
      StructField("_ingested_ts", TimestampType(), True)
    ])

    return spark.readStream.schema(players_drop_schema).json('/mnt/databricks/sleeper/stg/players_trend_drop/')

# COMMAND ----------

# MAGIC %md
# MAGIC ### bronze_league

# COMMAND ----------

@dlt.table(
  comment="The raw league information from the Sleeper API",
  table_properties={
    "SleeperWarehouse.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  },
  partition_cols=["_year", "_matchup_week"]
)
def bronze_leagues():
    league_schema = StructType([
        StructField("name", StringType(), True),
        StructField("status", StringType(), True),
        StructField("metadata", StructType([
            StructField("auto_continue", StringType(), True),
            StructField("keeper_deadline", StringType(), True),
            StructField("latest_league_winner_roster_id", StringType(), True),
            StructField("promo_commish", StringType(), True),
            StructField("promo_id", StringType(), True),
            StructField("promo_nfl_commish_100", StringType(), True)
        ]), True),
        StructField("settings", StructType([
            StructField("best_ball", IntegerType(), True),
            StructField("last_report", IntegerType(), True),
            StructField("waiver_budget", IntegerType(), True),
            StructField("disable_adds", IntegerType(), True),
            StructField("capacity_override", IntegerType(), True),
            StructField("taxi_deadline", IntegerType(), True),
            StructField("draft_rounds", IntegerType(), True),
            StructField("reserve_allow_na", IntegerType(), True),
            StructField("start_week", IntegerType(), True),
            StructField("playoff_seed_type", IntegerType(), True),
            StructField("playoff_teams", IntegerType(), True),
            StructField("veto_votes_needed", IntegerType(), True),
            StructField("squads", IntegerType(), True),
            StructField("num_teams", IntegerType(), True),
            StructField("daily_waivers_hour", IntegerType(), True),
            StructField("playoff_type", IntegerType(), True),
            StructField("taxi_slots", IntegerType(), True),
            StructField("sub_start_time_eligibility", IntegerType(), True),
            StructField("last_scored_leg", IntegerType(), True),
            StructField("daily_waivers_days", IntegerType(), True),
            StructField("playoff_week_start", IntegerType(), True),
            StructField("waiver_clear_days", IntegerType(), True),
            StructField("reserve_allow_doubtful", IntegerType(), True),
            StructField("commissioner_direct_invite", IntegerType(), True),
            StructField("veto_auto_poll", IntegerType(), True),
            StructField("reserve_allow_dnr", IntegerType(), True),
            StructField("taxi_allow_vets", IntegerType(), True),
            StructField("waiver_day_of_week", IntegerType(), True),
            StructField("playoff_round_type", IntegerType(), True),
            StructField("reserve_allow_out", IntegerType(), True),
            StructField("reserve_allow_sus", IntegerType(), True),
            StructField("veto_show_votes", IntegerType(), True),
            StructField("trade_deadline", IntegerType(), True),
            StructField("taxi_years", IntegerType(), True),
            StructField("daily_waivers", IntegerType(), True),
            StructField("disable_trades", IntegerType(), True),
            StructField("pick_trading", IntegerType(), True),
            StructField("type", IntegerType(), True),
            StructField("max_keepers", IntegerType(), True),
            StructField("waiver_type", IntegerType(), True),
            StructField("max_subs", IntegerType(), True),
            StructField("league_average_match", IntegerType(), True),
            StructField("trade_review_days", IntegerType(), True),
            StructField("bench_lock", IntegerType(), True),
            StructField("offseason_adds", IntegerType(), True),
            StructField("leg", IntegerType(), True),
            StructField("reserve_slots", IntegerType(), True),
            StructField("reserve_allow_cov", IntegerType(), True),
            StructField("daily_waivers_last_ran", IntegerType(), True)
        ]), True),
        StructField("avatar", StringType(), True),
        StructField("company_id", StringType(), True),
        StructField("sport", StringType(), True),
        StructField("season_type", StringType(), True),
        StructField("season", StringType(), True),
        StructField("shard", IntegerType(), True),
        StructField("scoring_settings", StructType([
            StructField("sack", IntegerType(), True),
            StructField("fgm_40_49", IntegerType(), True),
            StructField("bonus_rec_yd_100", IntegerType(), True),
            StructField("bonus_rush_yd_100", IntegerType(), True),
            StructField("pass_int", IntegerType(), True),
            StructField("pts_allow_0", IntegerType(), True),
            StructField("bonus_pass_yd_400", IntegerType(), True),
            StructField("pass_2pt", IntegerType(), True),
            StructField("st_td", IntegerType(), True),
            StructField("rec_td", IntegerType(), True),
            StructField("fgm_30_39", IntegerType(), True),
            StructField("xpmiss", IntegerType(), True),
            StructField("rush_td", IntegerType(), True),
            StructField("rec_2pt", IntegerType(), True),
            StructField("st_fum_rec", IntegerType(), True),
            StructField("fgmiss", IntegerType(), True),
            StructField("ff", IntegerType(), True),
            StructField("rec", IntegerType(), True),
            StructField("pts_allow_14_20", IntegerType(), True),
            StructField("def_2pt", IntegerType(), True),
            StructField("fgm_0_19", IntegerType(), True),
            StructField("int", IntegerType(), True),
            StructField("def_st_fum_rec", IntegerType(), True),
            StructField("fum_lost", IntegerType(), True),
            StructField("pts_allow_1_6", IntegerType(), True),
            StructField("fgm_20_29", IntegerType(), True),
            StructField("pts_allow_21_27", IntegerType(), True),
            StructField("bonus_pass_yd_300", IntegerType(), True),
            StructField("xpm", IntegerType(), True),
            StructField("rush_2pt", IntegerType(), True),
            StructField("fum_rec", IntegerType(), True),
            StructField("bonus_rec_yd_200", IntegerType(), True),
            StructField("def_st_td", IntegerType(), True),
            StructField("fgm_50p", IntegerType(), True),
            StructField("def_td", IntegerType(), True),
            StructField("bonus_rush_yd_200", IntegerType(), True),
            StructField("safe", IntegerType(), True),
            StructField("pass_yd", FloatType(), True),
            StructField("blk_kick", IntegerType(), True),
            StructField("pass_td", IntegerType(), True),
            StructField("rush_yd", FloatType(), True),
            StructField("fum", IntegerType(), True),
            StructField("pts_allow_28_34", IntegerType(), True),
            StructField("pts_allow_35p", IntegerType(), True),
            StructField("fum_rec_td", IntegerType(), True),
            StructField("rec_yd", FloatType(), True),
            StructField("def_st_ff", IntegerType(), True),
            StructField("pts_allow_7_13", IntegerType(), True),
            StructField("st_ff", IntegerType(), True)
        ]), True),
        StructField("last_message_id", StringType(), True),
        StructField("draft_id", StringType(), True),
        StructField("last_author_avatar", StringType(), True),
        StructField("last_author_display_name", StringType(), True),
        StructField("last_author_id", StringType(), True),
        StructField("last_author_is_bot", BooleanType(), True),
        StructField("last_message_attachment", StringType(), True),
        StructField("last_message_text_map", StringType(), True),
        StructField("last_message_time", LongType(), True),
        StructField("last_pinned_message_id", StringType(), True),
        StructField("last_read_id", StringType(), True),
        StructField("league_id", StringType(), True),
        StructField("previous_league_id", StringType(), True),
        StructField("roster_positions", ArrayType(StringType()), True),
        StructField("bracket_id", LongType(), True),
        StructField("bracket_overrides_id", StringType(), True),
        StructField("group_id", StringType(), True),
        StructField("loser_bracket_id", LongType(), True),
        StructField("loser_bracket_overrides_id", StringType(), True),
        StructField("total_rosters", IntegerType(), True),
        StructField("_year", IntegerType(), True),
        StructField("_league_id", StringType(), True),
        StructField("_matchup_week", IntegerType(), True),
        StructField("_ingested_ts", TimestampType(), True)
    ])

    return spark.readStream.schema(league_schema).json('/mnt/databricks/sleeper/stg/league/')

# COMMAND ----------

# MAGIC %md
# MAGIC ### bronze_transactions

# COMMAND ----------

@dlt.table(
  comment="The raw league information from the Sleeper API",
  table_properties={
    "SleeperWarehouse.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  },
  partition_cols=["_year", "_matchup_week"]
)
def bronze_transactions():
    transaction_schema = StructType([
        StructField("status", StringType(), True),
        StructField("type", StringType(), True),
        StructField("metadata", StructType([
            StructField("notes", StringType(), True)
        ]), True),
        StructField("created", LongType(), True),
        StructField("settings", StructType([
            StructField("priority", IntegerType(), True),
            StructField("seq", IntegerType(), True),
            StructField("waiver_bid", IntegerType(), True)
        ]), True),
        StructField("leg", IntegerType(), True),
        StructField("draft_picks", ArrayType(StringType()), True),
        StructField("creator", StringType(), True),
        StructField("transaction_id", StringType(), True),
        StructField("adds", MapType(StringType(), IntegerType()), True),
        StructField("drops", MapType(StringType(), IntegerType()), True),
        StructField("consenter_ids", ArrayType(IntegerType()), True),
        StructField("roster_ids", ArrayType(IntegerType()), True),
        StructField("status_updated", LongType(), True),
        StructField("waiver_budget", ArrayType(StringType()), True),
        StructField("_year", IntegerType(), True),
        StructField("_league_id", StringType(), True),
        StructField("_matchup_week", IntegerType(), True),
        StructField("_ingested_ts", TimestampType(), True)
    ])

    return spark.readStream.schema(transaction_schema).json('/mnt/databricks/sleeper/stg/transactions/')

# COMMAND ----------

# MAGIC %md
# MAGIC ##silver

# COMMAND ----------

# MAGIC %md
# MAGIC ### silver players dim

# COMMAND ----------

dlt.create_streaming_table(
    name="silver_players_dim",
    comment="SCD2 on player master data",
    table_properties={
        "SleeperWarehouse.quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    },
    partition_cols=["_year", "_matchup_week"]
)
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
  table_properties={
        "SleeperWarehouse.quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    },
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
# MAGIC ### silver_stg_rosters

# COMMAND ----------

@dlt.view(name="silver_stg_rosters", comment="Flattened & cleansed roster stats")
def silver_stg_rosters():
    return (
        dlt.read_stream("bronze_rosters")
        .withColumn("team_streak", expr("metadata['streak']"))
        .withColumn("team_record", expr("metadata['record']"))
        .withColumn("team_total_wins", expr("settings['wins']"))
        .withColumn("team_total_losses", expr("settings['losses']"))
        .withColumn("team_total_ties", expr("settings['ties']"))
        .withColumn("team_total_fpts", expr("settings['fpts'] + settings['fpts_decimal'] / 100"))
        .withColumn("team_total_fpts_against", expr("settings['fpts_against'] + settings['fpts_against_decimal'] / 100"))
        .withColumn("team_total_moves", expr("settings['total_moves']"))
        .withColumn("team_waiver_budget_used", expr("settings['waiver_budget_used']"))
        .withColumn("team_waiver_position", expr("settings['waiver_position']"))
        .select(
            "owner_id",
            "roster_id",
            "team_streak",
            "team_record",
            "team_total_wins",
            "team_total_losses",
            "team_total_ties",
            "team_total_fpts",
            "team_total_fpts_against",
            "team_total_moves",
            "team_waiver_budget_used",
            "team_waiver_position",
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
        col("points").alias("matchup_points"),
        "_league_id",
        "_matchup_week",
        "_year",
        "_ingested_ts"
    ).withColumn("_snapshot_ts", current_timestamp())

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ### silver_drafts_picks_fact

# COMMAND ----------

@dlt.table(
  name="silver_drafts_picks_fact",
  comment="Flattened draft pick information",
  partition_cols=["_league_id", "_matchup_week",]
)
def silver_drafts_picks_fact():
    return dlt.read_stream('bronze_drafts_picks')\
        .select(
            'draft_id',
            'draft_slot',
            'is_keeper',
            'metadata.*',
            'pick_no',
            'picked_by',
            'reactions',
            'roster_id',
            'round',
            '_year',
            '_league_id',
            '_matchup_week',
            '_ingested_ts'
        )

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
    keys             = ["_league_id","owner_id"],
    sequence_by      = col("_ingested_ts"),      # your bronze ingestion timestamp
    except_column_list = ["_ingested_ts"],       # drop ts from final dim
    stored_as_scd_type = "2"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### silver_model_player_performances

# COMMAND ----------

@dlt.table(
  name="silver_model_player_performances",
  comment="matchups",
  partition_cols=["_league_id", "_matchup_week",]
)
def silver_model_player_performances():
    df_matchups_players_dim = dlt.read('silver_matchups_players_dim')
    df_players_dim = dlt.read('silver_players_dim')

    df_players_dim = df_players_dim.select(
        "player_id",
        "_year",
        "_matchup_week",
        coalesce(col("full_name"), col("last_name")).alias("player_name"),
        col("position").alias("player_position"),
        col("team").alias("nfl_team"),
        "years_exp",
        "injury_status",
        concat_ws(" ", col("injury_body_part"), col("injury_notes")).alias("injury_notes"),
        "college",
        when(col("years_exp") == 1, True).otherwise(False).alias("is_rookie")
    )

    return df_matchups_players_dim.join(
        df_players_dim,
        (df_matchups_players_dim.player_id == df_players_dim.player_id) &
        (df_matchups_players_dim._year == df_players_dim._year) &
        (df_matchups_players_dim._matchup_week == df_players_dim._matchup_week)
    ).select(
        "roster_id",
        "matchup_id",
        df_matchups_players_dim.player_id,
        "player_name",
        "player_position",
        "nfl_team",
        "player_points",
        "is_starter",
        "years_exp",
        "is_rookie",
        "injury_status",
        "injury_notes",
        "college",
        df_matchups_players_dim._league_id,
        df_matchups_players_dim._matchup_week,
        df_matchups_players_dim._year,
        df_matchups_players_dim._ingested_ts
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### silver_model_roster_results

# COMMAND ----------

@dlt.table(
  name="silver_model_roster_results",
  comment="matchups",
  partition_cols=["_league_id", "_matchup_week",]
)
def silver_model_roster_results():
    df_matchups_fact = dlt.read('silver_matchups_fact')
    df_rosters_dim = dlt.read('silver_rosters_dim')
    df_users_dim = dlt.read('silver_users_dim')

    return df_matchups_fact.join(
        df_rosters_dim,
        (df_matchups_fact._league_id == df_rosters_dim._league_id) &
        (df_matchups_fact.roster_id == df_rosters_dim.roster_id) &
        (df_matchups_fact._matchup_week == df_rosters_dim._matchup_week)
    ).join(
        df_users_dim,
        (df_matchups_fact._league_id == df_users_dim._league_id) &
        (df_rosters_dim.owner_id == df_users_dim.owner_id) &
        (df_matchups_fact._matchup_week == df_users_dim._matchup_week)
    ).select(
        "matchup_id",
        df_matchups_fact.roster_id,
        df_users_dim.owner_id,
        df_users_dim.owner_name,
        df_users_dim.is_commissioner,
        df_users_dim.team_name,
        df_matchups_fact.matchup_points,
        df_rosters_dim.team_streak,
        df_rosters_dim.team_record,
        df_rosters_dim.team_total_wins,
        df_rosters_dim.team_total_losses,
        df_rosters_dim.team_total_ties,
        df_rosters_dim.team_total_fpts,
        df_rosters_dim.team_total_fpts_against,
        df_rosters_dim.team_waiver_budget_used,
        df_rosters_dim.team_waiver_position,
        df_matchups_fact._league_id,
        df_matchups_fact._matchup_week,
        df_matchups_fact._year,
        df_matchups_fact._ingested_ts
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### silver_model_drafts

# COMMAND ----------

@dlt.table(
  name="silver_model_drafts",
  comment="user draft picks",
  partition_cols=["_league_id", "_matchup_week",]
)
def silver_model_drafts():
    draft_picks_df = dlt.read('silver_drafts_picks_fact')
    users_df = dlt.read('silver_users_dim')

    joined_df = draft_picks_df.alias('picks').join(
        users_df.alias('users'),
        (draft_picks_df['_league_id'] == users_df['_league_id']) &
        (draft_picks_df['_matchup_week'] == users_df['_matchup_week']) &
        (draft_picks_df['picked_by'] == users_df['owner_id']),
        'inner'
    )

    return joined_df.select(
        'draft_id',
        'owner_id',
        'owner_name',
        'team_name',
        'roster_id',
        'draft_slot',
        'round',
        'pick_no',
        'player_id',
        'first_name',
        'last_name',
        'team',
        'position',
        'years_exp',
        'injury_status',
        'picks._league_id',
        'picks._year',
        'picks._matchup_week',
        'picks._ingested_ts'
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## gold

# COMMAND ----------

# MAGIC %md
# MAGIC ### gold_power_ranks
# MAGIC Tribute: https://stackoverflow.com/questions/62033781/how-can-i-use-pysparks-window-function-to-model-exponential-decay

# COMMAND ----------

from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.window import Window
from pyspark.sql import functions as F

@dlt.table(
  name="gold_power_ranks",
  comment="Aggregated and ranked rosters with isolated performance metric for that given week. Stateful exponential‐decay of week_power_points → power_rank_points, safe for full‐refresh",
  partition_cols=["_league_id", "_matchup_week"]
)
def gold_weekly_performance_ranks():
    df_roster_results = dlt.read('silver_model_roster_results')
    df_player_performances = dlt.read('silver_model_player_performances')

    # join rosters with player information to get complete breakdown of the matchup week
    df_joined_results = df_roster_results.alias("roster").join(
        df_player_performances.alias("performance"),
        (col("roster._league_id") == col("performance._league_id")) &
        (col("roster.roster_id") == col("performance.roster_id")) &
        (col("roster.matchup_id") == col("performance.matchup_id")) &
        (col("roster._matchup_week") == col("performance._matchup_week"))
    )

    # aggregate the data at a roster level to get total amount of starter and roster points, to compute roster strength
    df_aggregated = df_joined_results.groupBy(
        "roster._league_id", "roster._matchup_week","roster.matchup_id", "roster.roster_id", "roster.owner_id", "roster.owner_name",
        "roster.is_commissioner", "roster.team_name", "roster.matchup_points", "roster.team_streak",
        "roster.team_record", "roster.team_total_wins", "roster.team_total_losses", "roster.team_total_ties",
        "roster.team_total_fpts", "roster.team_total_fpts_against", "roster.team_waiver_budget_used",
        "roster.team_waiver_position", "roster._year"
    ).agg(
        F.sum(F.when(col("performance.is_starter"), col("performance.player_points")).otherwise(0)).alias("starter_points"),
        F.sum(F.when(~col("performance.is_starter"), col("performance.player_points")).otherwise(0)).alias("bench_points")
    ).withColumn(
        "roster_strength", col("starter_points") + (0.2 * col("bench_points"))
    )

    # rank the starter points against other teams in a given matchup week
    window_spec = Window.partitionBy("roster._league_id","roster._matchup_week",).orderBy(F.asc("matchup_points"))
    df_aggregated = df_aggregated.withColumn("starter_points_percentile", F.percent_rank().over(window_spec))

    # rank all the players for a given position on a roster to understand strong players at each position
    window_spec = Window.partitionBy("performance._league_id","performance._matchup_week", "performance.roster_id", "performance.player_position").orderBy(F.desc("performance.player_points"))
    df_ranked = df_joined_results.withColumn("rank", F.rank().over(window_spec))

    # get list of bench players
    df_bench_better_than_starters = df_ranked.filter(
        ~col("performance.is_starter")
    ).select(
        "performance._league_id", "performance._matchup_week", "performance.roster_id", "performance.player_id",
        "performance.player_name", "performance.player_position", "performance.player_points"
    ).alias("bench")

    # get list of starter players
    df_starters = df_joined_results.filter(col("performance.is_starter")).select(
        "performance._league_id", "performance._matchup_week", "performance.roster_id", "performance.player_position", "performance.player_id",
        col("performance.player_name").alias("starter_player_name"),
        col("performance.player_points").alias("starter_player_points")
    ).alias('starters')

    # join based on the matchup roster and player position to make suggestions about bench players that could have been starters
    df_bench_better_than_starters = df_bench_better_than_starters.alias("bench").join(
        df_starters.alias("starters"),
        (col("bench._league_id") == col("starters._league_id")) &
        (col("bench._matchup_week") == col("starters._matchup_week")) &
        (col("bench.roster_id") == col("starters.roster_id")) &
        (col("bench.player_position") == col("starters.player_position"))
    ).filter(
        (col("bench.player_points") > col("starters.starter_player_points"))
    ).withColumn(
        "point_oppertunity_cost",
        (col("bench.player_points") - col("starters.starter_player_points")).alias("point_opportunity_cost")
    )

    # dedup bench players who get recommended twice (take the highest point potential)
    window_spec = Window.partitionBy("bench._league_id","bench._matchup_week", "bench.roster_id", "bench.player_id").orderBy(F.desc("point_oppertunity_cost"))
    df_bench_better_than_starters = df_bench_better_than_starters.withColumn("bench_player_dedup", F.rank().over(window_spec)).filter("bench_player_dedup == 1")

    # dedup starter players who get recommended against twice (take the highest point potential)
    window_spec = Window.partitionBy("bench._league_id","bench._matchup_week", "bench.roster_id", "starters.player_id").orderBy(F.desc("point_oppertunity_cost"))
    df_bench_better_than_starters = df_bench_better_than_starters.withColumn("starter_player_dedup", F.rank().over(window_spec)).filter("starter_player_dedup == 1")

    # create array of struct to list each suggestion
    df_bench_better_than_starters = df_bench_better_than_starters.select(
        col("bench._league_id"), col("bench._matchup_week"), col("bench.roster_id"),
        F.struct(
            col("bench.player_name").alias("benched_player_name"),
            col("bench.player_points").alias("benched_player_points"),
            col("starters.starter_player_name"), col("starters.starter_player_points"),
            (col("bench.player_points") - col("starters.starter_player_points")).alias("point_opportunity_cost")
        ).alias("bench_better_than_starter")
    )

    # collect list of bench better than starters and sum of their points (max points)
    df_bench_better_than_starters_agg = df_bench_better_than_starters.groupBy(
        "_league_id", "_matchup_week", "roster_id"
    ).agg(
        F.collect_list("bench_better_than_starter").alias("bench_better_than_starters"),
        F.sum("bench_better_than_starter.point_opportunity_cost").alias("missed_starter_points")
    )

    # join back to aggregated roster data
    df_aggregated = df_aggregated.alias("aggregated").join(
        df_bench_better_than_starters_agg.alias("bench_agg"),
        ["_league_id", "_matchup_week", "roster_id"], "left"
    )

    # get top 3 highest performing starters
    window_spec_highest_scoring = Window.partitionBy("roster._league_id", "roster._matchup_week", "roster.roster_id").orderBy(F.desc("performance.player_points"))
    df_highest_scoring = df_joined_results.filter(col("performance.is_starter")).withColumn("rank", F.row_number().over(window_spec_highest_scoring)).filter(col("rank") <= 3)

    # create array of structs for top 3 highest scoring players
    df_highest_scoring_agg = df_highest_scoring.groupBy("roster._league_id", "roster._matchup_week","roster.roster_id").agg(
        F.collect_list(
            F.struct(
                col("performance.player_name").alias("highest_scoring_player_name"),
                col("performance.player_points").alias("highest_scoring_player_points")
            )
        ).alias("highest_scoring_players")
    )

    # join back to roster data
    df_aggregated = df_aggregated.join(
        df_highest_scoring_agg.alias("high_score_agg"),
        ["_league_id", "_matchup_week", "roster_id"], "left"
    )

    # get opponent points
    df_opponent_points = df_aggregated.select(
        col("_league_id"), 
        col("_matchup_week"),
        col("matchup_id"), 
        col("roster_id").alias("opponent_roster_id"), 
        col("matchup_points").alias("opponent_starter_points"),
        col("team_name").alias("opponent_team_name")
    )

    # self join to get opponent points and determine if missed starter points is enough to make a difference in the matchup. Determine managers efficiency based on ratio of starter points / max points
    df_aggregated = df_aggregated.alias("aggregated").join(
        df_opponent_points.alias("opponent"),
        (col("aggregated._league_id") == col("opponent._league_id")) &
        (col("aggregated._matchup_week") == col("opponent._matchup_week")) &
        (col("aggregated.matchup_id") == col("opponent.matchup_id")) &
        (col("aggregated.roster_id") != col("opponent.opponent_roster_id")),
        "left"
    ).withColumn(
        "couldve_won_with_missed_bench_points",
        F.when(col("matchup_points") > col("opponent_starter_points"), None)
        .when((col("matchup_points") + col("missed_starter_points")) > col("opponent_starter_points"), True)
        .otherwise(False)
    ).withColumn(
        "manager_efficiency",
        col("starter_points") / (col("starter_points") + F.coalesce(col("missed_starter_points"), F.lit(0)))
    ).withColumn(
        "matchup_outcome",
        F.when(col("matchup_points") > col("opponent_starter_points"), "Win")
        .when(col("matchup_points") == col("opponent_starter_points"), "Draw")
        .otherwise("Loss")
    ).withColumn(
        "matchup_outcome_type",
        F.when((col("matchup_outcome") == "Win") & (col("starter_points_percentile") < 0.5), "Lucky Win")
        .when((col("matchup_outcome") == "Win") & (col("starter_points_percentile") >= 0.5), "Deserving Win")
        .when((col("matchup_outcome") == "Loss") & (col("starter_points_percentile") < 0.5), "Deserving Loss")
        .when((col("matchup_outcome") == "Loss") & (col("starter_points_percentile") >= 0.5), "Unlucky Loss")
        .when((col("matchup_outcome") == "Draw") & (col("starter_points_percentile") < 0.5), "Low Scoring Draw")
        .when((col("matchup_outcome") == "Draw") & (col("starter_points_percentile") >= 0.5), "High Scoring Draw")
    )

    # generate percentile rank of the roster strength points to normalize the power points
    window_spec_percentile = Window.partitionBy("aggregated._league_id", "aggregated._matchup_week").orderBy("roster_strength")

    # compute power_points for the week based on roster strength percentile and manager efficiency
    df_aggregated = df_aggregated.withColumn("roster_strength_percentile", F.percent_rank().over(window_spec_percentile)).withColumn(
        "week_power_points",
        (col("roster_strength_percentile") * 0.85) + (col("manager_efficiency") * 0.15)
    )

    df_to_power_rank =  df_aggregated.select(
        "aggregated._league_id", "aggregated.matchup_id", "aggregated.roster_id", "aggregated.owner_id",
        "aggregated.owner_name", "aggregated.is_commissioner", "aggregated.team_name", "aggregated.team_streak",
        "aggregated.team_record", "aggregated.team_total_wins", "aggregated.team_total_losses", "aggregated.team_total_ties",
        "aggregated.team_total_fpts", "aggregated.team_total_fpts_against", "aggregated.team_waiver_budget_used",
        "aggregated.matchup_points", "starter_points", "starter_points_percentile", "bench_points", "bench_better_than_starters", "matchup_outcome", "matchup_outcome_type",
        "missed_starter_points", "couldve_won_with_missed_bench_points", "opponent_starter_points", "opponent_team_name", "highest_scoring_players",
        "roster_strength", "roster_strength_percentile", "manager_efficiency", "week_power_points",
        "aggregated._matchup_week", "aggregated._year"
    )

    schema = StructType([
        StructField("_league_id", StringType(), True),
        StructField("roster_id", IntegerType(), True),
        StructField("_matchup_week", IntegerType(), True),
        StructField("week_power_points", DoubleType(), True),
        StructField("previous_power_rank_score", DoubleType(), True),
        StructField("power_rank_score", DoubleType(), True)
    ])

    pandas_udf(schema, functionType=PandasUDFType.GROUPED_MAP)

    def power_rankings(pdf):
        pdf = pdf.sort_values("_matchup_week").reset_index(drop=True)

        prscore_list = []
        prev_prscore_list = []

        for i, row in pdf.iterrows():
            if i == 0:
                prscore = row["week_power_points"]
                prev_prscore = None
            else:
                prscore = prev_prscore * 0.7 + row["week_power_points"] * 0.3
            
            prscore_list.append(prscore)
            prev_prscore_list.append(prev_prscore)
            prev_prscore = prscore

        pdf["power_rank_score"] = prscore_list
        pdf["previous_power_rank_score"] = prev_prscore_list

        return pdf[["_league_id", "roster_id", "_matchup_week", "week_power_points", "power_rank_score", "previous_power_rank_score"]]

    # df = dlt.read('gold_weekly_performance_ranks')

    # Apply the function
    power_rank_df = df_to_power_rank.groupby("_league_id", "roster_id").applyInPandas(power_rankings, schema)

    joined_df = df_to_power_rank.alias('src').join(
        power_rank_df.alias('powerrank_df'), 
        on=["_league_id", "roster_id", "_matchup_week", "week_power_points"]
    ).select(
        "src.*", 
        "powerrank_df.power_rank_score", 
        "powerrank_df.previous_power_rank_score"
    )

    # Define a window over each league and week
    rank_window = Window.partitionBy("_league_id", "_matchup_week").orderBy(F.desc("power_rank_score"))

    # Add the power rank (1 = best score)
    ranked_df = joined_df.withColumn("power_rank", F.dense_rank().over(rank_window))

    prev_week_window = Window.partitionBy("_league_id", "roster_id").orderBy("_matchup_week")
    final_df = ranked_df.withColumn(
        "previous_power_rank",
        F.lag("power_rank").over(prev_week_window)
    )

    final_df = final_df.withColumn('power_rank_change', col('previous_power_rank') - col('power_rank'))

    return final_df

# COMMAND ----------

# from pyspark.sql.functions import pandas_udf, PandasUDFType
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
# from pyspark.sql.window import Window
# from pyspark.sql import functions as F

# @dlt.table(
#   name="gold_power_rankings",
#   comment="Stateful exponential‐decay of week_power_points → power_rank_points, safe for full‐refresh",
#   partition_cols=["_league_id", "_matchup_week"]
# )

# def gold_power_rankings():
#     schema = StructType([
#         StructField("_league_id", StringType(), True),
#         StructField("roster_id", IntegerType(), True),
#         StructField("_matchup_week", IntegerType(), True),
#         StructField("week_power_points", DoubleType(), True),
#         StructField("previous_power_rank_score", DoubleType(), True),
#         StructField("power_rank_score", DoubleType(), True)
#     ])

#     pandas_udf(schema, functionType=PandasUDFType.GROUPED_MAP)

#     def power_rankings(pdf):
#         pdf = pdf.sort_values("_matchup_week").reset_index(drop=True)

#         prscore_list = []
#         prev_prscore_list = []

#         for i, row in pdf.iterrows():
#             if i == 0:
#                 prscore = row["week_power_points"]
#                 prev_prscore = None
#             else:
#                 prscore = prev_prscore * 0.7 + row["week_power_points"] * 0.3
            
#             prscore_list.append(prscore)
#             prev_prscore_list.append(prev_prscore)
#             prev_prscore = prscore

#         pdf["power_rank_score"] = prscore_list
#         pdf["previous_power_rank_score"] = prev_prscore_list

#         return pdf[["_league_id", "roster_id", "_matchup_week", "week_power_points", "power_rank_score", "previous_power_rank_score"]]

#     df = dlt.read('gold_weekly_performance_ranks')

#     # Apply the function
#     power_rank_df = df.groupby("_league_id", "roster_id").applyInPandas(power_rankings, schema)

#     joined_df = df.alias('src').join(
#         power_rank_df.alias('powerrank_df'), 
#         on=["_league_id", "roster_id", "_matchup_week", "week_power_points"]
#     ).select(
#         "src.*", 
#         "powerrank_df.power_rank_score", 
#         "powerrank_df.previous_power_rank_score"
#     )

#     # Define a window over each league and week
#     rank_window = Window.partitionBy("_league_id", "_matchup_week").orderBy(F.desc("power_rank_score"))

#     # Add the power rank (1 = best score)
#     ranked_df = joined_df.withColumn("power_rank", F.dense_rank().over(rank_window))

#     prev_week_window = Window.partitionBy("_league_id", "roster_id").orderBy("_matchup_week")
#     final_df = ranked_df.withColumn(
#         "previous_power_rank",
#         F.lag("power_rank").over(prev_week_window)
#     )

#     final_df = final_df.withColumn('power_rank_change', col('previous_power_rank') - col('power_rank'))

#     return final_df
