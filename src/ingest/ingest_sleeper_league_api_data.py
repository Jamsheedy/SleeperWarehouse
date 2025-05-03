# Databricks notebook source
# MAGIC %md
# MAGIC # Sleaper API Relative Endpoints

# COMMAND ----------

# fetch parameters
par_league_id = dbutils.widgets.get("LEAGUE_ID")
par_matchup_week = dbutils.widgets.get("MATCHUP_WEEK")
par_year = dbutils.widgets.get("YEAR")

# COMMAND ----------

import requests
import json
from datetime import datetime
tgt_dir = '/mnt/databricks/sleeper/stg'

# COMMAND ----------

# MAGIC %md
# MAGIC ## /v1/league

# COMMAND ----------



def add_metadata(array_data):
    for row in array_data:
        row['_year'] = int(par_year)
        row['_league_id'] = par_league_id
        row['_matchup_week'] = int(par_matchup_week)
        row['_ingested_ts'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    return array_data


# COMMAND ----------

timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

# COMMAND ----------

endpoints = ['rosters', f'matchups/{par_matchup_week}', 'users']

for endpoint in endpoints:

    # Fetch data from the API
    response = requests.get(f"https://api.sleeper.app/v1/league/{par_league_id}/{endpoint}")
    data = response.json()

    # add metadata
    data = add_metadata(data)
    # Write JSON data to a volume with a unique timestamp
    endpoint_dir = endpoint.split('/')[0]

    dbutils.fs.put(f"{tgt_dir}/{endpoint_dir}/{par_year}/{endpoint_dir}_{par_league_id}_{par_matchup_week}_{timestamp}.json", json.dumps(data), overwrite=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## v1/drafts

# COMMAND ----------

league_endpoints = ['drafts']

timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

for endpoint in league_endpoints:

    # Fetch data from the API
    response = requests.get(f"https://api.sleeper.app/v1/league/{par_league_id}/{endpoint}")
    data = response.json()

    # add metadata
    data = add_metadata(data)
    # Write JSON data to a volume with a unique timestamp
    endpoint_dir = endpoint.split('/')[0]

    #dbutils.fs.put(f"{tgt_dir}/{endpoint_dir}/{par_year}/{endpoint_dir}_{par_league_id}_{par_matchup_week}_{timestamp}.json", json.dumps(data), overwrite=True)

    latest_draft_id = data[0]['draft_id']

    response = requests.get(f"https://api.sleeper.app/v1/draft/{latest_draft_id}/picks")
    data = response.json()

    data = add_metadata([data])

    #dbutils.fs.put(f"{tgt_dir}/{endpoint_dir + '_picks'}/{par_year}/{endpoint_dir + '_picks'}_{par_league_id}_{par_matchup_week}_{timestamp}.json", json.dumps(data), overwrite=True)


# COMMAND ----------

# MAGIC %md
# MAGIC ## v1/players

# COMMAND ----------

player_api_response = requests.get(f'https://api.sleeper.app/v1/players/nfl')
player_api_raw_json = json.loads(player_api_response.text)

# COMMAND ----------

player_api_json = [v for k, v in player_api_raw_json.items() if isinstance(v, dict) and 'player_id' in v.keys()]

# COMMAND ----------

player_api_json = add_metadata(player_api_json)
dbutils.fs.put(f"{tgt_dir}/players/{par_year}/players_{par_league_id}_{par_matchup_week}_{timestamp}.json", json.dumps(player_api_json), overwrite=True)
