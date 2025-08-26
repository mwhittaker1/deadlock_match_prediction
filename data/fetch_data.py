
# imports
from pyparsing import Dict
import requests
import json
import os
import sys
import pandas as pd
import logging
from urllib.parse import urlencode
import time
from datetime import timedelta, datetime, timezone
#from data.process_data import separate_match_players


""" About this script
This script fetches data from various sources and prepares it for analysis.
"""

# logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def unix_utc_start(date_str: str) -> int:
    # YYYY-MM-DD at 00:00:00 UTC
    dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    dt = dt.replace(hour=0, minute=0, second=0)
    return int(dt.timestamp())

def unix_utc_eod(date_str: str) -> int:
    # YYYY-MM-DD at 23:59:59 UTC (inclusive)
    dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    dt = dt.replace(hour=23, minute=59, second=59)
    return int(dt.timestamp())

# fetch active matches for prediction

# fetch specific set of matches for analysis, training supplement, or prediction of a specific period

def fetch_match_data(
    min_average_badge: int = 100,
    fetch_till_date: int | None = None,
    fetch_from_date: int | None = None,
    m_id: str | None = None,
    include_player_info: bool = True,
    limit: int = 1000
    ) -> json:

    """Fetches match data from the Deadlock API.

    Key Parameters:
    - min_average_badge: Minimum average rank to return matches.
    - fetch_till_date: Newest time to filter matches. i.e. matches before yesterday
    - fetch_from_date: Oldest time to filter matches. i.e. matches 3 months ago -> max time
    - m_id: Specific match ID to fetch metadata for.
    - include_player_info: Whether to include player information in the response. this is required for match_player data
    - limit: Maximum number of matches to return.
    Returns:
    - JSON response containing match metadata with 12 players per match.
    """

    logging.debug(f"Fetching match data..")
    base = "https://api.deadlock-api.com/v1/matches"

    # if a specific match ID is given, check player_data and hit that endpoint
    if m_id:
        path = f"{base}/{m_id}/metadata"
        params = {}
        if include_player_info:
            params["include_player_info"] = "true"

        query = urlencode(params)
        full_url = f"{path}?{query}" if query else path

        response = requests.get(full_url)

        if response.status_code != 200:
            print(f"Error: API request failed with status code {response.status_code}")
            print(f"URL: {full_url}")
            return {"error": f"API request failed with status code {response.status_code}"}
        return response.json()

    # Bulk-metadata endpoint
    path = f"{base}/metadata"
    params: dict[str, str] = {}

    if include_player_info:
        params["include_player_info"] = "true"
    
    print(f"Time range: {fetch_from_date} to {fetch_till_date}")

    fetch_from_date = unix_utc_start(fetch_from_date) if fetch_from_date else None
    fetch_till_date = unix_utc_eod(fetch_till_date) if fetch_till_date else None

    if fetch_from_date is not None:
        params["min_unix_timestamp"] = (fetch_from_date)
    if fetch_till_date is not None:
        params["max_unix_timestamp"] = (fetch_till_date)
    if min_average_badge is not None:
        params["min_average_badge"] = str(min_average_badge)
    if limit is not None:
        params["limit"] = str(limit)

    query = urlencode(params)
    full_url = f"{path}?{query}" if query else path

    response = requests.get(full_url)

    if response.status_code != 200:
        print(f"Error: API request failed with status code {response.status_code}")
        print(f"URL: {full_url}")
        return {"error": f"API request failed with status code {response.status_code}"}
    
    return response.json()

def bulk_fetch_matches(start_date, end_date, limit=1000)->list:
    """fetches a batch of matches, 1 day per pull, list of jsons, 1 element per batch.
    batch return is unnormalized, 'players' contains a df of each matches 'players'
    min_days = Oldest time barrier (more days ago)
    max_days = Newest time barrier (fewer days ago)
    """
    batch_matches = []
    
    # Calculate the starting day (defaults to today)
    current_start = datetime.strptime(start_date, "%Y-%m-%d")
    current_end = datetime.strptime(end_date, "%Y-%m-%d")

    total_batches = (current_end - current_start).days + 1
    batch_num = 1

    while current_start <= current_end:
        fetch_from = current_start.strftime("%Y-%m-%d")
        fetch_till = current_start.strftime("%Y-%m-%d")

        logging.debug(f"\nBatch {batch_num} of {total_batches}: fetching day from {fetch_from} to {fetch_till}")

        # Note: API expects min_unix_timestamp to be OLDER than max_unix_timestamp
        fetched_matches = fetch_match_data(
            fetch_till_date=fetch_from,  # Older timestamp (more days ago)
            fetch_from_date=fetch_till,  # Newer timestamp (fewer days ago)
            limit=limit
        )
        
        logging.info(f"fetch matches for day {fetch_from}. total matches found: {len(fetched_matches)}")

        # Check if there was an error in the API response
        if "error" in fetched_matches:
            print(f"Error encountered during batch {batch_num+1}. Skipping this batch.")
        else:
            batch_matches.append(fetched_matches)
            
        # Move backward in time by one day
        current_start += timedelta(days=1)
        batch_num += 1

    return batch_matches

    # For each unique player within raw_players, fetch player_hero and calculate player_stats

def fetch_player_hero_stats(account_ids: list[int], fetch_till_date, fetch_from_date=None) -> dict:
    """Fetches hero stats for a specific player from the Deadlock API.
    Generally used in conjunction with run_player_batches and 
    process_player_stats_parallel

    - account_ids: list of Player's account IDs to fetch stats for (can be string or numeric)
    
    Returns:
    - Dict response containing player's hero stats or error dict
    """
    
    base = "https://api.deadlock-api.com/v1/players/hero-stats"
    path = f"{base}"
    params: dict[str, str] = {}

    params["account_ids"] = ",".join(str(i) for i in account_ids)

    fetch_from_date = unix_utc_start(fetch_from_date) if fetch_from_date else None
    fetch_till_date = unix_utc_eod(fetch_till_date) if fetch_till_date else None

    if fetch_from_date is not None:
        params["min_unix_timestamp"] = (fetch_from_date)
    if fetch_till_date is not None:
        params["max_unix_timestamp"] = (fetch_till_date)

    query = urlencode(params)
    full_url = f"{path}?{query}" if query else path

    print(f"**DEBUG** params = {params},full_url = \n\n{full_url}")

    try:
        response = requests.get(full_url)
        if response.status_code != 200:
            logging.error(f"API request failed for list of players with status code {response.status_code}")
            logging.error(f"Response: {response.text}")
            return {"error": f"API request failed with status code {response.status_code}"}
        
        
        return response.json()
    except Exception as e:
        logging.error(f"Exception fetching hero stats for players list: {e}")
        return {"error": str(e)}

# send players in batches of 1,000
def fetch_player_hero_stats_batch(batch_size, account_ids: list[int], fetch_till_date, fetch_from_date=None) -> pd.DataFrame:
    """Fetches hero stats for a batch of players from the Deadlock API.
    Generally used in conjunction with run_player_batches and
    process_player_stats_parallel

    - account_ids: list of Player's account IDs to fetch stats for (can be string or numeric)

    Returns:
    - Dict response containing player's hero stats or error dict
    """

    results = []
    for i in range(0, len(account_ids), batch_size):
        print(len(account_ids))
        batch = account_ids[i:i + batch_size]
        response = fetch_player_hero_stats(batch, fetch_till_date=fetch_till_date, fetch_from_date=fetch_from_date)
        results.extend(format_player_hero_response(response))
    return pd.DataFrame(results)

def format_player_hero_response(players_hero_data: list[Dict]):
    """removes matches nested list, normalizes to player<>hero stat row"""
    ph_stats = []

    for entry in players_hero_data:
        entry = {k: v for k, v in entry.items() if k != "matches"}
        ph_stats.append(entry)
    return ph_stats

def process_player_stats(player_hero_stats:pd.DataFrame)->pd.DataFrame:
    """Creates aggreagate player stats from player_hero stats"""

    columns = ["matches_played", "kills", "deaths", "wins", "assists", "time_played"]
    
    for c in columns:
        if c in player_hero_stats.columns:
            player_hero_stats[c] = pd.to_numeric(player_hero_stats[c], errors='coerce')

    p_stats = player_hero_stats.groupby('account_id', as_index=False).agg(
        p_total_matches_played=('matches_played', 'sum'),
        p_total_kills=('kills', 'sum'),
        p_total_deaths=('deaths', 'sum'),
        p_total_wins=('wins', 'sum'),
        p_total_assists=('assists', 'sum'),
        p_total_time_played=('time_played', 'sum')
    )

    p_stats['p_avg_kills'] = (p_stats['p_total_kills'] / p_stats['p_total_matches_played'].replace(0, pd.NA)).fillna(0)
    p_stats['p_win_rate'] = (p_stats['p_total_wins'] / p_stats['p_total_matches_played'].replace(0, pd.NA)).fillna(0)

    return p_stats

def fetch_hero_stats(
    min_average_badge: int = 100,
    fetch_till_date: int = None,
    fetch_from_date: int = None,
    ) -> json:
    """Fetches match data from the Deadlock API.
    
    Key Parameters:
    - min_average_badge: Minimum average rank to capture hero stats
    - fetch_till_date: Newest time to filter hero stats. i.e. matches before yesterday
    - fetch_from_date: Oldest time to filter hero stats. i.e. matches 3 months ago -> max time

    Returns:
    - JSON response, list of dicts
    """

    logging.debug(f"Fetching match data..")
    base = 'https://api.deadlock-api.com/v1/analytics'

    path = f"{base}/hero-stats"
    params: dict[str, str] = {}

    
    print(f"Time range: {fetch_from_date} to {fetch_till_date}")

    fetch_from_date = unix_utc_start(fetch_from_date) if fetch_from_date else None
    fetch_till_date = unix_utc_eod(fetch_till_date) if fetch_till_date else None

    if fetch_from_date is not None:
        params["min_unix_timestamp"] = (fetch_from_date)
    if fetch_till_date is not None:
        params["max_unix_timestamp"] = (fetch_till_date)
    if min_average_badge is not None:
        params["min_average_badge"] = str(min_average_badge)

    query = urlencode(params)
    full_url = f"{path}?{query}" if query else path
    
    response = requests.get(full_url)
    if response.status_code != 200:
        print(f"Error: API request failed with status code {response.status_code}")
        print(f"URL: {full_url}")
        return {"error": f"API request failed with status code {response.status_code}"}
    return response.json()

if __name__ == "__main__":
    # Example usage
    start_date = "2025-08-19"
    end_date = "2025-08-21"
    folder_name = f"v2_data//pred_data//test_pred_v2_{start_date}_{end_date}"

    batch_matches = bulk_fetch_matches(
        start_date=start_date,
        end_date=end_date, 
        limit=1000
    )

    raw_matches, raw_players = separate_match_players(batch_matches)
    player_matches = raw_players[['account_id', 'match_id']]

    bulk_player_hero_stats= fetch_player_hero_stats_batch(
    account_ids=raw_players["account_id"].unique().tolist(),
    fetch_till_date=start_date,
    fetch_from_date=None,
    batch_size=700
    )

    hero_stats = pd.json_normalize(fetch_hero_stats(fetch_from_date='2020-01-01', fetch_till_date=end_date))
    hero_stats = hero_stats.add_prefix('h_')
    hero_stats = hero_stats.rename(columns={'h_hero_id': 'hero_id'})

    p_stats = process_player_stats(bulk_player_hero_stats)

    bulk_player_hero_stats.to_csv(f"{folder_name}/player_hero_stats.csv", index=False)
    raw_players.to_csv(f"{folder_name}/raw_players.csv", index=False)
    raw_matches.to_csv(f"{folder_name}/raw_matches.csv", index=False)
    hero_stats.to_csv(f"{folder_name}/h_stats.csv", index=False)
    player_matches.to_csv(f"{folder_name}/player_matches.csv", index=False)
