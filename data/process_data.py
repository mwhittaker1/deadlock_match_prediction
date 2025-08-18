import requests
import json
import os
import sys
import pandas as pd
import logging
from urllib.parse import urlencode
import time
from datetime import timedelta, datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

"""Contains functions for processing match data for different workflows"""

def get_unique_players(df):
    """Extract unique players from the match data DataFrame."""
    if df is None or df.empty:
        logger.warning("Empty DataFrame provided to get_unique_players.")
        return pd.Series(dtype=str)

    unique_players = pd.Series(df['player_id'].unique(), dtype=str)
    logger.info(f"Extracted {len(unique_players)} unique players from {len(df)} players.")

    return unique_players

def compare_player_dataframes(df1, df2) -> tuple[int, set]:
    """finds missing unique players between datasets"""
    ids1 = set(df1['account_id'].unique())
    ids2 = set(df2['account_id'].unique())
    mismatched_ids = ids1.symmetric_difference(ids2)
    return len(mismatched_ids), mismatched_ids

def separate_match_players(
        matches_grouped_by_day: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Normalizes bulk match data into two dataframes: matches and players."""

    logging.info("Normalizing bulk match data")
    matches = []
    players = []
    if not matches_grouped_by_day:
        logging.warning("No match data found — matches_grouped_by_day is empty.")
        return pd.DataFrame(), pd.DataFrame()
    
    for day_idx, day_matches in enumerate(matches_grouped_by_day): #day = key, match = value
        logging.info(f"Processing day #{day_idx} with {len(day_matches)} matches")
        
        for match in day_matches: # match: day = key: value | match_id: 7432551
            try:
                match_id = match["match_id"]
                start_time = match["start_time"]
                game_mode = match["game_mode"]
                match_mode = match["match_mode"]
                duration_s = match["duration_s"]
                winning_team = match["winning_team"]
           
            except KeyError as e:
                logging.error(f"Match missing key {e}: {match.get('match_id', 'unknown')}", exc_info=True)
                continue

            # Append to matches list
            matches.append({
                "match_id": match_id, # PK
                "start_time": start_time,
                "game_mode": game_mode,
                "match_mode": match_mode,
                "duration_s": duration_s,
                "winning_team": winning_team
            })
            
            # Append each player to players list
            if "players" not in match or len(match["players"]) != 12:
                logging.error(f"Match {match.get('match_id', 'unknown')} has invalid player count: {len(match.get('players', []))}")
                continue
            for player in match["players"]: # player: match["players"] = key: value | player_id: 1234567
                try:
                    players.append({
                        "account_id": player["account_id"],
                        "match_id": match_id,
                        "team": player["team"],
                        "hero_id": player["hero_id"],
                        "kills": player["kills"],
                        "deaths": player["deaths"],
                        "assists": player["assists"],
                        "denies": player["denies"],
                        "net_worth": player["net_worth"],
                    })
                except KeyError as e:
                    logging.error(f"Player missing key {e}: {player.get('account_id', 'unknown')}", exc_info=True)
                    continue

    # Convert lists to DataFrames
    df_matches = pd.DataFrame(matches)
    df_players = pd.DataFrame(players)
    if not matches:
        logging.warning("No matches appended — matches list is empty.")
    if not players:
        logging.warning("No players appended — players list is empty.")

    return df_matches, df_players

def fetch_player_hero_stats(account_id) -> dict:
    """Fetches hero stats for a specific player from the Deadlock API.
    Generally used in conjunction with process_player_stats_parallel

    - account_id: Player's account ID to fetch stats for (can be string or numeric)
    
    Returns:
    - JSON response containing player's hero stats or error dict
    """
    
    base = "https://api.deadlock-api.com/v1/players"
    
    # Convert to string if it's not already
    account_id_str = str(account_id)
    
    path = f"{base}/{account_id_str}/hero-stats"
    
    try:
        print(f"Fetching stats for player {account_id_str}")
        response = requests.get(path)
        if response.status_code != 200:
            print(f"Error: API request failed for player {account_id_str} with status code {response.status_code}")
            print(f"Response: {response.text}")
            return {"error": f"API request failed with status code {response.status_code}"}
        return response.json()
    except Exception as e:
        print(f"Exception fetching hero stats for player {account_id_str}: {e}")
        return {"error": str(e)}