import requests
import json
import os
import sys
import pandas as pd
import numpy as np
import logging
from urllib.parse import urlencode
import time
from datetime import timedelta, datetime
#import fetch_data as fd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

"""Contains functions for processing data and creating statistics"""

def prepare_match_stats(raw_matches: pd.DataFrame, raw_players: pd.DataFrame) -> pd.DataFrame:
    """Prepares match for future merging by creating win column, and adjusting columns"""
    players = raw_players.merge(
        raw_matches[['match_id', 'winning_team']],
        on='match_id',
        how='left'
    )

    players['win'] = players.apply(
        lambda row: 'Y' if row['team'] == row['winning_team'] else 'N',
        axis=1
    )

    return players

def check_unique_naming(p_h_stats=None,
                        p_stats=None, 
                        h_stats=None
                        ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    static_names = ['account_id', 'hero_id', 'match_id']
    if p_h_stats is not None:
        p_h_stats = p_h_stats.rename(columns={col: f"ph_{col}" for col in p_h_stats.columns 
            if col not in static_names and not col.startswith('ph_')})
    if p_stats is not None:
        p_stats = p_stats.rename(columns={col: f"p_{col}" for col in p_stats.columns
            if col not in static_names and not col.startswith('p_')})
    if h_stats is not None:
        h_stats = h_stats.rename(columns={col: f"h_{col}" for col in h_stats.columns
            if col not in static_names and not col.startswith('h_')})

    return p_h_stats, p_stats, h_stats

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

def merge_stats(player_stats, player_hero_stats, hero_stats)->pd.DataFrame:
    p_ph_stats = player_stats.merge(player_hero_stats, on='account_id')
    p_ph_h_stats = p_ph_stats.merge(hero_stats, on='hero_id')

    move_col = 'hero_id'
    pos = 1

    col = p_ph_h_stats.pop(move_col)
    p_ph_h_stats.insert(pos, move_col, col)

    return p_ph_h_stats

def calculate_ph_stats(p_ph_h_stats: pd.DataFrame) -> pd.DataFrame:
    """
    Create player hero stats by aggregating the player_hero_stats DataFrame.
    Creates aggregate function across all hero_stats for players in df.
    Checks for potential divide by zero errors and sets result to 0 if denominator is zero.
    """
    import numpy as np
    
    all_stats = p_ph_h_stats.copy()

    # player_hero kd for the player<>hero combo
    all_stats['ph_total_kd'] = np.where(all_stats['ph_deaths'] == 0, 0, 
                                        all_stats['ph_kills'] / all_stats['ph_deaths'])

    # all 100 badge hero stats, total kd- used to compare player performance with hero
    all_stats['h_total_kd'] = np.where(all_stats['h_total_deaths'] == 0, 0, 
                                        all_stats['h_total_kills'] / all_stats['h_total_deaths'])

    # compares player_hero kd to average hero_kd - player skill with hero.
    all_stats['ph_kd_ratio'] = np.where(all_stats['ph_total_kd'] == 0, 0, 
                                        all_stats['ph_total_kd']/ all_stats['h_total_kd'])

    # What % of matches the player played with hero, compared to all matches played with hero.
    all_stats['ph_hero_xp_ratio'] = np.where(all_stats['ph_matches_played'] == 0, 0, 
                                        all_stats['ph_matches_played'] / all_stats['h_matches'])
    
    # Create damage per match ratio, player_hero to hero
    all_stats['ph_avg_match_length'] = np.where(all_stats['ph_time_played'] == 0, 0,
                                        all_stats['ph_time_played'] / all_stats['ph_matches_played']/60)
    all_stats['ph_avg_damage_per_match'] = np.where(all_stats['ph_avg_match_length'] == 0, 0,
                                        all_stats['ph_damage_per_min'] * all_stats['ph_avg_match_length'])
    all_stats['h_damage_per_match'] = np.where(all_stats['h_matches'] == 0, 0,
                                        all_stats['h_total_player_damage'] / all_stats['h_matches'])
    all_stats['ph_damage_ratio'] = np.where(all_stats['ph_avg_damage_per_match'] == 0, 0,
                                        all_stats['ph_avg_damage_per_match'] / all_stats['h_damage_per_match'])

    # player_hero ratios
    all_stats['ph_assists_ratio'] = np.where(all_stats['h_total_assists'] == 0, 0, 
                                            all_stats['ph_assists']/ all_stats['h_total_assists'])
    all_stats['ph_win_rate'] = np.where(all_stats['ph_matches_played'] == 0, 0, 
                                        all_stats['ph_wins'] / all_stats['ph_matches_played'])
    all_stats['h_total_win_rate'] = np.where(all_stats['h_matches'] == 0, 0,
                                        all_stats['h_wins'] / all_stats['h_matches'])
    all_stats['ph_win_rate_ratio'] = np.where(all_stats['ph_win_rate'] == 0, 0, 
                                            all_stats['ph_win_rate'] / all_stats['h_total_win_rate'])
    
    return all_stats

if __name__ == "__main__":
    start_date = "2025-08-19"
    end_date = "2025-08-21"
    folder_name = f"v2_data//pred_data//test_pred_v2_{start_date}_{end_date}"

    # Player <> Match Linking table
    player_matches = pd.read_csv(f"{folder_name}/player_matches.csv")

    # player stats across lifetime of player
    player_stats = pd.read_csv(f"{folder_name}/p_stats.csv")

    # player hero stats across liftetime of games played
    player_hero_stats = pd.read_csv(f"{folder_name}/player_hero_stats.csv")

    # raw hero stats for badge 100+ based on end_date
    hero_stats = pd.read_csv(f"{folder_name}/hero_stats.csv")

    player_hero_stats, player_stats, hero_stats = check_unique_naming(
                player_hero_stats, player_stats, hero_stats
                )

    p_ph_h_stats = merge_stats(player_stats, player_hero_stats, hero_stats)

    # save merged stats to csv
    p_ph_h_stats.to_csv(f"{folder_name}/p_ph_h_stats.csv", index=False)

    all_stats = calculate_ph_stats(p_ph_h_stats)

    all_stats.to_csv(f"{folder_name}/all_stats.csv", index=False)
