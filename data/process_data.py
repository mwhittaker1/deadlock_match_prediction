import requests
import json
import os
import sys
import pandas as pd
import logging
from urllib.parse import urlencode
import time
from datetime import timedelta, datetime
#import fetch_data as fd

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

def fetch_player_hero_stats(account_id, start_date=None, end_date=None) -> dict:
    """Fetches hero stats for a specific player from the Deadlock API.
    Generally used in conjunction with run_player_batches and 
    process_player_stats_parallel

    - account_id: Player's account ID to fetch stats for (can be string or numeric)
    
    Returns:
    - JSON response containing player's hero stats or error dict
    """
    
    base = "https://api.deadlock-api.com/v1/players"
    
    if start_date is not None:
        older_time = str(f"?min_unix_timestamp={fd.get_unix_time(start_date)}")

    if end_date is not None:
        newer_time = str(f"&max_unix_timestamp={fd.get_unix_time(end_date)}")

    # Force acocunt_id to str
    account_id_str = str(account_id)

    path = f"{base}/{account_id_str}/hero-stats{older_time}{newer_time}"
    
    try:
        logging.info(f"Fetching stats for player {account_id_str}")
        response = requests.get(path)
        if response.status_code != 200:
            logging.error(f"API request failed for player {account_id_str} with status code {response.status_code}")
            logging.error(f"Response: {response.text}")
            return {"error": f"API request failed with status code {response.status_code}"}
        return response.json()
    except Exception as e:
        logging.error(f"Exception fetching hero stats for player {account_id_str}: {e}")
        return {"error": str(e)}

def process_player_stats_parallel(player_ids,start_date, end_date, max_workers=100, timeout=30):
    """
    Fetches and processes hero stats for multiple players in parallel
    
    Parameters:
    - player_ids: List of player account IDs to fetch stats for
    - max_workers: Maximum number of concurrent requests
    - timeout: Timeout in seconds for each request
    
    Returns:
    - Tuple of (player_stats_df, player_hero_stats_df)
    """

    import concurrent.futures
    import time
    from tqdm.notebook import tqdm

    player_stats = []
    player_hero_stats = []
    error_count = 0
    
    # Define stats we want to aggregate
    stats_columns = [
        'matches_played', 'wins', 'kills', 'deaths', 'assists',
        'damage_per_min', 'time_played'
    ]
    
    # Convert all player IDs to strings
    player_ids_str = [str(pid) for pid in player_ids]
    
    logging.info(f"Processing {len(player_ids_str)} players with {max_workers} workers")
    
    # Use ThreadPoolExecutor for parallel processing
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks and store futures
        future_to_id = {
            executor.submit(fetch_player_hero_stats, player_id, start_date, end_date): player_id
            for player_id in player_ids_str
        }
        
        # Process results as they complete
        for future in tqdm(concurrent.futures.as_completed(future_to_id), total=len(player_ids_str)):
            player_id = future_to_id[future]
            
            try:
                result = future.result(timeout=timeout)
                
                # Skip if error
                if "error" in result:
                    error_count += 1
                    continue
                
                # Skip if empty response
                if not result:
                    logging.warning(f"Empty result for player {player_id}")
                    continue
                
                # Calculate aggregate stats per player
                player_matches_played = sum(hero_stat.get('matches_played', 0) for hero_stat in result)
                if player_matches_played == 0:
                    # Skip players with no matches
                    logging.warning(f"Player {player_id} has no matches")
                    continue
                    
                total_kills = sum(hero_stat.get('kills', 0) for hero_stat in result)
                total_deaths = sum(hero_stat.get('deaths', 0) for hero_stat in result)
                total_wins = sum(hero_stat.get('wins', 0) for hero_stat in result)
                total_assists = sum(hero_stat.get('assists', 0) for hero_stat in result)
                total_time_played = sum(hero_stat.get('time_played', 0) for hero_stat in result)
                
                # Calculate averages
                avg_kd = total_kills / max(total_deaths, 1)  # Avoid division by zero
                win_rate = total_wins / player_matches_played if player_matches_played > 0 else 0
                
                # Add to player stats dataframe
                player_stats.append({
                    'account_id': player_id,
                    'matches_played': player_matches_played,
                    'total_kills': total_kills,
                    'total_deaths': total_deaths,
                    'total_assists': total_assists,
                    'avg_kd': avg_kd,
                    'win_rate': win_rate,
                    'total_time_played': total_time_played
                })
                
                # Add hero-specific stats to player_hero_stats dataframe
                for hero_stat in result:
                    hero_id = hero_stat.get('hero_id')
                    if hero_id is not None:
                        # Extract valid stats, handling potential missing keys
                        valid_stats = {
                            col: hero_stat.get(col, 0) for col in stats_columns
                        }
                        
                        # Add entry to hero stats
                        player_hero_stats.append({
                            'account_id': player_id,
                            'hero_id': hero_id,
                            **valid_stats
                        })
                
            except Exception as e:
                logging.error(f"Error processing player {player_id}: {e}")
                error_count += 1

    logging.info(f"Completed with {error_count} errors out of {len(player_ids_str)} players")

    # Convert to DataFrames
    df_player_stats = pd.DataFrame(player_stats) if player_stats else pd.DataFrame()
    df_player_hero_stats = pd.DataFrame(player_hero_stats) if player_hero_stats else pd.DataFrame()
    
    return df_player_stats, df_player_hero_stats

def run_player_batches(player_ids, start_date, end_date, batch_size=50, max_workers_per_batch=50, timeout=30):
    """
    Process all players in batches
    
    Parameters:
    - player_ids: List of player IDs to process
    - batch_size: Number of players to process in each batch
    - max_workers_per_batch: Number of concurrent workers per batch
    - timeout: Request timeout in seconds
    
    Returns:
    - Tuple of (player_stats_df, player_hero_stats_df)
    """

    num_batches = (len(player_ids) + batch_size - 1) // batch_size  # Ceiling division

    batched_player_stats = []
    batched_player_hero_stats = []
    successful_players = 0
    failed_players = 0

    logging.info(f"Processing {len(player_ids)} players in {num_batches} batches of {batch_size}")

    total_start_time = time.time()

    for batch_num in range(num_batches):
        start_idx = batch_num * batch_size
        end_idx = min(start_idx + batch_size, len(player_ids))
        batch_players = player_ids[start_idx:end_idx]

        logging.info(f"Processing batch {batch_num+1}/{num_batches} with {len(batch_players)} players")

        batch_start_time = time.time()
        
        # Process batch
        batch_player_stats, batch_player_hero_stats = process_player_stats_parallel(
            batch_players, 
            start_date,
            end_date,
            max_workers=max_workers_per_batch, 
            timeout=timeout
        )
        
        batch_end_time = time.time()
        batch_duration = batch_end_time - batch_start_time

        batch_successful = len(batch_player_stats) if not batch_player_stats.empty else 0
        batch_failed = len(batch_players) - batch_successful
        
        successful_players += batch_successful
        failed_players += batch_failed

        logging.info(f"Batch {batch_num+1} completed in {batch_duration:.2f} seconds")
        logging.info(f"  Players processed in batch: {batch_successful}/{len(batch_players)}")

        # Add results to overall lists
        if not batch_player_stats.empty:
            batched_player_stats.append(batch_player_stats)
            batched_player_hero_stats.append(batch_player_hero_stats)

            logging.info(f"  Players processed in batch: {len(batch_player_stats)}")
            logging.info(f"  Hero stats entries in batch: {len(batch_player_hero_stats)}")

    # Combine all batches
    if batched_player_stats:
        combined_player_stats = pd.concat(batched_player_stats, ignore_index=True)
        combined_player_hero_stats = pd.concat(batched_player_hero_stats, ignore_index=True)
        
        total_end_time = time.time()
        total_duration = total_end_time - total_start_time
        logging.info(f"Total processing completed in {total_duration:.2f} seconds")

        # Display summary results
        logging.info("\nPlayer Stats Summary:")
        logging.info(f"Total players processed: {len(combined_player_stats)}")
        logging.info(f"Total hero entries: {len(combined_player_hero_stats)}")
        logging.info(f"Average K:D ratio: {combined_player_stats['avg_kd'].mean():.2f}")
        logging.info(f"Average win rate: {combined_player_stats['win_rate'].mean():.2f}")

        # Save test data
        combined_player_stats.to_csv("v2_data/test_player_stats.csv", index=False)
        combined_player_hero_stats.to_csv("v2_data/test_player_hero_stats.csv", index=False)
        
        return combined_player_stats, combined_player_hero_stats
    
    else:
        logging.error("No player data was successfully processed")
    
    
def retry_failed_players(original_player_set = "v2_data/players.csv", completed_player_stats = "v2_data/player_stats.csv"):
    """
    Identifies player IDs that failed to process and retries them
    
    This function:
    1. Finds all player IDs in the original dataset that aren't in the processed results
    2. Retries processing these failed players in smaller batches
    3. Combines the retry results with the original results
    4. Saves the updated complete dataset
    """
    print("Starting retry process for failed player IDs...")
    

    try:
        # Load the original unique player IDs from the match data
        original_players_df = pd.read_csv(original_player_set)
        all_original_players = set(original_players_df['account_id'].astype(str).unique())
        print(f"Total unique players in original data: {len(all_original_players)}")
    
        # load the processed player stats
        processed_stats = pd.read_csv(completed_player_stats)
        processed_player_ids = set(processed_stats['account_id'].astype(str))
        print(f"Successfully processed players: {len(processed_player_ids)}")
        
        # Find failed players (in original set but not in processed set)
        failed_player_ids = list(all_original_players - processed_player_ids)
        print(f"Found {len(failed_player_ids)} failed player IDs to retry")
        
        if not failed_player_ids:
            print("No failed players to retry. All players were processed successfully.")
            return
            
        # Process the failed players with smaller batch size and more timeout
        print("\n--- Retrying failed players ---")
        retry_stats, retry_hero_stats = process_all_players(
            failed_player_ids,
            batch_size=50,            # Smaller batch size
            max_workers_per_batch=10,  # Fewer concurrent workers
            timeout=45                # Longer timeout
        )
        
        # Check if retry was successful
        if retry_stats.empty:
            print("Retry process didn't yield any successful results")
            return
            
        # Combine the retry results with the original results
        print("\nCombining retry results with original results...")
        
        # Load original hero stats
        processed_hero_stats = pd.read_csv("v2_data/player_hero_stats.csv")
        
        # Combine data
        combined_stats = pd.concat([processed_stats, retry_stats], ignore_index=True)
        combined_hero_stats = pd.concat([processed_hero_stats, retry_hero_stats], ignore_index=True)
        
        # Save the updated complete dataset
        combined_stats.to_csv("v2_data/player_stats_with_retries.csv", index=False)
        combined_hero_stats.to_csv("v2_data/player_hero_stats_with_retries.csv", index=False)
        
        # Calculate success rate
        total_success_count = len(combined_stats)
        success_rate = total_success_count / len(all_original_players) * 100
        
        print("\n--- Retry Summary ---")
        print(f"Original successful players: {len(processed_player_ids)}")
        print(f"Additional players from retry: {len(retry_stats)}")
        print(f"Total successful players: {total_success_count}/{len(all_original_players)} ({success_rate:.1f}%)")
        print(f"Total hero entries: {len(combined_hero_stats)}")
        print("Updated data saved to v2_data/player_stats_with_retries.csv and v2_data/player_hero_stats_with_retries.csv")
        
        return combined_stats, combined_hero_stats

    except FileNotFoundError:
        print("Error: Processed data files not found. Make sure the main processing completed and saved files.")
    except Exception as e:
        print(f"Error during retry process: {e}")

def merge_player_match_stats(players, player_stats):
    """Merge player match statistics with player information."""
    merged = players.merge(player_stats, on="account_id", how="left")
    return merged

def rename_match_stats(df):
    """rename columns to specific data origin"""
    df.rename(columns={
        "kills": "pm_kills",
        "deaths": "pm_deaths",
        "assists": "pm_assists",
        "damage_per_min": "pm_damage_per_min",
        "denies": "pm_denies",
        "net_worth": "pm_net_worth",
        "win": "pm_win",
        'matches_played': 'p_total_matches_played',
        'total_kills': 'p_total_kills',
        'total_deaths': 'p_total_deaths',
        'total_assists': 'p_total_assists',
        'avg_kd': 'p_total_avg_kd',
        'win_rate': 'p_total_win_rate',
        'total_time_played': 'p_total_time_played'
    }, inplace=True)

    return df

def create_player_hero_stats(ph_stats_base: pd.DataFrame) -> pd.DataFrame:
    """
    Create player hero stats by aggregating the player_hero_stats DataFrame.
    Checks for potential divide by zero errors and sets result to 0 if denominator is zero.
    """
    ph_stats = pd.DataFrame()
    ph_stats = ph_stats_base.copy()

    # Avoid divide by zero for deaths
    ph_stats['ph_total_kd'] = np.where(ph_stats['deaths'] == 0, 0, ph_stats['kills'] / ph_stats['deaths'])

    ph_stats['h_total_kd'] = (ph_stats.groupby('hero_id')['ph_total_kd'].transform("mean"))
    # Avoid divide by zero for ph_total_kd
    ph_stats['ph_kd_ratio'] = np.where(ph_stats['ph_total_kd'] == 0, 0, ph_stats['h_total_kd']/ ph_stats['ph_total_kd'])

    ph_stats['h_avg_total_time_played'] = (ph_stats.groupby('hero_id')['time_played'].transform("mean"))
    # Avoid divide by zero for h_avg_total_time_played
    ph_stats['ph_time_played_ratio'] = np.where(ph_stats['h_avg_total_time_played'] == 0, 0, ph_stats['time_played']/ ph_stats['h_avg_total_time_played'])

    ph_stats['h_total_damage_per_min'] = (ph_stats.groupby('hero_id')['damage_per_min'].transform("mean"))
    # Avoid divide by zero for h_total_damage_per_min
    ph_stats['ph_damage_per_min_ratio'] = np.where(ph_stats['h_total_damage_per_min'] == 0, 0, ph_stats['damage_per_min']/ ph_stats['h_total_damage_per_min'])

    ph_stats['h_total_assists'] = (ph_stats.groupby('hero_id')['assists'].transform("mean"))
    # Avoid divide by zero for h_total_assists
    ph_stats['ph_assists_ratio'] = np.where(ph_stats['h_total_assists'] == 0, 0, ph_stats['assists']/ ph_stats['h_total_assists'])

    # Avoid divide by zero for matches_played
    ph_stats['ph_win_rate'] = np.where(ph_stats['matches_played'] == 0, 0, ph_stats['wins'] / ph_stats['matches_played'])
    ph_stats['h_total_win_rate'] = (ph_stats.groupby('hero_id')['ph_win_rate'].transform("mean"))
    # Avoid divide by zero for h_total_win_rate
    ph_stats['ph_win_rate_ratio'] = np.where(ph_stats['h_total_win_rate'] == 0, 0, ph_stats['ph_win_rate'] / ph_stats['h_total_win_rate'])

    ph_stats.rename(columns={
        "wins": "ph_wins",
        "kills": "ph_kills",
        "deaths": "ph_deaths",
        "assists": "ph_assists",
        "damage_per_min": "ph_damage_per_min",
        'time_played': 'ph_time_played'
    }, inplace=True)

    return ph_stats
