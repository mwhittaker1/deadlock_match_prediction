import data.fetch_data as fd
import data.process_data as dp
import logging
import numpy as np
import pandas as pd
import sklearn.preprocessing import MinMaxScaler

logging.basicConfig(level=logging.DEBUG)
logging = logging.getLogger(__name__)

"""Orchestrators for match data fetching, model training, and predictions"""

# Orchestrator for fetching a range of matches, and preparing for training
def orch_build_training_base(
        train_start_date, 
        train_end_date, 
        max_days=30, 
        folder_name="undefined", 
        compare_df=None, 
        retry_count=1) -> pd.DataFrame:
    """
    Orchestrator for building training data or append to existing data.

    args:
        train_start, train_end - Window of days to fetch matches for player list to train on
            e.g. train_start= 30, train_end= 1 (Fetch matches from 30 days ago to yesterday)
        *note - selecting 0 limits the number of matches able to use for prediction since it'll capture
            all available matches.
        max_days defaults to 30 which will override the time period sent as a fail-safe
    config: 
        folder_name - Name of the folder to save the processed data
        compare_df - if a second dataframe is provided, it will compare the player dataframes,
            identify gaps, and run batch processing on missing ids
    returns:
        pd.DataFrame of data, prepared for training a ML model
    """

    from datetime import datetime
    import os

    now = datetime.now()
    date = now.date()
    hour = now.time()

    list_match_days = fd.bulk_fetch_matches(
        max_days_ago=train_end_date, 
        min_days_ago=train_start_date, 
        max_days=max_days)
    
    # Show how many matches in each day
    if logging.debug("Match data by day:"):
        for i, day_data in enumerate(list_match_days):
            if "error" in day_data:
                logging.debug(f"Day {i}: Error")
            else:
                logging.debug(f"Day {i}: {len(day_data)} matches")
        logging.debug(f"data = {list_match_days}")
    
    df_matches, df_players = dp.separate_match_players(list_match_days)

    # Show the shape of the resulting DataFrames
    logging.debug(f"Matches DataFrame shape: {df_matches.shape}")
    logging.debug(f"Players DataFrame shape: {df_players.shape}")

    os.makedirs(f"{date}_data", exist_ok=True)

    df_matches.to_csv(f"{date}_data/matches_{train_start_date}-{train_end_date}.csv", index=False)
    df_players.to_csv(f"{date}_data/players_{train_start_date}-{train_end_date}.csv", index=False)

    if compare_df:
        # Compare player dataframes and run processing if a second dataframe is provided
        mismatched_count, missing_ids = dp.compare_player_dataframes(df_players, compare_df)
        logging.info(f"Number of mismatched players: {mismatched_count}")
        player_stats, player_hero_stats = dp.run_player_batches(
            missing_ids['account_id'].unique(),
            batch_size=200,
            max_workers_per_Batch=25,
            timeout=30
            )

    else:
        logging.info(f"running batches on len(df_players['account_id'].unique())")
        player_stats, player_hero_stats = dp.run_player_batches(df_players['account_id'].unique())
        
        logging.info(f"Processed player stats: {player_stats.shape[0]} rows")
        logging.info(f"Processed player hero stats: {player_hero_stats.shape[0]} rows")

    #Save the complete data to CSV files if processing was successful
    if not player_stats.empty:
        player_stats.to_csv("v2_data/player_stats.csv", index=False)
        player_hero_stats.to_csv("v2_data/player_hero_stats.csv", index=False)
        print("Data saved to CSV files in v2_data folder")

    if retry_count > 0:
        logging.info(f"Retrying failed players {retry_count} more times")

        player_stats, player_hero_stats = dp.retry_failed_players(df_players, player_stats)
        retry_count -=1
        if retry_count == 0:
            logging.info(f"Completed retries for failed players. Total failed players: {len(df_players['account_id'].unique()) - player_stats.shape[0]}")

        player_stats.to_csv("v2_data/player_stats_with_retries.csv", index=False)
        player_hero_stats.to_csv("v2_data/player_hero_stats_with_retries.csv", index=False)

    # merge match and player stats
    logging.info("Merging player match stats")
    player_match_stats = dp.merge_player_match_stats(df_players, player_stats)

    # rename columns to match player vs pm
    logging.info("Renaming player match stats columns")
    player_match_stats = dp.rename_match_stats(player_match_stats)

    # Generate player hero statistics
    logging.info("Calculating player hero stats")
    calc_player_hero_stats = dp.create_player_hero_stats(player_hero_stats)

    # Merge player match stats with player hero stats
    logging.info("Merging player match stats with player hero stats")
    phm_stats = pd.merge(player_match_stats,calc_player_hero_stats,on=["hero_id", "account_id"], how="left")

    # Checkpoint, save to .csv
    logging.info("Saving player hero match stats to CSV")
    phm_stats.to_csv("v2_data/player_hero_match_stats.csv", index=False)

    return phm_stats

# Orchestrator for final data processing and training a model

def orch_model_training(model_type="rf", aggregate_type="differential")->pd.DataFrame:
    return


# Orchestrator for running a set of match predictions



if __name__ == "__main__":
    print("no errors")