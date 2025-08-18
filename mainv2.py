import data.fetch_data as fd
import data.process_data as dp
import logging

logging.basicConfig(level=logging.DEBUG)
logging = logging.getLogger(__name__)

"""Orchestrators for match data fetching, model training, and predictions"""

# Orchestrator for fetching a range of matches, and preparing for training
def orch_build_train_data(train_start, train_end, max_days=30, compare=None):
    """
    Orchestrator for building training data.

    args=
        train_start, train_end - Window of days to fetch matches for player list to train on
            e.g. train_start=-30, train_end=-1
        *note - selecting 0 limits the number of matches able to use for prediction
        max_days defaults to 30 which will override the time period sent as a fail-safe
    compare - if a second dataframe is provided, it will compare the player dataframes,
        identify mismatches, and run batch processing on missing ids
    returns:
        pd.DataFrame of data, prepared for training a ML model
    """

    from datetime import datetime
    import os

    now = datetime.now()
    date = now.date()
    hour = now.time()

    list_match_days = fd.bulk_fetch_matches(
        max_days_ago=train_end, 
        min_days_ago=train_start, 
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

    df_matches.to_csv(f"{date}_data/matches_{train_start}-{train_end}.csv", index=False)
    df_players.to_csv(f"{date}_data/players_{train_start}-{train_end}.csv", index=False)

    if compare:
        # Compare player dataframes and run processing if a second dataframe is provided
        mismatched_count, missing_ids = dp.compare_player_dataframes(df_players, compare)
        logging.info(f"Number of mismatched players: {mismatched_count}")
        player_stats, player_hero_stats = dp.process_player_stats_parallel(missing_ids['account_id'].unique())

    else:
        logging.info(f"running batches on len(df_players['account_id'].unique())")
        player_stats, player_hero_stats = dp.process_player_stats_parallel(df_players['account_id'].unique())


# Orchestrator for training a model


# Orchestrator for running a set of match predictions



if __name__ == "__main__":
    print("no errors")