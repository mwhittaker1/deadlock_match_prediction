import pandas as pd
import logging
import duckdb
import time
from services import database_functions as dbf
from services import fetch_data as fd
from services import transform_and_load as tal
from services import function_tools as u
import data.db as db


def run_etl_bulk_matches(max_days_fetch=2):
    """ETL for bulk match data, fetches, normalizes and loads into db"""
    # Fetch data
    logging.info(f"*INFO* ETL: Fetching data")
    matches_grouped_by_day = fd.bulk_fetch_matches(max_days_fetch,min_days=1,max_days=0)
    logging.info(f"*INFO* ETL: Data fetched")
    
    # Normalize data
    logging.info(f"*INFO* ETL: Normalizing data")
    normalized_matches, normalized_players  = tal.normalize_bulk_matches(matches_grouped_by_day)
    logging.info(f"*INFO* ETL: Data normalized")
    
    # Load data into database
    logging.info(f"*INFO* ETL: Loading data into database")
    tal.save_bulk_matches_to_db(normalized_matches, normalized_players)
    logging.info(f"*INFO* ETL: Data loaded into database")

def run_etl_hero_trends():
    """ETL for 7-day and 30-day hero trends"""
    logging.info("starting run_etl_hero_trends ETL without critical errors.")

    #ETL 7 day hero trends
    trend_window = 7
    logging.info(f"*INFO* ETL: Fetching hero trends for {trend_window} days")
    raw_hero_trends_7d = fd.fetch_hero_trends(trend_window)
    hero_trends_7d = tal.build_hero_trends(trend_window,raw_hero_trends_7d)
    tal.save_hero_trends_to_db(hero_trends_7d)

    #ETL 30d hero trends
    trend_window = 30
    logging.info(f"*INFO* ETL: Fetching hero trends for {trend_window} days")
    raw_hero_Trends_30d = fd.fetch_hero_trends(trend_window)
    hero_trends_30d = tal.build_hero_trends(trend_window,raw_hero_Trends_30d)
    tal.save_hero_trends_to_db(hero_trends_30d)
    logging.info("completed 7d and 30d hero trends ETL without critical errors.")

def run_etl_player_hero_fetched_match_trends():
    """ETL all players for a set of matches.
    
    Extracts distinct players from player matches table,
    for each player (account_id) fetch their match history,
    calculates player trends, calculate streaks and counts,
    calculate player_hero trends, calcualte recent match history,
    and insert into approriate tables.
    """
    logging.info("Starting function tests")
    # pull distinct players from player_matches table
    start = time.time()
    try:
        players_to_trend = dbf.pull_trend_players_from_db(con=db.con)

        if players_to_trend is None or players_to_trend.empty:
            raise ValueError("Players to trend is empty, expected a non-empty DataFrame")
        
    except Exception as e:
        logging.error(f"Error fetching players to trend: {e}")
        return
    
    #fetch match history for each player
    total_players_to_process = len(players_to_trend)
    for player in players_to_trend.itertuples():
        if player.Index % 10 == 0:
            elapsed_time = time.time() - start
            print(f"Time passed = {elapsed_time:.2f}seconds. Processed {player.Index} of {total_players_to_process} players")
            logging.info(f"Time passed = {elapsed_time:.2f}seconds. Processed {player.Index} of {total_players_to_process} players")
        logging.debug(f"Processing player {player} of {total_players_to_process}")
        account_id = player.account_id

        #fetch player match history from API endpoint
        try:
            player_match_history = fd.fetch_player_match_history(account_id)
            if player_match_history.empty or player_match_history is None:
                logging.warning(f"No match history found for player {account_id}")
                continue

        except Exception as e:
            logging.error(f"Error processing player {account_id}: {e}")

       # caclulcate player won stat and add as new column
        player_match_history = tal.calculate_won_column(player_match_history)

        #calculate player trends and streaks
        player_stats = tal.compute_player_stats(player_match_history)
        logging.debug(f"Player stats columns: {player_stats.columns}")

        # combine player_stats and player_hero trends, then insert into db
        hero_trends = dbf.pull_hero_trends_from_db(db.con,trend_window_days=30)
        player_stats = tal.process_player_hero_stats(player_stats,hero_trends)
        logging.debug(f"POST MERGE, Player stats columns: {player_stats.columns}")
        tal.save_player_trends_to_db(player_stats)
        
        # complete calculations for rolling stats and insert into db.
        player_rolling_stats = tal.compute_player_rolling_stats(player_match_history)
        tal.save_player_rolling_stats_to_db(player_rolling_stats)

    print("Completed test run")
    logging.info(f"*TEST*completed player_trends")
    return

def run_etl_player_hero_match_trends_from_db():
    """ETL all players for a set of matches.
    
    Extracts distinct players from player matches table,
    for each player (account_id) fetch their match history
    from player_matches_history table,
    calculates player trends, calculate streaks and counts,
    calculate player_hero trends, calcualte recent match history,
    and insert into approriate tables.
    """
    logging.info("Starting function tests")
    # pull distinct players from player_matches table
    start = time.time()
    try:
        players_to_trend = dbf.pull_trend_players_from_db(con=db.con)

        if players_to_trend is None or players_to_trend.empty:
            raise ValueError("Players to trend is empty, expected a non-empty DataFrame")
        
    except Exception as e:
        logging.error(f"Error fetching players to trend: {e}")
        return
    
    #fetch match history for each player
    total_players_to_process = len(players_to_trend)
    for player in players_to_trend.itertuples():
        if player.Index % 10 == 0:
            elapsed_time = time.time() - start
            logging.info(f"Time passed = {elapsed_time:.2f}seconds. Processed {player} of {total_players_to_process} players")
        logging.debug(f"Processing player {player} of {total_players_to_process}")
        account_id = player.account_id

    #fetch player match history from db
        try:
            player_match_history = dbf.pull_player_match_history_from_db(db.con,account_id)
            u.any_to_csv(player_match_history, "data/test_data/player_match_history_from_db")

        except Exception as e:
            logging.error(f"Error processing player {account_id}: {e}")

            #calculate player trends and streaks
            player_stats = tal.compute_player_stats(player_match_history) #player_match_histry = df, #player_stats pd.Series
            logging.debug(f"Player stats columns: {player_stats.columns}")

            # combine player_stats and player_hero trends, then insert into db
            hero_trends = dbf.pull_hero_trends_from_db(db.con,trend_window_days=30)
            player_stats = tal.process_player_hero_stats(player_stats,hero_trends)
            logging.debug(f"POST MERGE, Player stats columns: {player_stats.columns}")
            #tal.save_player_trends_to_db(player_stats)
            print(f"testing player starts. \n {player_stats.columns} \n {player_stats.head()}")
            u.any_to_csv(player_stats, "data/test_data/player_stats_from_db")
            
            # complete calculations for rolling stats and insert into db.
            player_rolling_stats = tal.compute_player_rolling_stats(player_match_history)
            #tal.save_player_rolling_stats_to_db(player_rolling_stats)
            print(f"testing player_rolling_stats. \n {player_rolling_stats.columns} \n {player_rolling_stats.head()}")
            u.any_to_csv(player_rolling_stats, "data/test_data/player_rolling_stats_from_db")
            
            # calculate palyer_history_streak and insert into db
            player_match_history_streaks = tal.get_player_win_loss_streak(db.con,player_match_history,streak_length=6)
            #tal.save_player_history_streaks_to_db(player_match_history)
            print(f"testing player_match_history. \n {player_match_history_streaks.columns} \n {player_match_history_streaks.head()}")
            u.any_to_csv(player_match_history_streaks, "data/test_data/player_match_history_from_db")

if __name__ == "__main__":
    pass
    

def old_run_etl_player_hero_match_trends_from_db():
    """ETL all players for a set of matches.
    
    Extracts distinct players from player matches table,
    for each player (account_id) fetch their match history
    from player_matches_history table,
    calculates player trends, calculate streaks and counts,
    calculate player_hero trends, calcualte recent match history,
    and insert into approriate tables.
    """
    logging.info("Starting function tests")
    # pull distinct players from player_matches table
    start = time.time()
    try:
        players_to_trend = dbf.pull_trend_players_from_db(con=db.con)

        if players_to_trend is None or players_to_trend.empty:
            raise ValueError("Players to trend is empty, expected a non-empty DataFrame")
        
    except Exception as e:
        logging.error(f"Error fetching players to trend: {e}")
        return
    
    #fetch match history for each player
    total_players_to_process = len(players_to_trend)
    for player in players_to_trend.itertuples():
        if player.Index % 10 == 0:
            elapsed_time = time.time() - start
            print(f"Time passed = {elapsed_time:.2f}seconds. Processed {player.Index} of {total_players_to_process} players")
            logging.info(f"Time passed = {elapsed_time:.2f}seconds. Processed {player.Index} of {total_players_to_process} players")
        logging.debug(f"Processing player {player} of {total_players_to_process}")
        account_id = player.account_id

        #fetch player match history from db
        try:
            player_match_history = dbf.pull_player_match_history_from_db(db.con,account_id)
            if player_match_history.empty or player_match_history is None:
                logging.warning(f"No match history found for player {account_id}")
                continue

        except Exception as e:
            logging.error(f"Error processing player {account_id}: {e}")

       # caclulcate player won stat and add as new column
        #player_match_history = tal.calculate_won_column(player_match_history)

        #calculate player trends and streaks
        player_stats = tal.compute_player_stats(player_match_history)
        logging.debug(f"Player stats columns: {player_stats.columns}")

        # combine player_stats and player_hero trends, then insert into db
        hero_trends = dbf.pull_hero_trends_from_db(db.con,trend_window_days=30)
        player_stats = tal.process_player_hero_stats(player_stats,hero_trends)
        logging.debug(f"POST MERGE, Player stats columns: {player_stats.columns}")
        tal.save_player_trends_to_db(player_stats)
        
        # complete calculations for rolling stats and insert into db.
        player_rolling_stats = tal.compute_player_rolling_stats(player_match_history)
        tal.save_player_rolling_stats_to_db(player_rolling_stats)

    print("Completed test run")
    logging.info(f"*TEST*completed player_trends")
    return