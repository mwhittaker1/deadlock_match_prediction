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

    """
    #ETL 7 day hero trends
    trend_window = 7
    logging.info(f"*INFO* ETL: Fetching hero trends for {trend_window} days")
    raw_hero_trends_7d = fd.fetch_hero_trends(trend_window)
    hero_trends_7d = tal.build_hero_trends(trend_window,raw_hero_trends_7d)
    tal.save_hero_trends_to_db(hero_trends_7d)"""

    #ETL 30d hero trends
    trend_window = 30
    logging.info(f"*INFO* ETL: Fetching hero trends for {trend_window} days")
    raw_hero_Trends_30d = fd.fetch_hero_trends(trend_window)
    hero_trends_30d = tal.build_hero_trends(trend_window,raw_hero_Trends_30d)
    tal.save_hero_trends_to_db(hero_trends_30d)
    logging.info("completed 7d and 30d hero trends ETL without critical errors.")

def batched_etl_player_hero_match_trends_from_db():
    logging.info("Starting run_etl_player_hero_match_trends_from_db tests")
    # pull distinct players from player_matches table
    start = time.time()
    try:
        players_to_trend = dbf.pull_trend_players_from_db(con=db.con)

        if players_to_trend is None or players_to_trend.empty:
            raise ValueError("Players to trend is empty, expected a non-empty DataFrame")
        
    except Exception as e:
        logging.error(f"Error fetching players to trend: {e}")
        return
    
    batch_size = 250
    hero_trends = dbf.pull_hero_trends_from_db(db.con,trend_window_days=30)
    total_players = len(players_to_trend)

    for batch_start in range(0, total_players, batch_size):
        batch_end = min(batch_start + batch_size, total_players)
        current_batch = players_to_trend.iloc[batch_start:batch_end]

        account_ids = current_batch['account_id'].tolist()
        account_ids_str = ', '.join(map(str, account_ids))

        logging.info(f"Processing batch {batch_start//batch_size + 1}/{(total_players+batch_size-1)//batch_size} "
            f"({batch_start}-{batch_end-1} of {total_players} players)")

        try:
            with duckdb.connect(db.DB_PATH) as con:
                con.execute(f"""
                    CREATE TEMPORARY TABLE temp_player_match_history as
                    SELECT * FROM player_matches_history
                    WHERE account_id IN ({account_ids_str})
                            """)

                player_trend_batch = []
                rolling_stats_batch = []

                for account_id in account_ids:
                    player_history_df = con.execute(f"""
                        SELECT * FROM temp_player_match_history
                        WHERE account_id = {account_id}
                    """).fetchdf()

                    if player_history_df.empty:
                        logging.warning(f"No match history found for player {account_id}")
                        continue
                    try:
                        player_stats = tal.compute_player_stats(player_history_df)
                        player_trend_batch.append(player_stats)
                    except Exception as e:
                        logging.error(f"Error processing player {account_id}: {e}")
                        continue
                    try:
                        rolling_stats = tal.compute_player_match_history(player_history_df)
                        rolling_stats = tal.process_player_hero_stats(rolling_stats, hero_trends)
                        rolling_stats_batch.append(rolling_stats)
                    except Exception as e:
                        logging.error(f"Error processing player {account_id}: {e}")
                        continue

                con.execute("DROP TABLE temp_player_match_history")
                if player_trend_batch:
                    player_trends_df = pd.concat(player_trend_batch, ignore_index=True)
                    tal.save_player_trends_to_db(player_trends_df)
                    con.register("batch_player_trends", player_trends_df)
                    con.execute("""
                        INSERT INTO player_hero_trends
                        SELECT * FROM batch_player_trends
                                """)
                    logging.info(f"Inserted {len(player_trends_df)} rows into player_hero_trends")

                if rolling_stats_batch:

                    #concate rolling_stats in batch and insert
                    rolling_stats_df = pd.concat(rolling_stats_batch, ignore_index=True)
                    tal.save_computed_player_match_data_to_db(rolling_stats_df)
                    rolling_stats_batch = []

        except Exception as e:
            logging.error(f"Error saving player rolling stats to DB: {e}")

        batch_time = time.time() - start
        remaining_batches = (total_players - batch_end) / batch_size
        estimated_time_remaining = remaining_batches * (batch_time / ((batch_end - batch_start) / batch_size))
        logging.info(f"Batch completed in {batch_time:.2f}s. Estimated time remaining: {estimated_time_remaining:.2f}s")
        
    total_time = time.time() - start                
    logging.info(f"Total time taken for processing {total_players} players: {total_time:.2f} seconds")
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
    logging.info("Starting run_etl_player_hero_match_trends_from_db tests")
    # pull distinct players from player_matches table
    start = time.time()
    try:
        players_to_trend = dbf.pull_trend_players_from_db(con=db.con)

        if players_to_trend is None or players_to_trend.empty:
            raise ValueError("Players to trend is empty, expected a non-empty DataFrame")
        
    except Exception as e:
        logging.error(f"Error fetching players to trend: {e}")
        return
    
    batch_size = 250
    hero_trends = dbf.pull_hero_trends_from_db(db.con,trend_window_days=30)

    #fetch match history for each player
    total_players = len(players_to_trend)
    player_trends_batch= []
    rolling_stats_batch = []

    for i, player in enumerate(players_to_trend.itertuples()):
        if i % 50 == 0:
            elapsed_time = time.time() - start
            print(f"Time passed = {elapsed_time:.2f}seconds. Processed {player} of {total_players} players")
            logging.info(f"Time passed = {elapsed_time:.2f}seconds. Processed {player} of {total_players} players")
        logging.debug(f"Processing player {player} of {total_players}")
        account_id = player.account_id

            #fetch player match history from db
        try:
            player_match_history = dbf.pull_player_match_history_from_db(account_id,db.con)
            #u.any_to_csv(player_match_history, "data/test_data/raw_player_match_history_from_db")
            if player_match_history.empty or player_match_history is None:
                logging.warning(f"No match history found for player {account_id}")
                
        except Exception as e:
            logging.error(f"Error processing player {account_id}: {e}")
            
        try:    
            #calculate player trends and streaks
            player_stats = tal.compute_player_stats(player_match_history) #player_match_histry = df, #player_stats pd.Series
            logging.debug(f"Player stats columns: {player_stats.columns}")
            player_trends_batch.append(player_stats)
        except Exception as e:
            logging.error(f"Error processing player {account_id}: {e}")
        
        try:   
            # complete calculations for rolling stats  
            player_rolling_stats = tal.compute_player_match_history(player_match_history) 
            # compile player_hero trends    
            player_rolling_stats = tal.process_player_hero_stats(player_rolling_stats, hero_trends)
       
            rolling_stats_batch.append(player_rolling_stats)
        except Exception as e:
            logging.error(f"Error processing player {account_id}: {e}")
        
        if len(player_trends_batch) >= batch_size or i == total_players - 1:
            if player_trends_batch:
                try:
                    #concate player_stats in batch and insert
                    player_trends_df = pd.concat(player_trends_batch, ignore_index=True)
                    tal.save_player_trends_to_db(player_trends_df)
                    player_trends_batch = []
                except Exception as e:
                    logging.error(f"Error saving player trends to DB: {e}")
            if rolling_stats_batch:
                try:
                    #concate rolling_stats in batch and insert
                    rolling_stats_df = pd.concat(rolling_stats_batch, ignore_index=True)
                    tal.save_computed_player_match_data_to_db(rolling_stats_df)
                    rolling_stats_batch = []
                except Exception as e:
                    logging.error(f"Error saving player rolling stats to DB: {e}")
    total_time = time.time() - start                
    logging.info(f"Total time taken for processing {total_players} players: {total_time:.2f} seconds")
    return

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
        player_rolling_stats = tal.compute_player_match_history(player_match_history)
        tal.save_computed_player_match_data_to_db(player_rolling_stats)

    print("Completed test run")
    logging.info(f"*TEST*completed player_trends")
    return

def setup_duckdb_indexes():
    """
    Set up indexes on player_matches_history table to improve query performance.
    Focus on account_id as the primary lookup column.
    """
    try:
        with duckdb.connect(database=db.DB_PATH) as con:
            logging.info(f"Setting up indexes on player_matches_history in {db.DB_PATH}")
            
            # In DuckDB, we can use PRAGMA_INDEX_LIST directly as a table function
            try:
                # Try to check if indexes exist by querying the internal system tables
                index_exists = con.execute("""
                    SELECT count(*) 
                    FROM information_schema.indexes 
                    WHERE table_name = 'player_matches_history' 
                    AND index_name = 'idx_player_matches_account_id'
                """).fetchone()[0]
                
                # If we reach here, the query was successful
                index_check_works = True
            except:
                # If the above fails, we'll just try to create the indexes and catch any errors
                index_check_works = False
                index_exists = 0
            
            if not index_check_works or index_exists == 0:
                # Create indexes safely by using a try-except block for each
                try:
                    # Create index on account_id
                    con.execute("""
                        CREATE INDEX IF NOT EXISTS idx_player_matches_account_id 
                        ON player_matches_history (account_id)
                    """)
                    logging.info("Created or confirmed index on player_matches_history(account_id)")
                except Exception as e:
                    logging.warning(f"Could not create account_id index: {str(e)}")
                
                try:
                    # Create index on combo of account_id and match_id
                    con.execute("""
                        CREATE INDEX IF NOT EXISTS idx_player_matches_account_match 
                        ON player_matches_history (account_id, match_id)
                    """)
                    logging.info("Created or confirmed index on player_matches_history(account_id, match_id)")
                except Exception as e:
                    logging.warning(f"Could not create account_id+match_id index: {str(e)}")
                
                try:
                    # Optional: Add index on start_time for time-based queries
                    con.execute("""
                        CREATE INDEX IF NOT EXISTS idx_player_matches_start_time 
                        ON player_matches_history (start_time)
                    """)
                    logging.info("Created or confirmed index on player_matches_history(start_time)")
                except Exception as e:
                    logging.warning(f"Could not create start_time index: {str(e)}")
            else:
                logging.info("Indexes already exist on player_matches_history")
                
            # Analyze table to update statistics
            try:
                con.execute("ANALYZE player_matches_history")
                logging.info("Analyzed player_matches_history table to update statistics")
            except Exception as e:
                logging.warning(f"Could not analyze table: {str(e)}")
            
    except Exception as e:
        logging.exception(f"Failed to set up indexes: {str(e)}")
        raise

def run_etl_player_hero_fetched_match_trends():
    """ETL all players for a set of matches.
    
    Extracts distinct players from player matches table,
    for each player (account_id) fetch their match history,
    calculates player trends, calculate streaks and counts,
    calculate player_hero trends, calcualte recent match history,
    and insert into approriate tables.
    """
    logging.info("Starting run_etl_player_hero_fetched_match_trends")
    # pull distinct players from player_matches table
    start = time.time()
    try:
        players_to_trend = dbf.test_pull_trend_players_from_db(con=db.con)

        if players_to_trend is None or players_to_trend.empty:
            raise ValueError("Players to trend is empty, expected a non-empty DataFrame")
        
    except Exception as e:
        logging.error(f"Error fetching players to trend: {e}")
        return
    
    #fetch match history for each player
    total_players_to_process = len(players_to_trend)
    hero_trends = dbf.pull_hero_trends_from_db(db.con,trend_window_days=30)
    for player in players_to_trend.itertuples():
        if player.Index % 10 == 0:
            elapsed_time = time.time() - start
            print(f"Time passed = {elapsed_time:.2f}seconds. Processed {player.Index} of {total_players_to_process} players")
            logging.info(f"Time passed = {elapsed_time:.2f}seconds. Processed {player.Index} of {total_players_to_process} players")
        logging.debug(f"Processing player {player} of {total_players_to_process}")
        account_id = player.account_id

        #fetch player match history from API endpoint
        try:
            player_match_history = dbf.pull_player_match_history_from_db(account_id,con=db.con)
            if player_match_history.empty or player_match_history is None:
                logging.warning(f"No match history found for player {account_id}")
                continue

        except Exception as e:
            logging.error(f"Error processing player {account_id}: {e}")

       # caclulcate player won stat and add as new column
        player_match_history = tal.calculate_won_column(player_match_history)
        #u.any_to_csv(player_match_history, "data/test_data/won_column_player_match_history")
        #calculate player trends and streaks
        player_stats = tal.compute_player_stats(player_match_history)
        logging.debug(f"Player stats columns: {player_stats.columns}")

        # combine player_stats and player_hero trends, then insert into db
        player_stats = tal.process_player_hero_stats(player_stats,hero_trends)
        logging.debug(f"POST MERGE, Player stats columns: {player_stats.columns}")
        tal.save_player_trends_to_db(player_stats)
        
        # complete calculations for rolling stats and insert into db.
        player_rolling_stats = tal.compute_player_match_history(player_match_history)
        #u.any_to_csv(player_rolling_stats, "data/test_data/computer_player_rolling_stats")
        tal.save_computed_player_match_data_to_db(player_rolling_stats)

    print("Completed test run")
    logging.info(f"*TEST*completed player_trends")
    return