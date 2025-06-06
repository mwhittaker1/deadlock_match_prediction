import json
import pandas as pd
import logging
import os
from data import db
from pathlib import Path
from services import orchestrators as o
from services import function_tools as u
from services import database_functions as dbf
from services import fetch_data as fd
from services import transform_and_load as tal

def test_save_data(file,fname):
    tests_dir = Path(__file__).parent / "dl_match_prediction" / "tests"
    tests_dir.mkdir(parents=True, exist_ok=True)   # make sure it exists
    out_file = tests_dir / f"{fname}.json"
    with out_file.open("w", encoding="utf-8") as f:
        json.dump(file, f, ensure_ascii=False, indent=2)

def test_pull_players_to_trend(con):
    """test for pull_players_to_trend"""
    query = """
    SELECT DISTINCT account_id
    FROM player_matches
    LIMIT 10
    """
    
    players = con.execute(query).fetchdf()
    print(f"players: {players}")
    logging.info(f"Pulled {len(players)} players to trend")
    return players

def test_live_match_fetch_data():

    print(f"*TEST* Test #1, fetch_match_data with just limit")
    data = fd.fetch_match_data(limit = 1)
    assert isinstance(data, list), "Data should be a list"
    for match in data:
        assert "match_id" in match, "Data should contain 'match_id' key"
    for players in data:
        assert "players" in players, "Data should contain 'players' key"
        assert len(players["players"]) > 0, "'players' list should not be empty"
        assert "account_id" in players["players"][0], "'players' should contain 'account_id' key"
    test_save_data(data, "test_match_data")
    print(f"*TEST* Test #1 concluded.")

    print(f"*TEST* Test #2, fetch_match_data with min max times")
    data = fd.fetch_match_data(limit = 1, min_unix_timestamp=0, max_unix_timestamp=1)
    earliest = min(data, key=fd.parse_ts)
    latest   = max(data, key=fd.parse_ts)

    assert isinstance(data, list), "Data should be a list"
    for match in data:
        assert "match_id" in match, "Data should contain 'match_id' key"
        assert "start_time" in match, "Data should contain 'start_time' key"
    for players in data:
        assert "players" in players, "Data should contain 'players' key"
        assert len(players["players"]) > 0, "'players' list should not be empty"
        assert "account_id" in players["players"][0], "'players' should contain 'account_id' key"
    print(f"**INFO** min anx max times are: {earliest} & {latest}")

def test_fetch_data_timestamps():
    print(f"\n\n***Starting Function Tests****\n\n")
    data= fd.fetch_match_data(limit=2,min_unix_timestamp=1, max_unix_timestamp=0)
    print(f"**TEST** printed data {data}")
    assert isinstance(data, list), "Data should be a list"
    for match in data:
        assert "match_id" in match, "Data should contain 'match_id' key"
        assert "start_time" in match, "Data should contain 'start_time' key"
    for players in data:
        assert "players" in players, "Data should contain 'players' key"
        assert len(players["players"]) > 0, "'players' list should not be empty"
        assert "account_id" in players["players"][0], "'players' should contain 'account_id' key"
    print(f"**TEST** printed data: {data}")
    print(f"\n\n***Function Tests Complete****\n\n")

def test_bulk_fetch_matches(max_days_fetch=4,min_days=1,max_days=0)->json:
    """fetches a batch of matches, 1 day per pull, returns json and exports.

    batch is unnormalized, 'players' contains a df of each matches 'players'
    limit = max matches within a day to pull
    max_days_fetch = how many days to cycle through for total fetch"""

    #print(f"start")
    limit = 2
    batch_matches = []
    
    for batch in range(max_days_fetch):
        print(f"\n*day:{batch} min ={min_days} max = {max_days}")
        fetched_matches = fd.fetch_match_data(min_unix_timestamp=min_days, max_unix_timestamp=max_days, limit=limit)
        batch_matches.append(fetched_matches)
        max_days +=1
        min_days +=1

    return batch_matches

def save_response_to_file(response, filename):
    with open("dl_match_prediction/tests/match_response.json", "w", encoding="utf-8") as f:
        json.dump(response, f, indent=2)

def match_fixture(path)->json:
    fixture_path = path
    with fixture_path.open("r", encoding="utf-8") as f:
        return json.load(f)

def test_orc_bulk_fetch_matches():

    max_days_fetch = 2
    print(f"\n\n***Starting Function Tests****\n\n")
    list_matches= test_bulk_fetch_matches(max_days_fetch=2)
    #save_response_to_file(list_matches, "match_response")

    #list_matches = match_fixture()

    # list_matches = [match_day1,match_day2] #json
    # match_day1 = [match1,match2,match3] ##dict
    # match1 = {match_id, start_time, players: [player1,player2]} #dict
    # player1 = {account_id, hero_id, ...} #dict

    assert isinstance(list_matches, list), "Data should be list"
    assert len(list_matches) == max_days_fetch,f"data should be list of max_days_fetch got len list_matches:{len(list_matches)}, max_days_fetch:{max_days_fetch}"
    print(list_matches)
    for idx,day_matches in enumerate(list_matches):
        assert isinstance(day_matches, list), f"Day {idx} should be list, got {type(day_matches)}"
        for match in day_matches:
            assert isinstance(match, dict), f"Each match should be a dict type is {type(match)} match is {match}"
            assert "match_id"   in match, "Each match needs a match_id"
            assert "start_time" in match, "Each match needs a start_time"
            assert "players"    in match, "Each match needs a players list"
            players = match["players"]
            assert isinstance(players, list), f"'players' should be a list each players is {type(players)}"
            assert players, "'players' list must not be empty"
            assert "account_id" in players[0], "Each player needs an account_id"
    print(f"\n\n***Function Tests Complete****\n\n")

def test_local_etl_bulk_matches():
    
    p = Path(__file__).parent / "match_response.json"
    if not p.exists():
        logging.critical(f"Fixture file {p} not found. Exiting test.")
        return
    
    # Fetch data
    print(f"*INFO* ETL: Fetching match data")
    matches_grouped_by_day = match_fixture(p)
    print(f"*INFO* ETL: Data fetched")
    
    # Normalize data
    print(f"*INFO* ETL: Normalizing data")
    normalized_matches, normalized_players = tal.normalize_bulk_matches(matches_grouped_by_day)
    print(f"*INFO* ETL: Data normalized")
    u.df_to_csv(normalized_matches, "data/test_data/normalized_matches")
    u.df_to_csv(normalized_players, "data/test_data/normalized_players")
    # Load data into database
    #print(f"*INFO* ETL: Loading data into database")
    #dbf.load_bulk_matches(normalized_data)
    #print(f"*INFO* ETL: Data loaded into database")

def test_fetch_etl_bulk_matches():
    
    # Fetch data
    print(f"*INFO* ETL: Fetching match data")
    matches_grouped_by_day = fd.bulk_fetch_matches(max_days_fetch=2,min_days=1,max_days=0)
    print(f"*INFO* ETL: Data fetched")
    
    # Normalize data
    print(f"*INFO* ETL: Normalizing data")
    normalized_matches, normalized_players = tal.normalize_bulk_matches(matches_grouped_by_day)
    print(f"*INFO* ETL: Data normalized")
    u.df_to_csv(normalized_matches, "normalized_matches")
    u.df_to_csv(normalized_players, "normalized_players")
    # Load data into database
    #print(f"*INFO* ETL: Loading data into database")
    #dbf.load_bulk_matches(normalized_data)
    #print(f"*INFO* ETL: Data loaded into database")

def test_load_bulk_matches():
    # Load data into database
    normalized_matches = pd.read_csv("normalized_matches.csv")
    normalzed_players = pd.read_csv("normalized_players.csv")
    logging.info("Loading data into database")
    tal.save_bulk_matches_to_db(normalized_matches, normalzed_players)
    logging.info("Data loaded into database")

def test_fetch_hero_trends()->pd.DataFrame:
    """fetches hero trends for 7 days, saves to csv data/test_data/hero_trends_7d.csv"""
    hero_df = fd.fetch_hero_data(min_unix_time=7, min_average_badge=0)
    assert isinstance(hero_df, pd.DataFrame), "Data should be a DataFrame"
    if len(hero_df) != 26:
        logging.warning(f"Data should have 26 rows, has {len(hero_df)} rows")
    u.df_to_csv(hero_df, "data/test_data/hero_data")
    print(f"\n\nHero data fetched, hero_data = {hero_df.head()} hero_df len = {len(hero_df)}\n\n")
    return hero_df

def test_etl_hero_trends_single():
    print(f"\n\n***Starting Function Tests****\n\n")
    dbf.reset_all_tables(db.con)
    base_hero_trends = test_fetch_hero_trends()
    transformed_hero_trends = tal.build_hero_trends(trend_range=7, hero_trends=base_hero_trends)
    assert isinstance(transformed_hero_trends, pd.DataFrame), "Transformed data should be a DataFrame"
    tal.save_hero_trends_to_db(transformed_hero_trends)    
    print(f"\n\n ***Function Tests Complete****\n\n")

def test_orchestrators():
    #dbf.reset_all_tables(db.con)
    o.run_etl_bulk_matches(max_days_fetch=2)
    #o.run_etl_hero_trends()

def test_calculate_won_column(df)->pd.DataFrame:
    """Calculates the 'won' column based on 'match_result' and 'player_team'."""
    logging.info("Calculating 'won' column")
    df['won'] = df['match_result'] == df['player_team']
    return df

def test_players_to_trend_from_db():
    logging.info("Starting player statistics ETL process")
    
        # fetch hero trends from db to compare with player stats later.

    players_to_trend = dbf.test_pull_players_to_trend(con=db.con)
    logging.info(f"Found {len(players_to_trend)} players to process")
    batch_size = 5
    total_players_to_process = len(players_to_trend)
    all_player_stats = []

    for i in range(0, total_players_to_process, batch_size):
        batch_players = players_to_trend[i:i+batch_size]
        logging.info(f"Processing batch of {len(batch_players)} players from total {total_players_to_process}")
        logging.debug(f"Batch players: {batch_players}")
        batch_players_histories = []
        # Perform operations on the batch of players
        for _,player in batch_players.iterrows():
            account_id = player["account_id"]
            logging.debug(f"Processing player with account_id: {account_id}")
            # Fetch and process match data for the player
            try:
                player_match_history = fd.fetch_player_match_history(account_id)
                if not player_match_history.empty:
                    batch_players_histories.append(player_match_history)
                    logging.debug(f"Processed match history for player {account_id}, lenght = {len(player_match_history)}")
            except Exception as e:
                logging.warning(f"Error processing player {account_id}, error: {e}")

        u.any_to_csv(batch_players_histories, "data/test_data/normalized_player_histories")
        #calculate player, player_hero trends
        logging.info(f"Calculating player trends for {len(batch_players_histories)} of {len(players_to_trend)} players")
        
        for player_history in batch_players_histories:
            logging.debug(f"Calculating player base stats for player {player_history['account_id']} \n**history:\n\n {player_history}")
            player_stats = tal.generate_player_performance_metrics(player_history)
            all_player_stats.append(player_stats)
        logging.debug(f"\n**full all player stats:\n\n {all_player_stats}")
        u.any_to_csv(all_player_stats, "data/test_data/player_statscsv")
        logging.debug(f"length of all_player_stats: {len(all_player_stats)}")
        df_player_stats = pd.DataFrame(all_player_stats)
        logging.debug(f"length of df_player_stats converted to df: {len(df_player_stats)}")
        logging.debug(f"Calculated player trends.\ndata type = {type(df_player_stats)} \nexample data:\n\n {all_player_stats[:2]}")
    u.any_to_csv(all_player_stats, "data/test_data/player_stats")
    return all_player_stats, batch_players_histories

def test_count_player_streaks(player_history: pd.DataFrame)->pd.DataFrame:
    """counts player streaks from match history"""
    
    logging.info("Counting player streaks")
    # Check if the player history is empty
    if player_history.empty:
        logging.warning("Player history is empty")
        return None
    
    #calculate win from match_result and player_team
    player_history = player_history.sort_values(by=['start_time'])
    #creates unique identifier for each streak
    player_history['won_int'] = player_history['won'].astype(int)
    player_history['streak_change'] = (player_history['won'] != player_history['won']
                                    .shift()).astype(int)
    player_history['streak_id'] = player_history['streak_change'].cumsum()

    #group by streak_id and win to count streaks
    streaks = player_history.groupby('streak_id').agg(
        streak_len=('won', 'size'),
        won=('won', 'first'))

    # Win streak stats
    win_streaks = streaks[streaks['won'] == True]['streak_len']
    win_avg = win_streaks.mean()
    win_2 = (win_streaks >= 2).sum()
    win_3 = (win_streaks >= 3).sum()
    win_4 = (win_streaks >= 4).sum()
    win_5 = (win_streaks >= 5).sum()

     # Loss streak stats
    loss_streaks = streaks[streaks['won'] == False]['streak_len']
    loss_avg = loss_streaks.mean()
    loss_2 = (loss_streaks >= 2).sum()
    loss_3 = (loss_streaks >= 3).sum()
    loss_4 = (loss_streaks >= 4).sum()
    loss_5 = (loss_streaks >= 5).sum()

    player_history = player_history.drop(columns=['won_int', 'streak_change', 'streak_id'])
    player_history = player_history.assign(
        win_streaks_2plus=win_2,
        win_streaks_3plus=win_3,
        win_streaks_4plus=win_4,
        win_streaks_5plus=win_5,
        loss_streaks_2plus=loss_2,
        loss_streaks_3plus=loss_3,
        loss_streaks_4plus=loss_4,
        loss_streaks_5plus=loss_5,
        win_streaks_avg=win_avg,
        loss_streaks_avg=loss_avg
        )
    return player_history

def test_calculate_player_match_rolling_stats(player_history: pd.DataFrame)->pd.DataFrame:
    """Calculates player streak trends from player match history"""
    logging.info("Calculating player streak trends.")
    # Check if the player history is empty
    if player_history.empty:
        logging.warning("Player history is empty")
        return None

    player_history = player_history.sort_values(by='start_time', ascending=False)
    
    # calculates rolling player win percentage
    for w in range(2, 7):
        player_history[f'p_win_pct_{w}'] = player_history['won'].rolling(window=w).mean()*100
        player_history[f'p_loss_pct_{w}'] = (1 - player_history['won'].rolling(window=w).mean())*100
    logging.debug(f"Calculated player streak trends for player {player_history['account_id'].iloc[0]}")
    
    return player_history

def test_calculate_batch_player_streak_trends(list_dfs_player_history)->list:
    """Calculates player streak trends from player match history
    
    expects list of dataframes, each dataframe is a player history."""

    logging.info("Calculating player streak trends")

    batch_player_streaks = []
    batch_player_streaks_wl_avg = []
    # for each player histry in the list, calculate player streaks and streak counts
    for player_history in list_dfs_player_history:
        logging.debug(f"Calculating player streaks for: {player_history['account_id'].iloc[0]}")
        #create won column
        player_history = test_calculate_won_column(player_history)
        #from player_history 
        # create p_win_pct_x columns
        #player_stats = test_calculate_player_streak_trends(player_history)

        logging.debug(f"Calculated player count streaks for: {player_history['account_id'].iloc[0]}")
        #from player_history 
        # create steak counts and averages
        player_streaks, player_streak_wl_avg = test_count_player_streaks(player_history)
        batch_player_streaks.append(player_streaks)
        batch_player_streaks_wl_avg.append(player_streak_wl_avg)


    return batch_player_streaks, batch_player_streaks_wl_avg

def etl_player_player_match_trends():
    """ETL a list of players from player_matches table, transform, and re-insert
    
    Extracts distinct players from player matches table,
    for each player (account_id) fetch their match history,
    calculates player trends, calculate streaks and counts,
    calculate player_hero trends, and insert into approriate tables.
    """
    logging.info("Starting function tests")
    # pull distinct players from player_matches table
    try:
        players_to_trend = dbf.pull_trend_players_from_db(con=db.con)

        if players_to_trend is None or players_to_trend.empty:
            raise ValueError("Players to trend is empty, expected a non-empty DataFrame")
        
    except Exception as e:
        logging.error(f"Error fetching players to trend: {e}")
        return

    total_players_to_process = len(players_to_trend)
    for player in players_to_trend.itertuples():
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
        player_match_history = test_calculate_won_column(player_match_history)

        tal.compute_player_stats(player_match_history)

        # Calculate win per match and player trends
        
        # complete calculations for rolling stats and insert into db.
        tal.compute_rolling_stats(player_match_history)


    print("Completed test run")
    logging.info(f"*TEST*completed player_trends")
    return

def test_pull_player_match_history_from_db():
    import duckdb
    con = duckdb.connect("c:/Code/Local Code/deadlock_match_prediction/data/deadlock.db")

    test_df = dbf.pull_player_match_history_from_db(con,1699896029)
    print(f"Test completed, fetched player match history columns:\n {test_df.columns}\nlength expected 117:\n {len(test_df)}")
    pass

def test_run_tl_player_hero_match_trends_from_db():
    logging.debug("Starting function tests")
    account_id = 1036606523
    player_match_history = dbf.pull_player_match_history_from_db(db.con,account_id)
    u.any_to_csv(player_match_history, "data/test_data/player_match_history_from_db")
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
    player_rolling_stats = tal.compute_player_match_history(player_match_history)
    #tal.save_player_rolling_stats_to_db(player_rolling_stats)
    print(f"testing player_rolling_stats. \n {player_rolling_stats.columns} \n {player_rolling_stats.head()}")
    u.any_to_csv(player_rolling_stats, "data/test_data/player_rolling_stats_from_db")
    
    # calculate palyer_history_streak and insert into db
    player_match_history_streaks = tal.get_player_win_loss_streak(db.con,player_match_history,streak_length=6)
    #tal.save_player_history_streaks_to_db(player_match_history)
    print(f"testing player_match_history. \n {player_match_history_streaks.columns} \n {player_match_history_streaks.head()}")
    u.any_to_csv(player_match_history_streaks, "data/test_data/player_match_history_from_db")

    pass

if __name__ == "__main__":
    test_run_tl_player_hero_match_trends_from_db()

#outdated
def v1_calculate_player_trends():
    logging.info("Starting function tests")
    
    #unsing sample data
    list_players_history_stats = (pd.read_csv("data/test_data/player_stats.csv"))
    batch_players_histories = (pd.read_csv("data/test_data/normalized_player_histories.csv"))
    #turn normal df into a list again.
    list_batch_players_histories = [group for _, group in batch_players_histories.groupby("account_id")]

    #returns the player stats, consumes the player history dataframes
    batch_player_streaks, batch_player_streaks_wl_avg  = test_calculate_batch_player_streak_trends(list_batch_players_histories)    


        #fetch hero trends from db
        #handled in main handler.
        
        #calculate player_hero trends and return list of stats
    #players_hero_calculated_stats = calculate_player_hero_trends(list_batch_players_histories)

    #insert batch players into player_trends player_hero_trends tables
    print("Completed test run")
    logging.info(f"*TEST*completed player_trends")