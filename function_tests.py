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

log_file = os.getenv("data/logs.txt")  
logging.basicConfig(
    filename=log_file,
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] [%(name)s.%(funcName)s] %(message)s"
)

def test_save_data(file,fname):
    tests_dir = Path(__file__).parent / "dl_match_prediction" / "tests"
    tests_dir.mkdir(parents=True, exist_ok=True)   # make sure it exists
    out_file = tests_dir / f"{fname}.json"
    with out_file.open("w", encoding="utf-8") as f:
        json.dump(file, f, ensure_ascii=False, indent=2)

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
    tal.load_bulk_matches(normalized_matches, normalzed_players)
    logging.info("Data loaded into database")

def test_fetch_hero_trends()->pd.DataFrame:
    """fetches hero trends for 7 days, saves to csv data/test_data/hero_trends_7d.csv"""
    hero_df = fd.fetch_hero_data(min_unix_time=7, min_average_badge=0)
    assert isinstance(hero_df, pd.DataFrame), "Data should be a DataFrame"
    assert len(hero_df) == 26, f"Data should have 26 rows, has {len(hero_df)} rows"
    u.df_to_csv(hero_df, "data/test_data/hero_data")
    print(f"\n\nHero data fetched, hero_data = {hero_df.head()} hero_df len = {len(hero_df)}\n\n")
    return hero_df

def test_etl_hero_trends_single():
    print(f"\n\n***Starting Function Tests****\n\n")
    dbf.reset_all_tables(db.con)
    base_hero_trends = test_fetch_hero_trends()
    transformed_hero_trends = tal.transform_hero_trends(trend_range=7, hero_trends=base_hero_trends)
    assert isinstance(transformed_hero_trends, pd.DataFrame), "Transformed data should be a DataFrame"
    tal.load_hero_trends(transformed_hero_trends)    
    print(f"\n\n ***Function Tests Complete****\n\n")

def test_orchestrators():
    #dbf.reset_all_tables(db.con)
    o.run_etl_bulk_matches(max_days_fetch=2)
    #o.run_etl_hero_trends()

def test():
    logging.info("Starting player statistics ETL process")
    
    players_to_trend = dbf.test_pull_players_to_trend(con=db.con)
    logging.info(f"Found {len(players_to_trend)} players to process")
    batch_size = 3
    total_players_to_process = len(players_to_trend)

    for i in range(0, total_players_to_process, batch_size):
        batch_players = players_to_trend[i:i+batch_size]
        logging.info(f"Processing batch of {len(batch_players)} players from total {total_players_to_process}")
        logging.debug(f"Batch players: {batch_players}")
        batch_players_trended = []
        # Perform operations on the batch of players
        for _,player in batch_players.iterrows():
            account_id = player["account_id"]
            logging.debug(f"Processing player with account_id: {account_id}")
            # Fetch and process match data for the player
            try:
                player_match_history = fd.fetch_player_match_history(account_id)
                if not player_match_history.empty:
                    batch_players_trended.append(player_match_history)
                    logging.debug(f"Processed match history for player {account_id}")
            except Exception as e:
                logging.warning(f"Error processing player {account_id}, error: {e}")
    
        #calculate player, player_hero trends
        logging.info(f"Calculating player trends for {len(batch_players_trended)} of {len(players_to_trend)} players")
        
        all_player_stats = []
        for player_history in batch_players_trended:
            player_stats = tal.calcuate_player_base_stats(player_history)
            all_player_stats.append(player_stats)
        
        logging.debug(f"length of all_player_stats: {len(all_player_stats)}")
        df_player_stats = pd.DataFrame(all_player_stats)
        logging.debug(f"length of df_player_stats converted to df: {len(df_player_stats)}")
        logging.debug(f"Calculated player trends.\ndata type = {type(df_player_stats)} \nexample data:\n\n {all_player_stats[:2]}")
        u.any_to_csv(df_player_stats, "data/test_data/player_stats")
        
        
        #insert batch players into player_trends player_hero_trends tables
        #logging.info(f"Inserting {len(batch_players_trended)} player trends into database")
        #dbf.insert_player_trends(batch_players_trended)
        #logging.info(f"Inserted {len(batch_players_trended)} player trends into database")
        logging.info(f"*TEST*completed player_trends")
        
if __name__ == "__main__":  
    test()