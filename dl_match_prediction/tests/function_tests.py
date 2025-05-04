import os
import sys

# Add the parent directory to sys.path
# This line should be at the top before any other imports
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parent_dir)

print(f"Current directory: {os.path.abspath(__file__)}")
print(f"Parent directory added to path: {parent_dir}")
print(f"Current sys.path: {sys.path}")

import requests
import json
import pandas as pd
import logging
import duckdb
from pathlib import Path
from services import function_tools as u
from services import database_functions as dbf
from services import fetch_data as fd
from services import transform_and_load as tal


u.setup_logger()
logging.info("Logger initialized.")

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

def test_bulk_fetch_matches():

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

def test_etl_bulk_matches():
    
    p = Path(__file__).parent / "dl_match_prediction" / "tests" / "match_response.json"
    if not p.exists():
        logging.critical(f"Fixture file {p} not found. Exiting test.")
        return
    
    # Fetch data
    print(f"*INFO* ETL: Fetching match data")
    matches_grouped_by_day = match_fixture(p)
    print(f"*INFO* ETL: Data fetched")
    
    # Normalize data
    print(f"*INFO* ETL: Normalizing data")
    normalized_data = tal.normalize_bulk_matches(matches_grouped_by_day)
    print(f"*INFO* ETL: Data normalized")
    u.df_to_csv(normalized_data, "normalized_matches")
    # Load data into database
    #print(f"*INFO* ETL: Loading data into database")
    #dbf.load_bulk_matches(normalized_data)
    #print(f"*INFO* ETL: Data loaded into database")

def run_tests():
    print(f"\n\n***Starting Function Tests****\n\n")
    test_etl_bulk_matches()
    pass

if __name__ == "__main__":  
    run_tests()