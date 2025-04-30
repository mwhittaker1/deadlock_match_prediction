import pandas as pd
import duckdb
import time
import json
import pyarrow.parquet as pq
import services.dl_process_data as prdt
import services.dl_fetch_data as fd
from services.utility_functions import to_csv, to_xlsx, get_time_delta, setup_logging, initialize_logging
from data_orchestrator import orchestrate_hero_trends

def test_insert_data(all_matches_players_history):
    """inserts data to df, needs match_df, player_df, trends_df, and/or her_trends_df"""
    con = duckdb.connect("c:/Code/Local Code/Deadlock Database/Deadlock_Match_Prediction/deadlock.db")
    
    print(f"\n\n***Splitting and inserting to database.***\n\n")
    match_df, player_df, trends_df = prdt.split_dfs_for_insertion(con, all_matches_players_history)
    prdt.insert_dataframes(con, match_df, player_df, trends_df)

    print(f"\n\n***getting 7d and 30d hero trends***")
    hero_trends_df_7, hero_trends_df_30 = orchestrate_hero_trends()

    prdt.insert_dataframes(con, hero_trends_df=hero_trends_df_30)
    prdt.insert_dataframes(con, hero_trends_df=hero_trends_df_7)

    return

def bulk_fetch_matches(limit=500,max_days_fetch=90):
    """fetches a batch of matches, 1 day per pull, exports to json.

    batch is unnormalized, 'players' contains a df of each matches 'players'
    limit = max matches within a day to pull
    max_days_fetch = how many days to cycle through for total fetch"""

    print(f"start")
    con = duckdb.connect("c:/Code/Local Code/Deadlock Database/Deadlock_Match_Prediction/deadlock.db")
    limit = 5000
    max_days_fetch = 90
    batch_matches = []
    max_days = 1
    min_days = 2
    i=0
    
    for batch in range(max_days_fetch):
        print(f"\n*day:{i} min ={min_days} max = {max_days}")
        fetched_matches = fd.fetch_match_data(limit, min_days, max_days)
        batch_matches.extend(fetched_matches)
        max_days +=1
        min_days +=1
        i+=1
    with open("non_normal_total_matches.json", "w") as f:
        json.dump(batch_matches, f)
    print(f"\n\nfin\n\n")

def noramlize_matches():
    with open("non_normal_total_matches.json", "r") as f:
        total_matches = json.load(f)
    norm_matches = prdt.normalize_match_json(total_matches)
    to_csv(norm_matches,"norm_matches")

def run_tests():
    print("Starting!")
    parquet_file = pq.ParquetFile("C:/Code/Local Code/Deadlock Database/Deadlock_Match_Prediction/test_data/match_player_35.parquet")
    print(f"number of rows:{parquet_file.metadata.num_rows}\nnum columns:{parquet_file.metadata.num_columns}")

    print(f"\n\n\n ***COMPLETED****")

if __name__ == "__main__":
    run_tests()
