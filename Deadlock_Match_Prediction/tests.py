import pandas as pd
import duckdb
import time
import json
import services.dl_process_data as prdt
import services.dl_fetch_data as fd
from services.utility_functions import to_csv, to_xlsx, get_time_delta, setup_logging, initialize_logging
from data_orchestrator import orchestrate_hero_trends, orchestrate_match_data

def test_insert_data(all_matches_players_history):
    con = duckdb.connect("c:/Code/Local Code/Deadlock Database/Deadlock_Match_Prediction/deadlock.db")
    
    
    print(f"\n\n***Splitting and inserting to database.***\n\n")
    match_df, player_df, trends_df = prdt.split_dfs_for_insertion(con, all_matches_players_history)
    prdt.insert_dataframes(con, match_df, player_df, trends_df)

    print(f"\n\n***getting 7d and 30d hero trends***")
    hero_trends_df_7, hero_trends_df_30 = orchestrate_hero_trends()

    prdt.insert_dataframes(con, hero_trends_df=hero_trends_df_30)
    prdt.insert_dataframes(con, hero_trends_df=hero_trends_df_7)

    return

def fetch_matches():
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
    

    print(f"\n\n\n ***COMPLETED****")

if __name__ == "__main__":
    #fetch_matches()
    #noramlize_matches()
