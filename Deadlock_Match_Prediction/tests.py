import pandas as pd
import duckdb
import time
import json
import pyarrow.parquet as pq
import inline.database_foundations as dbf
import services.dl_process_data as prdt
import services.dl_fetch_data as fd
from services.utility_functions import to_csv, to_xlsx, get_time_delta, setup_logging, initialize_logging
from data_orchestrator import orchestrate_hero_trends,orchestrate_match_data

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

def test_noramlize_matches(df):
    #with open("non_normal_total_matches.json", "r") as f:
        #total_matches = json.load(f)
    df = prdt.normalize_match_json(df)
    #to_csv(df,"norm_matches")

def test_orchestrate_match_data():
    num_matches = 5000
    df_training_matches = orchestrate_match_data(num_matches)
    
    print(df_training_matches.head())
    return df_training_matches

def test_insert_into_db():
    import inline.database_foundations as dbf
    dbf.reset_all_tables()
    con = duckdb.connect("c:/Code/Local Code/Deadlock Database/Deadlock_Match_Prediction/deadlock.db")
    df_training_matches = test_orchestrate_match_data()
    split_df = prdt.split_dfs_for_insertion(con, df_training_matches)
    match_df = split_df.get('match_columns')
    player_df = split_df.get('player_columns')
    trends_df = split_df.get('trend_columns')
    if match_df is not None or player_df is not None or trends_df is not None:
        prdt.insert_dataframes(con, match_df, player_df, trends_df)
    else:
        print("***tried to insert into db, but no valid dfs were identified***")

def test_insert_hero_data():
    con = duckdb.connect("c:/Code/Local Code/Deadlock Database/Deadlock_Match_Prediction/deadlock.db")
    hero_trends_df_7, hero_trends_df_30 = orchestrate_hero_trends()

    prdt.insert_dataframes(con, hero_trends_df=hero_trends_df_30)
    prdt.insert_dataframes(con, hero_trends_df=hero_trends_df_7)

def test_orchestrate_matches_players_stats(df):
    
#    def get_all_m_p_history(df)->pd.DataFrame:
#        """for all matches, get players_match_histories and stats"""

#        for _,match_row in df.iterrows():
#            """pulls match history of all players in match"""
#            single_matches_players_history = single_p_m_history_stats(match_row)
#            all_matches_players_history = pd.concat([all_matches_players_history, single_matches_players_history], ignore_index=True)
        
#        all_matches_players_history['start_time'] = pd.to_datetime(all_matches_players_history['start_time'], unit='s')
#        return all_matches_players_history
    pass

def test_single_p_m_history_stats(match_data)->pd.DataFrame:
        """
        creates statistics for player based on match history
        """
        single_matches_players_history = pd.DataFrame()
        player_count = 0
        for current_p_id in match_data['account_id']:
            player_count +=1
            p_m_history = test_get_p_m_history_stats(current_p_id)
            single_matches_players_history = pd.concat([single_matches_players_history,p_m_history],ignore_index=True)
        
        return single_matches_players_history

def test_get_p_m_history_stats(p_id):
        """for player, calculate player stats."""
        p_m_history = fd.fetch_player_match_history(p_id)
        p_m_history = prdt.match_history_outcome_add(p_m_history)
        p_m_history = prdt.win_loss_history(p_m_history)
        p_m_history = prdt.calculate_player_stats(p_m_history)
        return p_m_history

def run_tests():
    print("Starting!")
    df = pd.read_csv("match_df.csv")

    print(len(df))
    return

if __name__ == "__main__":
    run_tests()
