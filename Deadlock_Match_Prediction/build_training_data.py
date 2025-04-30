import pandas as pd
import duckdb
from data_orchestrator import orchestrate_hero_trends, orchestrate_match_data, orchestrate_match_players, orchestrate_fetch_training_data
from services.dl_process_data import insert_dataframes, split_dfs_for_insertion
from services.utility_functions import to_csv,to_xlsx

def build_training_data():
    """
    Fetch matches, match_players, 
    match_players_history, hero_trends
    splits and inserts into db @con
    """
    con = duckdb.connect("c:/Code/Local Code/Deadlock Database/Deadlock_Match_Prediction/deadlock.db")
    num_matches = 10
    days = 365
    min_badge = 100
    
    orchestrate_fetch_training_data(con, num_matches, days, min_badge)

    return

if __name__ == "__main__":
    build_training_data()