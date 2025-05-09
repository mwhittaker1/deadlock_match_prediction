import pandas as pd
import duckdb
from data_orchestrator import orchestrate_hero_trends, orchestrate_match_player_histories, orchestrate_build_training_data
import inline.database_foundations as dbf

def build_trianing_data():
    """
    Fetch matches, match_players, 
    match_players_history, hero_trends
    splits and inserts into db @con
    """
    print(f"**INFO** Starting build_training_data")
    pd.set_option('display.max_columns',None)
    con = duckdb.connect("c:/Code/Local Code/Deadlock Database/Deadlock_Match_Prediction/deadlock.db")
    #dbf.reset_all_tables()
    orchestrate_hero_trends(reset=True) #builds and inserts hero_trend 7d,30d data.
    orchestrate_build_training_data(con, max_days_fetch=45 ) #Fetches matches and inserts into matches table
    orchestrate_match_player_histories(con) #Fetch unique account_id in matches tables, fetches player_match_histories for each, calculates stats.
    print(f"**INFO** DONE build_training_data")
    return

if __name__ == "__main__":
    build_trianing_data()