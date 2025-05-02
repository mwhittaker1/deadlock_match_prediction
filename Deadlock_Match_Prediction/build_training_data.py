import pandas as pd
import duckdb
from data_orchestrator import orchestrate_hero_trends, orchestrate_match_player_histories, orchestrate_build_training_data


def build_trianing_data():
    """
    Fetch matches, match_players, 
    match_players_history, hero_trends
    splits and inserts into db @con
    """
    print(f"**INFO** Starting build_training_data")
    con = duckdb.connect("c:/Code/Local Code/Deadlock Database/Deadlock_Match_Prediction/deadlock.db")
    num_matches = 5000
    days = 3
    min_badge = 100
    orchestrate_hero_trends(reset=True) #builds and inserts hero_trend 7d,30d data.
    orchestrate_build_training_data(con, num_matches, days, min_badge) #Fetches matches and inserts into matches table
    orchestrate_match_player_histories(con) #Fetch unique account_id in matches tables, fetches player_match_histories for each, calculates stats.
    print(f"**INFO** DONE build_training_data")
    return

if __name__ == "__main__":
    build_trianing_data()