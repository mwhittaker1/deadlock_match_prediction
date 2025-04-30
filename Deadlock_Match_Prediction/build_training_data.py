import pandas as pd
import duckdb
from data_orchestrator import orchestrate_build_training_data


def get_training_matches(bulk=False):
    """
    Fetch matches, match_players, 
    match_players_history, hero_trends
    splits and inserts into db @con
    """
    con = duckdb.connect("c:/Code/Local Code/Deadlock Database/Deadlock_Match_Prediction/deadlock.db")
    num_matches = 10
    days = 365
    min_badge = 100
    orchestrate_build_training_data(con, num_matches, days, min_badge)
    return

def get_training_match_players():
    pass

if __name__ == "__main__":
    get_training_matches()