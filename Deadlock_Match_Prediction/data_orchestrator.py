import pandas as pd
import duckdb
import time
import services.dl_process_data as prdt
import services.dl_fetch_data as fd
import inline.database_foundations as dbf
from services.utility_functions import to_csv, to_xlsx, get_time_delta, setup_logging, initialize_logging

#initialize logging
verbose=False
setup_logging(verbose)
initialize_logging(verbose)

def orchestrate_build_training_data(con, max_days_fetch=90):
    """
    Fetch matches, normalizes, and inserts into matches_table
    """
    
    print(f"\n\n***Starting Build Training Data ****\n\n")
    
    df_training_matches = fd.bulk_fetch_matches(max_days_fetch)
    df_training_matches = prdt.normalize_match_json(df_training_matches)
    split_df = prdt.split_dfs_for_insertion(con, df_training_matches)
    match_df = split_df.get('match_columns')
    player_df = split_df.get('player_columns')
    trends_df = split_df.get('trend_columns')

    if match_df is not None or player_df is not None or trends_df is not None:
        prdt.insert_dataframes(con, match_df, player_df, trends_df)
    else:
        print("***tried to insert into db, but no valid dfs were identified***")

def orchestrate_match_player_histories(con):
    """
    from matches table, get unqiue account_ids, and if not fetch history.
    Does not check for existing data in match_players before fetching.
    """
    
    df_training_matches = prdt.get_distinct_matches(con)
    #print(f"df training matches: {df_training_matches}")
    
    prdt.batch_get_players_from_matches(con, df_training_matches, batch_size=500)

    #split_df = prdt.split_dfs_for_insertion(con, df_training_matches)
    #player_df = split_df.get('player_columns')
    #trends_df = split_df.get('trend_columns')
    #match_df = split_df.get('match_columns')

    #if match_df is not None or player_df is not None or trends_df is not None:
    #    prdt.insert_dataframes(con, match_df, player_df, trends_df)
    #else:
    #    print("***tried to insert into db, but no valid dfs were identified***")

def orchestrate_match_data(limit, min_average_badge=100, days=365,m_id=None)->pd.DataFrame:
    """Fetches match data over x days, miniumum rank, and max to fetch (hard limit 5000) """
    fetched_matches = fd.fetch_match_data(limit, days, min_average_badge)
    fetched_matches = prdt.normalize_match_json(fetched_matches)
    #flat_m_data = prdt.match_data_outcome_add(fetched_matches)
    print(f"\n**matches formatted!")
    return fetched_matches #df

def orchestrate_hero_trends(reset=True):
    """Creates two data dfs, one for 7 day hero trends, one for 30 day hero trends."""
    if reset:
        con = duckdb.connect("c:/Code/Local Code/Deadlock Database/Deadlock_Match_Prediction/deadlock.db")
        con.execute("DROP TABLE IF EXISTS hero_trends")
        dbf.create_hero_trends_table(con)

    current_time = get_time_delta(0,short=True)
    seven_days = get_time_delta(7,short=True)
    thirty_days = get_time_delta(3,short=True)
    min_average_badge = "min_average_badge=100"

    hero_trends_7d = fd.fetch_hero_data(seven_days, min_average_badge)
    hero_trends_7d = prdt.calculate_hero_stats(hero_trends_7d)
    hero_trends_7d = hero_trends_7d.assign(
        trend_start_date=current_time,
        trend_end_date=seven_days,
        trend_date = current_time,
        trend_window_days="7"
    )

    hero_trends_30d = fd.fetch_hero_data(thirty_days, min_average_badge)
    hero_trends_30d = prdt.calculate_hero_stats(hero_trends_30d)
    hero_trends_30d = hero_trends_30d.assign(
        trend_start_date=current_time,
        trend_end_date=thirty_days,
        trend_date = current_time,
        trend_window_days="30"
    )
    
    def filter_hero_trends(df)->pd.DataFrame:
        df_filters = [
        'hero_id','trend_start_date','trend_end_date','trend_date','trend_window_days',
        'pick_rate','win_rate','average_kills','average_deaths','average_assists'
        ]
        filtered_df = df[df_filters].copy()
        filtered_df['trend_start_date'] = pd.to_datetime(filtered_df['trend_start_date'], unit='s')
        filtered_df['trend_end_date'] = pd.to_datetime(filtered_df['trend_end_date'], unit='s')
        filtered_df['trend_date'] = pd.to_datetime(filtered_df['trend_date'], unit='s')
        return filtered_df
    
    hero_trends_30d = filter_hero_trends(hero_trends_30d)
    hero_trends_7d = filter_hero_trends(hero_trends_7d)
    prdt.insert_dataframes(con, hero_trends_df=hero_trends_30d)
    prdt.insert_dataframes(con, hero_trends_df=hero_trends_7d)
    
    return hero_trends_7d, hero_trends_30d

def orchestrate_hero_info():
    """fetch base hero information, aka name, description, etc."""
    hero_info = fd.fetch_hero_info()
    return hero_info

if __name__ == "__main__":
    con = duckdb.connect("c:/Code/Local Code/Deadlock Database/Deadlock_Match_Prediction/deadlock.db")
    num_matches = 10
    days = 10
    #args = [con, num_matches, days]
    #response = orchestrate_fetch_training_data(con, num_matches, days)
    #orchestrate_build_training_data(con,num_matches,days)
    #print("done")
    #to_csv(response, "large_match_pull")
    #df_training_matches = pd.read_csv("large_match_pull.csv")
    #all_matches_players_history = prdt.get_all_histories(df_training_matches)
    #to_csv(all_matches_players_history, "large_player_history_pull")