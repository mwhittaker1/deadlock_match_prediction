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

def orchestrate_build_training_data(con, max_days=90, min_days=0, min_average_badge=100):
    """
    Fetch matches, match_players, 
    match_players_history, hero_trends
    splits and inserts into db @con
    """
    
    print(f"\n\n***Starting Build Training Data ****\n\n")
    
    orchestrate_hero_trends(reset=True) #builds and inserts hero_trend 7d,30d data.
    
    df_training_matches = fd.bulk_fetch_matches(max_days,min_days,min_average_badge)
    df_training_matches = prdt.normalize_match_json(df_training_matches)
    split_df = prdt.split_dfs_for_insertion(con, df_training_matches)
    match_df = split_df.get('match_columns')


    if match_df is not None or player_df is not None or trends_df is not None:
        prdt.insert_dataframes(con, match_df, player_df, trends_df)
    else:
        print("***tried to insert into db, but no valid dfs were identified***")
    
    # from database, get unqiue account_ids, check if in player_matches, and if not fetch history.
    df_training_matches = prdt.get_distinct_matches(con)
    df_training_matches = prdt.get_players_from_matches(df_training_matches)
    split_df = prdt.split_dfs_for_insertion(con, df_training_matches)
    player_df = split_df.get('player_columns')
    trends_df = split_df.get('trend_columns')

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
        dbf.create_hero_trends_table()

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

def safe_pull(function, *args, retry=3):
    for attempt in range(retry):
        try:
            return function(*args)
        except Exception as e:
            print(f"\n\n**ERROR** Failed attempt #: {retry}, failed: {e}")
            time.sleep(.5)
    print(F"\n\n***CRITICAL*** to many failed attempts.")
    return None

def old_orchestrate():
    hero_info = False
    p_stats = False #Returns player stats for p_id by collecting all match history. (can be 1000+ matches)
    player_match_history = False #Returns match history for p_id
    hero_trends = False # returns 7d, 30d all_hero trend data.
    match_d = False # returns x matches metadata over x prior days and minimum badge
    
    a_match_d = False #deprecated # returns 200 most recent high level matches
    player_hero_stat = False #deprecated # returns JSON p_all_hero data, or if h_id, p_hero_data
    
    p_id = "385814004"
    h_id = 1
    min_average_badge = "100"
    limit = "5"
    days = 10
    csv = True #exports data to csv
    xlsx = False #exports data to xlsx



    if a_match_d:
        print(f"*ERROR* Method deprecated")
        return
        # Active Match Data - Grabs 100 high ranking games currently being played.
        # Returns 2 dataframes, one with match data, another with account and hero ids for the players.
        #print(f"\n\n** Orchestrating active match data, to_csv = {csv} to xlsx = {xlsx} **")
        #active_match_data, a_m_player_data = old.orchestrate_active_match_data()
        #if csv:
            #print(f"\n\n** active_match_data, a_m_player_data to .csv **\n\n")
           # to_csv(active_match_data, "active_match_data")
           # to_csv(a_m_player_data, "a_m_player_data")
        #if xlsx:
           # print(f"\n\n** active_match_data, a_m_player_data to .xlsx **\n\n")
           # to_xlsx(active_match_data, "active_match_data")
            #to_xlsx(a_m_player_data, "a_m_player_data")

    if match_d:
        print(f"\n\n** Orchestrating match data, to_csv = {csv} to xlsx = {xlsx} **")
        match_data = orchestrate_match_data(limit, days, min_average_badge)
        if csv:
            print(f"\n\n** match_data, match_data to .csv **")
            to_csv(match_data, "match_data")
        if xlsx:
            print(f"\n\n** match_data, match_data to .xlsx **")
            to_xlsx(match_data, "match_data")

    if hero_trends:
        h_trends_7d, h_trends_30d = orchestrate_hero_trends()
        if csv:
            print(f"\n\n** h_trends 7d, 30d to .csv **")
            to_csv(h_trends_7d, "h_trends_7d")
            to_csv(h_trends_30d, "h_trends_30d")
        if xlsx:
            print(f"\n\n** h_trends 7d, 30d to .xlsx **")
            to_xlsx(h_trends_7d, "h_trends_7d")
            to_xlsx(h_trends_30d, "h_trends_30d")

    if player_match_history:
        p_m_history = orchestrate_p_m_history_stats(p_id)
        if csv:
            print(f"\n\n** p_m_history to .csv **")
            to_csv(p_m_history, "p_m_history")
        if xlsx:
            print(f"\n\n** p_m_history to .xlsx **")
            to_xlsx(p_m_history, "p_m_history")

    if player_hero_stat: #deprecated
        print(f"\n\n*** Fetching player_hero stats ***\nplayer id = {p_id} and hero id = {h_id}")
        p_h_stats = old_orchestrate_player_hero_stats(p_id, h_id)
        print(f"\n\n **** finished filtering out matches, p_h_stats is now: {p_h_stats}")
        if csv:
            print(f"\n\n**  p_h_data to .csv **")
            to_csv(p_h_stats, "p_h_stats")
        if xlsx:
            print(f"\n\n**  p_h_data to .xlsx **")
            to_xlsx(p_h_stats, "p_h_stats")
    
    if hero_info:
        heroes_info = orchestrate_hero_info()
        if csv:
            print(f"\n\n**  hero_info to .csv **")
            to_csv(heroes_info, "hero_info")
        if xlsx:
            print(f"\n\n**  hero_info to .xlsx **")
            to_xlsx(heroes_info, "hero_info")

    if p_stats:
        player_stats = orchestrate_player_stats(p_id)
        if csv:
            print(f"\n\n**  player_stats to .csv **")
            to_csv(player_stats, "player_stats")
        if xlsx:
            print(f"\n\n**  player_stats to .xlsx **")
            to_xlsx(player_stats, "heplayer_statso_info")

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

def old_orchestrate_player_hero_stats(p_id, h_id):
    """fetch player_hero stats and add additional calculations
    
    p_hero_win_per
    
    """
    
    p_hero_data = fd.fetch_player_hero_stats(p_id, h_id)
    p_hero_data = prdt.calculate_player_hero_stats(p_id, h_id)