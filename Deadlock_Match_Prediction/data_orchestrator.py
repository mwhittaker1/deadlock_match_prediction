import pandas as pd
import duckdb
import time
import services.dl_process_data as prdt
import services.dl_fetch_data as fd
from services.utility_functions import to_csv, to_xlsx, get_time_delta, setup_logging, initialize_logging

#initialize logging
verbose=False
setup_logging(verbose)
initialize_logging(verbose)

def orchestrate_build_training_data(con, num_matches, days, min_average_badge=100):
    """
    Fetch matches, match_players, 
    match_players_history, hero_trends
    splits and inserts into db @con
    """
    
    print(f"\n\n***Starting Build Training Data ****\n\n")
    #match_data_args = [num_matches,days,min_average_badge]
    #df_training_matches = safe_pull(orchestrate_match_data,*match_data_args) #limit, max days, min badge
    df_training_matches = orchestrate_match_data(num_matches,days,min_average_badge)
    prdt.insert_dataframes(df_training_matches)
    ###   
    # Get unique account_id from entire matches list.
    # for each account id, get match history
    ###

    print(f"\n***getting matches histories data***\n")
    all_matches_players_history = prdt.get_all_histories(df_training_matches)
    #raw_matches_players_history = safe_pull(orchestrate_match_players,df_training_matches)
    #to_xlsx(training_data,"raw_matches_players_history")
    to_csv(all_matches_players_history,"all_matches_players_history")

    print(f"\n\n***Splitting and inserting to database.***\n\n")
    split_dfs = prdt.split_dfs_for_insertion(con, all_matches_players_history)
    match_df = split_dfs.get['match_df']
    player_df = split_dfs.get['player_df']
    trends_df = split_dfs.get['trends_df']
    if match_df is not None or player_df is not None or trends_df is not None:
        prdt.insert_dataframes(con, match_df, player_df, trends_df)
    else:
        print("***tried to insert into db, but no valid dfs were identified***")

    print(f"\n\n***getting 7d and 30d hero trends***")
    hero_trends_df_7, hero_trends_df_30 = orchestrate_hero_trends()

    prdt.insert_dataframes(con, hero_trends_df=hero_trends_df_30)
    prdt.insert_dataframes(con, hero_trends_df=hero_trends_df_7)

    return

def orchestrate_match_data(limit, min_average_badge, days=365,m_id=None)->pd.DataFrame:
    """Fetches match data over x days, miniumum rank, and max to fetch (hard limit 5000) """
    fetched_matches = fd.fetch_match_data(limit, days, min_average_badge, m_id)
    fetched_matches = prdt.normalize_match_json(fetched_matches)
    #flat_m_data = prdt.match_data_outcome_add(fetched_matches)
    print(f"\n**matches formatted!")
    return fetched_matches #df

def orchestrate_matches_players_stats(df):
    """
    For all matches in df, get all players match history and stats
    
    for each match in df:
        for each player in match:
            get player match history
            calculate player stats
    """
    def get_all_m_p_history(df)->pd.DataFrame:
        """for all matches, get players_match_histories and stats"""

        for _,match_row in df.iterrows():
            """pulls match history of all players in match"""
            single_matches_players_history = single_p_m_history_stats(match_row)
            all_matches_players_history = pd.concat([all_matches_players_history, single_matches_players_history], ignore_index=True)
        
        all_matches_players_history['start_time'] = pd.to_datetime(all_matches_players_history['start_time'], unit='s')
        return all_matches_players_history

    def single_p_m_history_stats(match_data)->pd.DataFrame:
        """
        creates statistics for player based on match history
        """

        player_count = 0
        #print(f"\n**pulling account_id from data m_id = {match['match_id']}**\n")
        
        for current_p_id in match_data['account_id']:
            player_count +=1
            p_m_history = get_p_m_history_stats(current_p_id)
            single_matches_players_history = pd.concat([single_matches_players_history,p_m_history],ignore_index=True)
        
        return single_matches_players_history

    def get_p_m_history_stats(p_id):
        """for player, calculate player stats."""
        p_m_history = fd.fetch_player_match_history(p_id)
        p_m_history = prdt.match_history_outcome_add(p_m_history)
        p_m_history = prdt.win_loss_history(p_m_history)
        p_m_history = prdt.calculate_player_stats(p_m_history)
        return p_m_history

    df = get_all_m_p_history(df)
    
    return df

def orchestrate_hero_trends():
    """Creates two data dfs, one for 7 day hero trends, one for 30 day hero trends."""

    current_time = get_time_delta(0,True)
    seven_days = get_time_delta(7,True)
    thirty_days = get_time_delta(3,True)
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

def orchestrate():
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
    #con = duckdb.connect("c:/Code/Local Code/Deadlock Database/Deadlock_Match_Prediction/deadlock.db")
    #num_matches = 2
    #days = 10
    #args = [con, num_matches, days]
    #response = orchestrate_fetch_training_data(con, num_matches, days)
    response = orchestrate_match_data(4000,100,30)
    to_csv(response, "large_match_pull")
    #df_training_matches = pd.read_csv("large_match_pull.csv")
    #all_matches_players_history = prdt.get_all_histories(df_training_matches)
    #to_csv(all_matches_players_history, "large_player_history_pull")

def old_orchestrate_player_hero_stats(p_id, h_id):
    """fetch player_hero stats and add additional calculations
    
    p_hero_win_per
    
    """
    
    p_hero_data = fd.fetch_player_hero_stats(p_id, h_id)
    p_hero_data = prdt.calculate_player_hero_stats(p_id, h_id)