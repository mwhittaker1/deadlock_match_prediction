import pandas as pd
from utility_functions import to_csv, to_xlsx, get_time_delta, setup_logging, initialize_logging
from dl_fetch_data import fetch_active_match_data, fetch_hero_stats, fetch_match_data, fetch_player_hero_data
from dl_process_data import filter_account_data, filter_match_data, filter_player_hero_data, split_players_from_matches, calculate_hero_stats, calculate_player_hero_stats

#initialize logging
verbose=True
setup_logging(verbose)
initialize_logging(verbose)


#Fetches match data from DeadlockAPI, splits player data from match data, and filters based on MATCH_FILTERS from .cfg
def orchestrate_active_match_data():

    #Fetch Match Data
    print(f"\n******fetching data**** \n")
    raw_match_data = fetch_active_match_data()  #Returns JSON of match data.

    #Returns DF for Match Data and Account Data.
    print(f"\n******spliting data**** \n")
    match_data, account_data = split_players_from_matches(raw_match_data)

    #Filters by "MATCH_FILTERS", "PLAYER_FILTERS" from config.py. 
    print(f"\n******filtering match data**** \n")
    match_data = filter_match_data(match_data)
    
    print(f"\n******filtering account data**** \n")
    account_data = filter_account_data(account_data)

    print(f"\n******data to csv****")
    #print(f"\nmatch_data is type {type(match_data)} and account_data is type {type(account_data)}")
    to_xlsx(match_data, "match")
    to_xlsx(account_data, "account")

def orchestrate_player_stats():
    #player specific stats, aka match history, w/l trends, hero diversity
    return

def orchestrate_player_hero_stats():
    # player_hero stats
    return

#Creates two data dfs, one for 7 day hero trends, one for 30 day hero trends.
def orchestrate_hero_data():
    min_average_badge = "min_average_badge=100"
    hero_trends_7d = fetch_hero_stats(get_time_delta(7), min_average_badge)
    hero_trends_7d = calculate_hero_stats(hero_trends_7d)
    hero_trends_30d = fetch_hero_stats(get_time_delta(30), min_average_badge)
    hero_trends_30d = calculate_hero_stats(hero_trends_30d)

def main():
    return

if __name__ == main():
    main()