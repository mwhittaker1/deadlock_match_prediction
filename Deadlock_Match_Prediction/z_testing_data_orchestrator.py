import pandas as pd
from utility_functions import to_csv, to_xlsx, get_time_delta, setup_logging, initialize_logging
from dl_fetch_data import fetch_active_match_data, fetch_hero_data, fetch_match_data, fetch_player_hero_data
from dl_process_data import filter_account_data, filter_match_data, filter_player_hero_data, split_players_from_matches, calculate_hero_stats, calculate_player_hero_stats

#initialize logging
verbose=True
setup_logging(verbose)
initialize_logging(verbose)


#Fetches match data from DeadlockAPI, splits player data from match data, and filters based on MATCH_FILTERS from .cfg
def orchestrate_active_match_data():

    print(f"\n******fetching data**** \n")
    raw_match_data = fetch_active_match_data()
    print(f"\n\n** Active_match_Data found, raw_match_data.head :  {raw_match_data.head()}:\n\n") 

    print(f"\n******spliting data**** \n")
    match_data, account_data = split_players_from_matches(raw_match_data)
    print(f"\n\n** account, match data split. match_data: {match_data.head()} \n\naccount_data: {account_data.head()}:\n\n") 
 
    """print(f"\n******filtering match data**** \n")
    match_data = filter_match_data(match_data) #Filters by "MATCH_FILTERS", "PLAYER_FILTERS" from config.py.
    print(f"\n\n** filter_match data complete, match_data: {match_data.head()}   \n\n")

    print(f"\n******filtering account data**** \n")
    account_data = filter_account_data(account_data)
    print(f"\n\n** Filtered account data, account data: {account_data.head()}\n \n")"""

    print(f"\n******data to csv****")
    to_xlsx(match_data, "match")
    to_xlsx(account_data, "account")

def orchestrate_player_stats():
    #player specific stats, aka match history, w/l trends, hero diversity
    return

def orchestrate_player_hero_stats(p_id, h_id=None):
    if h_id is not None:
        p_hero_data =fetch_player_hero_data(p_id, h_id)
        return p_hero_data

    else:
        p_all_hero_data = fetch_player_hero_data(p_id, h_id)
        return p_all_hero_data
        

def orchestrate_match_data(days,min_average_badge):
    fetch_match_data(days, min_average_badge)

#Creates two data dfs, one for 7 day hero trends, one for 30 day hero trends.
def orchestrate_hero_data():
    min_average_badge = "min_average_badge=100"
    hero_trends_7d = fetch_hero_data(get_time_delta(7), min_average_badge)
    hero_trends_7d = calculate_hero_stats(hero_trends_7d)
    hero_trends_30d = fetch_hero_data(get_time_delta(30), min_average_badge)
    hero_trends_30d = calculate_hero_stats(hero_trends_30d)
    return hero_trends_30d, hero_trends_7d

def main():
    hero_d = False # returns 7d, 30d all_hero trend data.
    match_d = False # returns match metadata for x days and minimum badge
    a_match_d = False # returns 200 most recent high level matches
    player_d = False #returns p_id data
    player_hero_d = True # returns p_all_hero data, or if h_id, p_hero_data
    p_id = "385814004"
    h_id = "58"

    csv = True #exports data to csv
    xlsx = False #exports data to xlsx

    if a_match_d:
        print(f"\n\n** Orchestrating active match data, to_csv = {csv} to xlsx = {xlsx} **\n\n")
        active_match_data, a_m_player_data = orchestrate_active_match_data()
        if csv:
            print(f"\n\n** active_match_data, a_m_player_data to .csv **\n\n")
            to_csv(active_match_data, "active_match_data")
            to_csv(a_m_player_data, "a_m_player_data")
        if xlsx:
            print(f"\n\n** active_match_data, a_m_player_data to .xlsx **\n\n")
            to_xlsx(active_match_data, "active_match_data")
            to_xlsx(a_m_player_data, "a_m_player_data")

    if hero_d:
        h_trends_7d, h_trends_30d = orchestrate_hero_data()
        if csv:
            print(f"\n\n** h_trends 7d, 30d to .csv **\n\n")
            to_csv(h_trends_7d, "h_trends_7d")
            to_csv(h_trends_30d, "h_trends_30d")
        if xlsx:
            print(f"\n\n** h_trends 7d, 30d to .xlsx **\n\n")
            to_xlsx(h_trends_7d, "h_trends_7d")
            to_xlsx(h_trends_30d, "h_trends_30d")

    if player_hero_d:
        p_h_stats = orchestrate_player_hero_stats(p_id, h_id) #needs player_id, hero_id filters by that hero
        if csv:
            print(f"\n\n**  p_h_data to .csv **\n\n")
            to_csv(p_h_stats, "p_h_stats")
        if xlsx:
            print(f"\n\n**  p_h_data to .xlsx **\n\n")
            to_xlsx(p_h_stats, "p_h_stats")

main()
#if __name__ == "__main__":
#    main()