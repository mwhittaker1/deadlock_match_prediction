import pandas as pd
from services.utility_functions import to_csv, to_xlsx, get_time_delta, setup_logging, initialize_logging
from services.dl_fetch_data import fetch_active_match_data, fetch_hero_data, fetch_match_data, fetch_player_hero_stats, fetch_hero_info, fetch_player_match_history
from services.dl_process_data import filter_account_data, filter_match_data, filter_player_hero_data, split_players_from_matches, calculate_hero_stats, calculate_player_hero_stats, match_data_outcome_add, match_history_outcome_add, win_loss_history

#initialize logging
verbose=False
setup_logging(verbose)
initialize_logging(verbose)


#Fetches match data from DeadlockAPI, splits player data from match data, and filters based on MATCH_FILTERS from .cfg
def orchestrate_active_match_data():

    print(f"\n******fetching data**** \n")
    raw_match_data = fetch_active_match_data()

    print(f"\n******spliting data**** \n")
    match_data, account_data = split_players_from_matches(raw_match_data)
 
    print(f"\n******data to csv****")
    to_xlsx(match_data, "match")
    to_xlsx(account_data, "account")

# fetch base hero information, aka name, description, etc.
def orchestrate_hero_info():
    hero_info = fetch_hero_info()
    return hero_info

#not currently pulling flat player stats.
def orchestrate_player_stats():
    #player specific stats, aka match history, w/l trends, hero diversity
    return

# fetch player_hero stats, if h_id is blank, returns stats for all heroes for the player.
def orchestrate_player_hero_stats(p_id, h_id=None):
    print(f"\n\n*** In orchestrate_player_hero_stats, h_id = {h_id} ***")
    if h_id:
        p_hero_data =fetch_player_hero_stats(p_id, h_id)
        return p_hero_data
    else:
        p_hero_data = fetch_player_hero_stats(p_id)
        return p_hero_data
    
    p_hero_data = calculate_player_hero_stats
    
# retreive all match history for p_id
def orchestrate_match_history(df):
    single_match_players_history = pd.DataFrame()
    player_count = 0
    #print(f"\n**pulling account_id from data m_id = {match['match_id']}**\n")
    for current_p_id in df['account_id']:
        player_count +=1
        p_m_history = fetch_player_match_history(current_p_id)
        p_m_history = match_history_outcome_add(p_m_history)
        p_m_history = win_loss_history(p_m_history)
        single_match_players_history = pd.concat([single_match_players_history,p_m_history],ignore_index=True)
    return single_match_players_history

#Creates two data dfs, one for 7 day hero trends, one for 30 day hero trends.
def orchestrate_hero_data():
    min_average_badge = "min_average_badge=100"
    hero_trends_7d = fetch_hero_data(get_time_delta(7), min_average_badge)
    hero_trends_7d = calculate_hero_stats(hero_trends_7d)
    hero_trends_30d = fetch_hero_data(get_time_delta(30), min_average_badge)
    hero_trends_30d = calculate_hero_stats(hero_trends_30d)
    return hero_trends_7d, hero_trends_30d

# Fetches match data over x days, miniumum rank, and max to fetch (hard limit 5000)
def orchestrate_match_data(days,min_average_badge,m_id=None):
    limit = 1
    df_m_data, json_match_data = fetch_match_data(limit,days, min_average_badge,m_id)
    print(f"\n\n** Data split, applying match outcome data ***\n")
    flat_m_data = match_data_outcome_add(df_m_data, json_match_data) #returns df
    print(f"\n** data flattened + stats, data =\n {flat_m_data}")
    return flat_m_data #df

# takes 1 match_id, fetches player data, fetches player_hero data for each, and returns all.
def orchestrate_match_hero_data(limit,days,min_average_badge):
    limit = 1
    single_match = orchestrate_match_data(limit,days,min_average_badge)
    m_id = single_match['match_id'][0]
    match_data = orchestrate_match_data(limit,days,min_average_badge,m_id)
    for row in match_data:
        current_p_id = row["account_id"]

    print(f"\n\n *** from orchestrate_match_hero_data. m_id = {m_id}***")
    return

def main():
    hero_info = False
    hero_d = False # returns 7d, 30d all_hero trend data.
    match_d = False # returns match metadata for x days and minimum badge
    a_match_d = False # returns 200 most recent high level matches
    player_match_history = False #Returns match history for p_id
    player_hero_stat = False # returns JSON p_all_hero data, or if h_id, p_hero_data
    p_id = "385814004"
    h_id = 1
    min_average_badge = "100"
    limit = "5"
    days = 10
    csv = False #exports data to csv
    xlsx = False #exports data to xlsx

    # Active Match Data - Grabs 100 high ranking games currently being played.
    # Returns 2 dataframes, one with match data, another with account and hero ids for the players.
    if a_match_d:
        print(f"\n\n** Orchestrating active match data, to_csv = {csv} to xlsx = {xlsx} **")
        active_match_data, a_m_player_data = orchestrate_active_match_data()
        if csv:
            print(f"\n\n** active_match_data, a_m_player_data to .csv **\n\n")
            to_csv(active_match_data, "active_match_data")
            to_csv(a_m_player_data, "a_m_player_data")
        if xlsx:
            print(f"\n\n** active_match_data, a_m_player_data to .xlsx **\n\n")
            to_xlsx(active_match_data, "active_match_data")
            to_xlsx(a_m_player_data, "a_m_player_data")

    if match_d:
        print(f"\n\n** Orchestrating match data, to_csv = {csv} to xlsx = {xlsx} **")
        match_data = orchestrate_match_data(limit, days, min_average_badge)
        if csv:
            print(f"\n\n** match_data, match_data to .csv **")
            to_csv(match_data, "match_data")
        if xlsx:
            print(f"\n\n** match_data, match_data to .xlsx **")
            to_xlsx(match_data, "match_data")

    if hero_d:
        h_trends_7d, h_trends_30d = orchestrate_hero_data()
        if csv:
            print(f"\n\n** h_trends 7d, 30d to .csv **")
            to_csv(h_trends_7d, "h_trends_7d")
            to_csv(h_trends_30d, "h_trends_30d")
        if xlsx:
            print(f"\n\n** h_trends 7d, 30d to .xlsx **")
            to_xlsx(h_trends_7d, "h_trends_7d")
            to_xlsx(h_trends_30d, "h_trends_30d")

    if player_match_history:
        p_m_history = orchestrate_match_history(p_id)
        if csv:
            print(f"\n\n** p_m_history to .csv **")
            to_csv(p_m_history, "p_m_history")
        if xlsx:
            print(f"\n\n** p_m_history to .xlsx **")
            to_xlsx(p_m_history, "p_m_history")

    if player_hero_stat:
        print(f"\n\n*** Fetching player_hero stats ***\nplayer id = {p_id} and hero id = {h_id}")
        p_h_stats = orchestrate_player_hero_stats(p_id, h_id)
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


#main()
#if __name__ == "__main__":
#    main()