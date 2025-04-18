import pandas as pd

from Deadlock_Data_Fetch import fetch_active_match_data
from Deadlock_Data_Filter import split_data,filter_match_data, filter_account_data
from output_to_file import to_xlsx

#Configuration flags
print_status = False #prints to console different steps and debugging
toxlsx = True #enables saving to .xlsx when formatting is complete.

#Fetches match data from DeadlockAPI, splits player data from match data, and filters based on MATCH_FILTERS from .cfg
def main():

    #Fetch Match Data
    print(f"\n******fetching data**** \n")
    raw_match_data = fetch_active_match_data()  #Returns JSON of match data.

    #Returns DF for Match Data and Account Data.
    print(f"\n******spliting data**** \n")
    match_data, account_data = split_data(raw_match_data)

    #Filters by "MATCH_FILTERS", "PLAYER_FILTERS" from config.py. 
    print(f"\n******filtering match data**** \n")
    match_data = filter_match_data(match_data)
    
    print(f"\n******filtering account data**** \n")
    account_data = filter_account_data(account_data)

    print(f"\n******data to csv****")
    #print(f"\nmatch_data is type {type(match_data)} and account_data is type {type(account_data)}")
    #to_xlsx(match_data, "match")
    to_xlsx(account_data, "account")
    


if __name__ == main():
    main()

"""
    MATCH DATA
    start_time INTEGER,
    match_id INTEGER,
    game_mode INTEGER,
    PRIMARY KEY (match_id)
"""
#need to add winning_team int,  lobby_id str, duration_s long, match_mode int, region_mode_parsed str

"""match_player
    match_id INTEGER,
    account_id INTEGER,
    hero_id INTEGER,
    PRIMARY KEY (match_id, account_id),
    FOREIGN  KEY (account_id) REFERENCES account(account_id)
    FOREIGN  KEY (hero_id) REFERENCES heroes(heroes_id)
"""