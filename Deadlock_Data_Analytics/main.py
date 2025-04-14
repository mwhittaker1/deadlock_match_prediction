import pandas as pd

from Deadlock_Data_Fetch import fetch_match_data
from Deadlock_Data_Filter import split_data,filter_match_data


#Fetches match data from DeadlockAPI, splits player data from match data, and filters based on MATCH_FILTERS from .cfg
def main():

    #Fetch Match Data
    raw_match_data = fetch_match_data()  #Returns JSON of match data.

    #Returns DF for Match Data and Account Data.
    match_data, account_data = split_data(raw_match_data)

    #Filters by "MATCH_FILTERS" from config.py. 
    match_data = filter_match_data(match_data)

main()