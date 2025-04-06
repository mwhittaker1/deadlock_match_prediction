import requests
import pandas as pd
import numpy as np


#Fetches data and returns DF
def fetch_match_data():
    
    site = "https://api.deadlock-api.com"
    endpoint = "/v1/matches/active"
    url = site+endpoint

    response = requests.get(url)
    
    # Check if the request was successful
    if response.status_code == 200:
        active_matches_data = response.json()  # Converts the JSON response to a Python dictionary
    else:
        print(f"Failed to retrieve match data: {response.status_code}")
    
    df = pd.DataFrame(active_matches_data)
    return df

#filter by specific columns only
def filter_match_data(raw_active_matches_df):
        
    #columns to fetch
    match_filters = ["start_time", "match_id", "game_mode"]
    filtered_active_matches = []
    match_players = []
    
    for _, item in raw_active_matches_df.iterrows():
        
        if 'players' in item:
            #player becomes dict
            player = item["players"]
            
            #extract the player information
            for player in item["players"]:
                account_id = player.get('account_id', None) 
                hero_id = player.get('hero_id', None) 

                #append to new dict
                match_players.append({
                'match_id': item['match_id'],
                'account_id': account_id,
                'hero_id': hero_id
                })
            
        #if not player column,    
        filtered_item = {key: item[key] for key in match_filters if key in item}
        filtered_active_matches.append(filtered_item)


 
    return filtered_active_matches, match_players

#fetch
active_matches_df = fetch_match_data()
#filter
simple_match_df, match_player_df = filter_match_data(active_matches_df)