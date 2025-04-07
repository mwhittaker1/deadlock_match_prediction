import requests
import pandas as pd
import numpy as np

#filters hero data using config/HERO_FILTERS
def filter_hero_data(df, filters):
    #columns to fetch
    

    filtered_data = []
    for _, item in df.iterrows():
        filtered_item = {key: item[key] for key in filters if key in item}
        filtered_data.append(filtered_item)
    
    return pd.DataFrame(filtered_data)

#filters match data using config/MATCH_FILTERS
def filter_match_data(df, filters):
        
    #columns to fetch
    filtered_active_matches = []
    match_players = []
    
    for _, item in df.iterrows():
        
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
