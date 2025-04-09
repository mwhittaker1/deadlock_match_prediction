import requests
import pandas as pd
import numpy as np
import json

from config import MATCH_FILTERS, HERO_FILTERS

def filter_data(df):
    result = {}
    if "hero" in df:
        filtered_data = []
        for _, item in df.iterrows():
            filtered_item = {key: item[key] for key in HERO_FILTERS if key in item}
            filtered_data.append(filtered_item)
        
        result["hero"] = filtered_data

    #splits match DF into a match_players dict and matches dic
    if "match" in df:
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
            filtered_item = {key: item[key] for key in MATCH_FILTERS if key in item}
            filtered_active_matches.append(filtered_item)
        result['matches'] = filtered_active_matches
        result['match_players'] = match_players

    else:
        print("Filter Data failed, expected 'hero' or 'match'")
    
    return result