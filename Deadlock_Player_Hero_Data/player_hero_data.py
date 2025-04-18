import pandas as pd
import requests
import logging
import time
import os
from xlsx_output import to_xlsx
from datetime import timedelta
from Deadlock_Active_Matches.Deadlock_Data_Fetch import fetch_active_match_data
from match_hero_data import get_hero_trends


## Fetch player data from player_id
def get_player_data(p_id):
    # fetch data from API on player_id
    site = "https://api.deadlock-api.com"
    endpoint = f"v1/players/{p_id}/hero-stats'"
    url = site+endpoint
    
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        match_data = pd.DataFrame(response.json())
        #match_data = response.json()
    else:
        print(f"Failed to retrieve match data: {response.status_code}")
    
    return match_data #Returns JSON of match data.
    return

def main():
    test_data = pd.read_csv(f'test_account_data.xlsx')

## p_hero total games played as p_hero_total_games (int)

## p_hero w/l over last 1 months as p_hero_1m_wl (int)

## p_hero w/l over last week as p_hero_1w_wl (int)

## player w/l over last 3 games as player_3g_wl (int)

## player w/l over last month as player_1m_wl (int)

def format_player_data(p_id):
    # formats player data received
    return

## Collect list of players from match, collect player+hero variable
def unpackage_players():
    # for each player
    # get_player_data
    # format_player_data
    # do things
    return


# Perform analytics on each player+hero combination

#                         Player Stats                #
## Player w/l over last 3 games (Short term tilt factor)
## Player w/l over past month (Bias for winning?)

#                        Player_Hero Stats             #
## p_hero total games as hero (experience modifier)

## p_hero w/l over past 30 days (Player skill)

## p_hero hero recency frequency (Player may be rusty with hero)



#Rebuild dataframe.


#Collect match data
#[players],"start_time", "winning_team", "match_id", "lobby_id", "duration_s","match_mode", "game_mode","region_mode_parsed" 

#Collect match players
## Expecting DataFrame of 12 account_id, hero_id pairs as pd.DataFrame[match_players]

## Sort match players, 0-5 = winning team, 6-12 = losing team

#for each player Fetch p_hero stats (dict)
### See Player/account_id/hero-stats


###Collect as dict{match_data: {m_data}, team_win: [[w_team]], team_lose: [[l_team]] } 

#Send data to AI for analytics

#Return prediction