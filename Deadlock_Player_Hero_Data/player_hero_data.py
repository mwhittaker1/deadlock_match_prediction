import pandas as pd
import requests
import logging
import openpyxl
from file_output import to_csv

def setup_logging(verbose: bool):
    level = logging.DEBUG if verbose else logging.WARNING

    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler("script.log"),
            logging.StreamHandler()
        ]
    )
    logging.debug("This is a debug message")
    logging.info("This is an info message")
    logging.warning("This is a warning")
    logging.error("This is an error")
    logging.critical("This is critical")

setup_logging(verbose=True)
logger = logging.getLogger(__name__)
logger.debug("Debug mode on")

## Fetch player data from account_id, collects all hero data for player, filters for h_id, then returns player_hero data for h_id hero.
def get_player_hero_data(p_id,h_id):

    # fetch player_hero from API on player_id
    site = "https://api.deadlock-api.com"
    endpoint = f"/v1/players/{p_id}/hero-stats"
    url = site+endpoint
    
    logging.info(f"sending request for response")
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        p_all_h_data = response.json() #player all hero data
        print(f" found player data! player: {p_id}")    
    else:
        print(f"Failed to retrieve match data: {response.status_code}")
    
    p_h_data = next((item for item in p_all_h_data if item['hero_id'] == h_id), None)
    
    if p_all_h_data:
        logging.info(f"\n\nfound player_hero data for hero: {h_id} player: {h_id}")
        to_csv(p_h_data, f"{h_id}_{p_id}_data")
    else:
        logging.error(f"get_player_hero_data, find h_id in player data did not find match")

    return p_h_data #.json of h_id for p_id

# for each account_id, hero_id in df, pull player_hero data.
def cycle_account_ids(df):
    i=1
    for index,row in df.iterrows(): #For each account_id in test_data
        p_id = row['account_id']
        h_id = row['hero_id']
        logging.info(f"\n\ngetting player data....p_id: is {p_id}, and h_id: is {h_id}, interation: {i}")
        p_hero_data = get_player_hero_data(p_id, h_id)
        
        print(f"player hero data is: {p_hero_data}\n")

        #filtered_p_hero_data = filter_player_hero_data(p_hero_data) # function not written yet
        #final_p_hero_data = calculate_player_hero_stats(filtered_p_hero_data) # function not written yet
        
        #append final_p_hero_data to row['account_id']
        i+=1
    return p_hero_data

def main():
    logging.info(f"\n\n***  ***  ***  ***  ***  *** \n\n****   Starting... reading .csv   ****\n\n   ***   ***   ***   ***   ***\n\n")
    test_data = pd.read_excel(f'test_account_data.xlsx')
    logging.info(f"\n\ntest_data is now\n\n {test_data}")
    i=0
    p_hero_data = cycle_account_ids(test_data) #will need to become a different variable later-

if __name__ == main():
    main()

def filter_player_hero_data(df):
    ## p_hero total games played as p_hero_total_games (int)
    ## p_hero w/l over last 1 months as p_hero_1m_wl (int)
    ## p_hero w/l over last week as p_hero_1w_wl (int)

    return

def calculate_player_hero_stats():
    ## player w/l over last 3 games as player_3g_wl (int)
    ## player w/l over last month as player_1m_wl (int)
    return







#                         Hero Stats                  #
## Hero pickrate past 7 days (Patch, or trend swings)
## Hero pickrate past 30 days (longer term hero trends)
## Hero w/l past 7 days (patch, or meta swings)
## Hero w/l past 30 days (general strength)
## Other hero corrolations to data points? Do player clusters trend to specific heros?

#                         Player Stats                #
## Player w/l over same day games, max 3 (Short term tilt factor)
## Player k/d over same day games max 3 (in a tilt)
## Player w/l over past month (Bias for winning?)
## Player first active match date (Smurf?)

#                        Player_Hero Stats             #

## p_hero total games as hero (experience modifier)
## p_hero w/l over past 60 days (Player skill)
## p_hero hero recency frequency (Player may be rusty with hero)
## p_hero hero diversity (Shannon Entropy + Gini methods)

#                       Player Cluster Expectations     #
## Smurf - High W/L, low matches played, first match recent
## One-Trick - Very low hero diversity, if not playing one-trick hero, significantly reduced win score
## Tilter - Large loss streaks, tends to lose 2+ games in a row
## 

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