import pandas as pd
import requests
import logging
import openpyxl


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
        print(f" found player data, here it is!: \n {p_all_h_data}")    
    else:
        print(f"Failed to retrieve match data: {response.status_code}")
    
    p_h_data = next((item for item in p_all_h_data if item['hero_id'] == h_id), None)
    
    if p_all_h_data:
        logging.info(f"\n\nfound player_hero data for hero: {h_id} printing data \n {p_h_data}")
    else:
        logging.error(f"get_player_hero_data, find h_id in player data did not find match")

    return p_h_data #.json of h_id for p_id

# for each account_id, hero_id in df, pull player_hero data.
def cycle_account_ids(df):
    i=1
    for index,row in df.iterrows(): #For each account_id in test_data
        p_id = row['account_id']
        h_id = row['hero_id']
        logging.info(f"\n\ngetting player data....p_id is {p_id} and h_id is {h_id} interation {i}")
        p_hero_data = get_player_hero_data(p_id, h_id)
        
        print(f"player hero data is: {p_hero_data}")

        filtered_p_hero_data = filter_player_hero_data(p_hero_data) # function not written yet
        final_p_hero_data = calculate_player_hero_stats(filtered_p_hero_data) # function not written yet
        #append final_p_hero_data to row['account_id']
        i+=1
        return p_hero_data

def main():
    logging.info(f"Starting... reading .csv")
    test_data = pd.read_excel(f'single_account_data.xlsx')
    logging.info(f"test_data is now {test_data}")
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