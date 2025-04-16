import pandas as pd
import requests
import logging
import time
from datetime import timedelta


# logging setup
def setup_logging(verbose: bool):
    level = logging.DEBUG if verbose else logging.INFO

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

### Fetch hero, match, account data, calculate win/loss (w/l) % as well as 1-2 month trends of data
#Fetch hero data
def get_m_hero_data():
    #API connection information
    site = "https://api.deadlock-api.com"
    endpoint = "/v1/analytics/hero-stats?" 

    min_average_badge = "/min_average_badge=115" #Minimum average match player rank
    c_unix_timestamp = int(time.time()) #current time
    x_days = 1    #days back to fetch
    x_days_ago = str(c_unix_timestamp - timedelta(days=1).total_seconds())    
    min_unix_time = f"/min_unix_timestamp={x_days_ago}"
    print(min_unix_time)
    url = site+endpoint+min_average_badge+min_unix_time
    logging.debug(f"Getting data from full url: {url}")

    #get json
    response = requests.get(url)
    if response.status_code == 200:
        m_hero_data = response
    else:
        logging.error(f"Error fetching data, code = {response.status_code}")

    #return json
    return m_hero_data

def main():
    logging.info(f"Starting scrpt...")
    logging.info(f"Fetchin match_hero data...")
    raw_m_h_data = get_m_hero_data()
    logging.info(f"match_hero data fetched.")

    logging.debug(f"")
    with open("output.json", "w") as f:
        f.write(raw_m_h_data)

    return

if __name__ == main():
    main()

## Hero pickrate for high ranked games over past 7 days

## Hero w/l for high ranked games over past month

## Hero w/l short term trend, past 7 days

#Collect match data
#[players],"start_time", "winning_team", "match_id", "lobby_id", "duration_s","match_mode", "game_mode","region_mode_parsed" 

#Collect match players
## Expecting DataFrame of 12 account_id, hero_id pairs as pd.DataFrame[match_players]

## Sort match players, 0-5 = winning team, 6-12 = losing team

#for each player Fetch p_hero stats (dict)
### See Player/account_id/hero-stats
## p_hero total games played as p_hero_total_games (int)

## p_hero w/l over last 2 months as p_hero_2m_wl (int)

## p_hero w/l over last week as p_hero_1w_wl (int)

## player w/l over last 3 games as player_3g_wl (int)

## player w/l over last month as player_1m_wl (int)

###Collect as dict{match_data: {m_data}, team_win: [[w_team]], team_lose: [[l_team]] } 

#Send data to AI for analytics

#Return prediction

