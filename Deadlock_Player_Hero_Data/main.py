import pandas as pd
import requests
import logging
import time
import os
from xlsx_output import to_xlsx
from datetime import timedelta

## Author : Mickey Whittaker
## Last Edit Date : 4/16/2025, 10:15am


# logging setup
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

### Fetch hero, match, account data, calculate win/loss (w/l) % 
### as well as 1-2 month trends of data
def get_time_delta(days):
    c_unix_timestamp = int(time.time()) #current time
    x_days_ago = str(int(c_unix_timestamp - timedelta(days=1).total_seconds()))    
    min_unix_time = f"min_unix_timestamp={x_days_ago}"
    return min_unix_time


#Fetch hero data
def get_match_hero_data(min_unix_time, min_average_badge):
    #API connection information
    site = "https://api.deadlock-api.com"
    endpoint = "/v1/analytics/hero-stats?" 

    url = site+endpoint+min_unix_time+"&"+min_average_badge
    logging.debug(f"Getting data from full url: {url}")

    #get json
    response = requests.get(url)
    if response.status_code == 200: #if 200, converts to pd.DataFrame: m_hero_data
        response_data = response.json()
        logging.info(response_data[:5])
        m_hero_data = pd.DataFrame(response_data)
        logging.info(f"DataFrame shape: {m_hero_data.shape}")
        logging.info(f"Columns: {m_hero_data.columns.tolist()}")
        logging.info(f"Data types:\n{m_hero_data.dtypes.to_string()}")
        logging.info(f"Sample data:\n{m_hero_data.head(3).to_string(index=False)}")
    else: #returns error code, logging.error
        logging.error(f"Error fetching data, code = {response.status_code}")

    #return DataFrame
    return m_hero_data


##Accepts DataFrame
def hero_stats(m_hero_df):
    #returns total matches
    def get_total_matches(df):
        total_matches = df['matches'].sum()
        total_matches = total_matches/12
        print(f"sum matches = :{total_matches}")
        return total_matches
    
    #adds pickrate to df
    def get_hero_pickrate(matches, df):
        df['hero_pickrate'] = ((df['matches'])/matches*100).round(2)
        return df

    #adds win_percentage to df
    def hero_win_percentage(df):
        df['win_percentage'] = (df['wins'].replace(0,1)/df['matches'].replace(0,1)*100).round(2)
        return df

    sum_matches = get_total_matches(m_hero_df) 
    m_hero_df = get_hero_pickrate(sum_matches, m_hero_df)
    m_hero_df = hero_win_percentage(m_hero_df)
    return m_hero_df

def main():
    days = "2" #number of days to fetch hero_match data from today.
    min_average_badge = "min_average_badge=100" #filters matches by msinimum average match rank
    min_unix_time = get_time_delta(days)

    logging.info(f"Starting scrpt...")
    logging.info(f"Fetchin match_hero data...")
    raw_match_hero_data = get_match_hero_data(min_unix_time, min_average_badge) #Returns Dataframe of hero data over x_days, above min_average_badge
    logging.info(f"match_hero data fetched.")
    logging.debug(f"raw_m_h_data has been received.")

    match_hero_stats = hero_stats(raw_match_hero_data)
    logging.info(f"hero_stats added! :\n {match_hero_stats}")
    return

if __name__ == main():
    main()

#Collect match data
#[players],"start_time", "winning_team", "match_id", "lobby_id", "duration_s","match_mode", "game_mode","region_mode_parsed" 

## Hero pickrate for high ranked games over past 7 days
## Hero w/l for high ranked games over past month
## Hero w/l short term trend, past 7 days

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

