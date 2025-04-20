import requests
import json
import pandas as pd
import logging
from utility_functions import to_csv, get_time_delta, setup_logging, initialize_logging

logger = logging.getLogger(__name__)
#Fetch Data Requests

#Fetches 200 active matches with high badge rating. Will be used to cast predictions against.
def fetch_active_match_data():
    site = "https://api.deadlock-api.com"
    endpoint = "/v1/matches/active"
    url = site+endpoint
    
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        match_data = pd.DataFrame(response.json())
        #match_data = response.json()
        #with open('json_match_data.json','w') as f:
            #json.dump(match_data,f,indent=4)
    else:
        print(f"Failed to retrieve match data: {response.status_code}")
    
    return match_data #Returns JSON of match data.

#Fetches historical match data, @days = historical days backwards, @min_average_badge represents skill level
def fetch_match_data(days,min_average_badge):
    site = "https://api.deadlock-api.com"
    endpoint = "/v1/matches/metadata?"
    min_unix_time = get_time_delta(days)
    url = site+endpoint+"include_player_info=true"+"&"+min_unix_time+min_average_badge+"&limit=5000"
    print(f"URL is: {url}")

    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        match_data = pd.DataFrame(response.json())
        to_csv(match_data, "10d_100b_match_metadata")
        #match_data = response.json()
        #with open('json_match_data.json','w') as f:
            #json.dump(match_data,f,indent=4)
    else:
        print(f"Failed to retrieve match data: {response.status_code}\n\n response: {response}")
    
    return match_data #Returns JSON of match data.

#Fetch hero data, @min_unix_time is historical time start point, @min_average_badge is min skill level
def fetch_hero_data(min_unix_time, min_average_badge):
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

def fetch_player_hero_data(p_id,h_id=None):

    # fetch player_hero from API on player_id
    site = "https://api.deadlock-api.com"
    endpoint = f"/v1/players/{p_id}/hero-stats"
    url = site+endpoint
    
    logging.info(f"sending request for response")
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        p_h_data = response.json() #player all hero data
        print(f" found player data! player: {p_id}")    
    else:
        print(f"Failed to retrieve match data: {response.status_code}")
    
    if h_id is not None:
        p_h_data = next((item for item in p_h_data if item['hero_id'] == h_id), None)

    return p_h_data #.json of h_id for p_id

def main():
    days = 10
    min_average_badge = f"&min_average_badge=100"
    match_data = fetch_match_data(days, min_average_badge)
    #with open('test_match_data.json','w') as f:
        #json.dump(match_data,f,indent=4)
    #print(match_data)

if __name__ == "__main__":
    main()