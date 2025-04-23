import requests
import json
import pandas as pd
import time
from datetime import timedelta
from Deadlock_Match_Prediction.utility_functions import to_csv

#fetches match data from DeadlockAPI

def get_time_delta(days):
    c_unix_timestamp = int(time.time()) #current time
    x_days_ago = str(int(c_unix_timestamp - timedelta(days=days).total_seconds()))    
    min_unix_time = f"min_unix_timestamp={x_days_ago}"
    return min_unix_time

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

def main():
    days = 10
    min_average_badge = f"&min_average_badge=100"
    match_data = fetch_match_data(days, min_average_badge)
    #with open('test_match_data.json','w') as f:
        #json.dump(match_data,f,indent=4)
    #print(match_data)

if __name__ == main():
    main()