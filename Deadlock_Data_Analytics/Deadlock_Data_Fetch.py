import requests
import pandas as pd
import numpy as np

#Fetches hero data from deadlock-api, returns as DataFrame
def fetch_hero_data():
    site = "https://assets.deadlock-api.com"
    endpoint = "/v2/heroes/"
    url = site+endpoint
    
    response = requests.get(url)
    
    # Check if the request was successful
    if response.status_code == 200:
        heroes_data = response.json()  # Converts the JSON response to a Python dictionary
    else:
        print(f"Failed to retrieve data: {response.status_code}")
    
    df = pd.DataFrame(heroes_data)
    return df

#Fetches Match data from deadlock-api, returns as DataFrame
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
