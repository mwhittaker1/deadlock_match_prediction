import requests
import pandas as pd
import numpy as np

#Takes data types to fetch and returns data type_df for each type.
## e.g. args "hero, match" will fetch and return hero_df and match_df
def fetch_data(*args):
    result = {}
    if "hero" in args:
        site = "https://assets.deadlock-api.com"
        endpoint = "/v2/heroes/"
        url = site+endpoint
        response = requests.get(url)
        
        # Check if the request was successful
        if response.status_code == 200:
            result["hero"] = pd.DataFrame(response.json())  # Converts the JSON response to a Python dictionary
        else:
            print(f"Failed to retrieve hero data: {response.status_code}")

    if "match" in args:
        site = "https://api.deadlock-api.com"
        endpoint = "/v1/matches/active"
        url = site+endpoint
        response = requests.get(url)
        
        # Check if the request was successful
        if response.status_code == 200:
             result["match"] = pd.DataFrame(response.json())  # Converts the JSON response to a Python dictionary  # Converts the JSON response to a Python dictionary
        else:
            print(f"Failed to retrieve match data: {response.status_code}")
        
    else:
        print(f"No valid matches found, expected 'hero' or 'match'")

    return result