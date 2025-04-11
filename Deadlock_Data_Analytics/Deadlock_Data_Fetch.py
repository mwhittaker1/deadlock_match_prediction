import requests
import pandas as pd
import numpy as np
import json
import random

#Takes data types to fetch and returns data type_df for each type.
## e.g. args "hero, match" will fetch and return hero_df and match_df
def fetch_data(args):
    result = pd.DataFrame()
    for arg in args:
        print(f"args is {arg}") #debugging
        if arg == "hero":
            site = "https://assets.deadlock-api.com"
            endpoint = "/v2/heroes/"
            url = site+endpoint
            response = requests.get(url)
            
            # Check if the request was successful
            if response.status_code == 200:
                result["hero"] = pd.DataFrame(response.json())  # Converts the JSON response to a Python DataFrame
            else:
                print(f"Failed to retrieve hero data: {response.status_code}")
        else:
            result['hero'] = "empty"
        if arg == "match":
            site = "https://api.deadlock-api.com"
            endpoint = "/v1/matches/active"
            url = site+endpoint
            response = requests.get(url)
            
            # Check if the request was successful
            if response.status_code == 200:
                match_data = pd.DataFrame(response.json())
                result["match"] = match_data 
            else:
                print(f"Failed to retrieve match data: {response.status_code}")
            
        else:
            print(f"No valid matches found, expected 'hero' or 'match'")
    for key, df in result.items():
        if isinstance(df, pd.DataFrame):  # Ensure it's a DataFrame
            sample = df.sample(n=5)  # Sample 5 rows randomly from the DataFrame
            print(f"Sample from {key}:")
            print(sample)
        else:
            print(f"{key} is not a DataFrame!")
    return result