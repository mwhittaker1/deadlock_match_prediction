import requests
import pandas as pd

#fetches match data from DeadlockAPI

def fetch_match_data():
    site = "https://api.deadlock-api.com"
    endpoint = "/v1/matches/active"
    url = site+endpoint
    
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        match_data = pd.DataFrame(response.json())
    else:
        print(f"Failed to retrieve match data: {response.status_code}")
    
    return match_data #Returns JSON of match data.

def main():
    match_data = fetch_match_data()
    #print(match_data)

if __name__ == main():
    main()