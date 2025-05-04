import requests
import json
import pandas as pd
import dl_match_prediction.services.function_tools as u
from urllib.parse import urlencode


def fetch_match_data(
    min_average_badge: int = 100,
    max_unix_timestamp: int | None = None,
    min_unix_timestamp: int | None = None,
    m_id: str | None = None,
    include_player_info: bool = True,
    limit: int = 1000
    ) -> json:

    base = "https://api.deadlock-api.com/v1/matches"
    # if a specific match ID is given, check player_data and just hit that endpoint

    if m_id:
        path = f"{base}/{m_id}/metadata"
        params = {}
        if include_player_info:
            params["include_player_info"] = "true"

        query = urlencode(params)
        full_url = f"{path}?{query}" if query else path
        print(f"*DEBUG* GET {full_url}")
        return requests.get(full_url).json()


    # Bulk-metadata endpoint
    path = f"{base}/metadata"
    params: dict[str, str] = {}

    if include_player_info:
        params["include_player_info"] = "true"
    if min_unix_timestamp is not None:
        params["min_unix_timestamp"] = str(u.get_unix_time(min_unix_timestamp))
    if max_unix_timestamp is not None:
        params["max_unix_timestamp"] = str(u.get_unix_time(max_unix_timestamp))
    if min_average_badge is not None:
        params["min_average_badge"] = str(min_average_badge)
    if limit is not None:
        params["limit"] = str(limit)

    query = urlencode(params)
    full_url = f"{path}?{query}" if query else path
    print(f"*DEBUG* GET {full_url}")
    return requests.get(full_url).json()

def bulk_fetch_matches(max_days_fetch=90,min_days=0,max_days=1)->json:
    """fetches a batch of matches, 1 day per pull, returns json and exports.

    batch is unnormalized, 'players' contains a df of each matches 'players'
    limit = max matches within a day to pull
    max_days_fetch = how many days to cycle through for total fetch"""

    #print(f"start")
    limit = 5000
    batch_matches = []
    
    for batch in range(max_days_fetch):
        print(f"\n*day:{batch} min ={min_days} max = {max_days}")
        fetched_matches = fetch_match_data(min_unix_timestamp=min_days, max_unix_timestamp=max_days, limit=limit)
        batch_matches.append(fetched_matches)
        max_days +=1
        min_days +=1

    return batch_matches

####

def fetch_hero_data(min_unix_time, min_average_badge):
    """fetches historical data for a hero @time @min badge"""
    #API connection information
    site = "https://api.deadlock-api.com"
    endpoint = "/v1/analytics/hero-stats?" 

    url = f"{site}{endpoint}{min_unix_time}&{min_average_badge}"
    print(f"\nGetting hero_data from full url: {url}")

    #get json
    response = requests.get(url)
    if response.status_code == 200: #if 200, converts to pd.DataFrame: m_hero_data
        response_data = response.json()
        m_hero_data = pd.DataFrame(response_data)
    else: 
        print(f"\n\nError fetching data, code = {response.status_code}\n\n")

    #return DataFrame
    return m_hero_data

def fetch_hero_info():
    """Returns base information for heros, i.e. name, background"""
    site = "https://assets.deadlock-api.com"
    endpoint = "/v2/heroes/"
    url = site+endpoint
    
    response = requests.get(url)
    
    # Check if the request was successful
    if response.status_code == 200:
        heroes_info = pd.DataFrame(response.json())
    else:
        print(f"Failed to retrieve data: {response.status_code}")
    
    df = pd.DataFrame(heroes_info)
    return df

def fetch_player_match_history(p_id):
    """For the account_id listed, fetches full match history of player"""
    site = "https://api.deadlock-api.com"
    endpoint = f"/v1/players/{p_id}/match-history?only_stored_history=true" 

    url = site+endpoint
    #print(f"\n\nGetting player_match_history p_id: {p_id} from full url: {url}\n\n")

    #get json
    response = requests.get(url)
    if response.status_code == 200: #if 200, converts to pd.DataFrame: m_hero_data
        response_data = response.json()
        #print(f"\n\nresponse_data[:5]\n\n")
        history = pd.DataFrame(response_data)
    else: 
        print(f"\n\nError fetch_player_match_history, code = {response.status_code}\n\n response= {response}\n\n")

    #return DataFrame
    if history is None:
        print(f"\n\n**ERROR** history is blank in fetch_player_match_history, p_id: {p_id}\n\n")
    if history.empty:
        print(f"\n\n**ERROR** history is empty in fetch_player_match_history, p_id: {p_id}\n\n")
    return history

def fetch_player_hero_stats(p_id,h_id=None):
    """Fetches players hero-stats, then filters by h_id if provided"""

    # fetch player_hero from API on player_id
    site = "https://api.deadlock-api.com"
    endpoint = f"/v1/players/{p_id}/hero-stats"
    url = site+endpoint
    
    print(f"\n\nsending request for response")
    response = requests.get(url)
    if response.status_code == 200:
        p_h_data = (response.json()) #player all hero data
        #response_data = (response.json()) #player all hero data
        #p_h_data = pd.DataFrame(response_data)
        #print(f"\n\n found player data! player: {p_id}")    
    else:
        print(f"\n\nFailed to retrieve match data: {response.status_code}")
    if h_id:
        print(f"\n\nin fetch_player_hero_data, all p_h_data found, filtering for h_id found: {h_id}")
        p_h_data = next((item for item in p_h_data if item['hero_id'] == h_id), None)
        p_h_matches = p_h_data.pop('matches')
        print(f"\n\n no matches, p_h_data is now {p_h_data}\n\n p_h_matches = {p_h_matches}")
        p_h_data = pd.DataFrame([p_h_data])
    else:
        print(f"*** h_id not found ID: {h_id} ***")
        for item in p_h_data:
            item.pop("matches",None)
        p_h_all_data = pd.DataFrame(p_h_data)
        return p_h_all_data
    return p_h_data

def old_fetch_active_match_data():
    """Fetches most recent 200 active matches, no parameters expected."""
    site = "https://api.deadlock-api.com"
    endpoint = "/v1/matches/active"
    url = site+endpoint
    print(f"\n\nURL is: {url}\n\n")
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        match_data = pd.DataFrame(response.json())
        #match_data = response.json()
        #with open('json_match_data.json','w') as f:
            #json.dump(match_data,f,indent=4)
    else:
        print(f"\n\nFailed to retrieve match data: {response.status_code}\n\n")
        return
    
    return match_data #Returns JSON of match data.

def test():
    days = 5
    min_average_badge = f"&min_average_badge=100"
    match_data = fetch_match_data(days, min_average_badge)
    print(match_data)
    #with open('test_match_data.json','w') as f:
        #json.dump(match_data,f,indent=4)
    #print(match_data)

if __name__ == "__main__":
    test()