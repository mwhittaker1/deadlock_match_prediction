import requests
import json
import pandas as pd
import services.dl_fetch_data as fd
from services.utility_functions import to_csv, get_time_delta

def bulk_fetch_matches(max_days_fetch=90,max_days=0,min_days=1)->json:
    """fetches a batch of matches, 1 day per pull, returns json and exports.

    batch is unnormalized, 'players' contains a df of each matches 'players'
    limit = max matches within a day to pull
    max_days_fetch = how many days to cycle through for total fetch"""

    #print(f"start")
    limit = 5000
    batch_matches = []
    i=0
    
    for batch in range(max_days_fetch):
        print(f"\n*day:{i} min ={min_days} max = {max_days}")
        fetched_matches = fd.fetch_match_data(limit, min_days, max_days)
        batch_matches.extend(fetched_matches)
        max_days +=1
        min_days +=1
        i+=1
    with open("non_normal_total_matches.json", "w") as f:
        json.dump(batch_matches, f)
    #print(f"\n\nfin\n\n")
    return batch_matches

def fetch_active_match_data():
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

def fetch_match_data(limit,days,max_days=0,min_average_badge=100,m_id=None)->json:
    """Fetches metadata for single match, or fetches match range
    
    limit = Max number of matches to return
    days = how many days backwards to fetch from
    min_average_badge = minumum average rank for the match
    m_id = Return data for a specific match only, other variables not used."""
    
    site = "https://api.deadlock-api.com"
    endpoint = "/v1/matches/metadata?"

    if m_id:
        print(f"ERROR m_id found!")
        url=f"{site}{endpoint}include_player_info=true&match_ids={m_id}"
        response = requests.get(url)
    elif max_days != 0:
        print(f"\n**debug** - days= {days}, max days = {max_days}")
        min_unix_time = get_time_delta(days)
        max_unix_time = get_time_delta(max_days,True)
        url = f"{site}{endpoint}include_player_info=true&{min_unix_time}&{max_unix_time}&min_average_badge={min_average_badge}&limit={limit}"
        print(f"\n\n**debug** fetch_match_data, max_days !=0 URL is: {url}\n\n")
        response = requests.get(url)

    else:
        min_unix_time = get_time_delta(days)
        url = f"{site}{endpoint}include_player_info=true&{min_unix_time}&min_average_badge={min_average_badge}&limit={limit}"
        #print(f"\n\nURL is: {url}\n\n")
        response = requests.get(url)
    
    # Check if the request was successful
    if response.status_code == 200:
        json_match_data = response.json()
    else:
        print(f"\n\nFailed to retrieve match data: {response.status_code}\n\n response: {response}\n\n")
        return
    
    return json_match_data

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
    endpoint = f"/v1/players/{p_id}/match-history" 

    url = site+endpoint
    #print(f"\n\nGetting player_match_history p_id: {p_id} from full url: {url}\n\n")

    #get json
    response = requests.get(url)
    if response.status_code == 200: #if 200, converts to pd.DataFrame: m_hero_data
        response_data = response.json()
        #print(f"\n\nresponse_data[:5]\n\n")
        history = pd.DataFrame(response_data)
    else: 
        print(f"\n\nError fetching data, code = {response.status_code}\n\n")

    #return DataFrame
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