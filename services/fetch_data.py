import requests
import json
import pandas as pd
import logging
from services import function_tools as u
from urllib.parse import urlencode

def fetch_match_data(
    min_average_badge: int = 100,
    max_unix_timestamp: int | None = None,
    min_unix_timestamp: int | None = None,
    m_id: str | None = None,
    include_player_info: bool = True,
    limit: int = 1000
    ) -> json:
    """Fetches match data from the Deadlock API.
    
    Key Parameters:
    - min_average_badge: Minimum average rank to return matches.
    - max_unix_timestamp: Newest time to filter matches. i.e. matches before yesterday
    - min_unix_timestamp: Oldest time to filter matches. i.e. matches 3 months ago -> max time
    - m_id: Specific match ID to fetch metadata for.
    - include_player_info: Whether to include player information in the response. this is required for match_player data
    - limit: Maximum number of matches to return.
    Returns:
    - JSON response containing match metadata with 12 players per match.
    """

    logging.debug(f"Fetching match data..")
    base = "https://api.deadlock-api.com/v1/matches"

    # if a specific match ID is given, check player_data and hit that endpoint
    if m_id:
        path = f"{base}/{m_id}/metadata"
        params = {}
        if include_player_info:
            params["include_player_info"] = "true"

        query = urlencode(params)
        full_url = f"{path}?{query}" if query else path
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

    return requests.get(full_url).json()

def bulk_fetch_matches(max_days_fetch=90,min_days=1,max_days=0)->json:
    """fetches a batch of matches, 1 day per pull, returns json and exports.

    batch is unnormalized, 'players' contains a df of each matches 'players'
    
    limit = max matches within a day to pull
    min_days = most recent day to pull, i.e. 1 day ago
    max_days_fetch = max days to fetch, i.e. 90 days ago
    
    example:
    bulk_fetch_matches(max_days_fetch=30,min_days=7,max_days=0)
    will fetch 30 days of matches, starting with 30 days ago, and going to 7 days ago.
    """

    limit = 5000
    batch_matches = []
    
    for batch in range(max_days_fetch):
        logging.debug(f"\n*day:{batch} min ={min_days} max = {max_days}")
        fetched_matches = fetch_match_data(min_unix_timestamp=min_days, max_unix_timestamp=max_days, limit=limit)
        batch_matches.append(fetched_matches)
        max_days +=1
        min_days +=1

    return batch_matches

def fetch_hero_trends(min_unix_time, min_average_badge=100)->pd.DataFrame:
    """fetches historical data for a hero @time @min badge"""
    site = "https://api.deadlock-api.com"
    endpoint = "/v1/analytics/hero-stats?" 

    url = f"{site}{endpoint}{min_unix_time}&{min_average_badge}"
    logging.info(f"\n\nGetting hero_data from full url: {url}\n\n")

    response = requests.get(url)
    if response.status_code == 200: #if 200, converts to pd.DataFrame: m_hero_data
        if not response.json():
            logging.critical(f"**ERROR** hero_data_fetched returned no results!")
        else:
            logging.info(f"hero data found!")
            response_data = response.json()
            m_hero_data = pd.DataFrame(response_data)
    else: 
        logging.error(f"Error fetching data, code = {response.status_code}\n\n")

    return m_hero_data

def fetch_player_match_history(p_id):
    """For the account_id listed, fetches full match history of player"""
    site = "https://api.deadlock-api.com"
    endpoint = f"/v1/players/{p_id}/match-history?only_stored_history=true" 

    url = site+endpoint
    logging.debug(f"Getting player match history from full url: {url}")

    response = requests.get(url)
    if response.status_code == 200: #if 200, converts to pd.DataFrame: m_hero_data
        response_data = response.json()
        player_match_history = pd.DataFrame(response_data)
    else: 
        logging.error(f"error fetching player match history, code = {response.status_code}\n\n")
        
    #return DataFrame
    if player_match_history is None:
        logging.error(f"player_match_history is None in fetch_player_match_history, p_id: {p_id}")
    if player_match_history.empty:
        logging.error(f"player_match_history is empty in fetch_player_match_history, p_id: {p_id}")
    return player_match_history

def fetch_hero_synergy_trends(days=60, min_average_badge=85, min_matches=50):
    """Fetches hero synergies for a given number of days and minimum average badge"""

    min_unix_timestamp = str(u.get_unix_time(days))

    site = "https://api.deadlock-api.com"
    endpoint = f"/v1/analytics/hero-synergy-stats?min_unix_timestamp={min_unix_timestamp}&min_average_badge={min_average_badge}&min_matches={min_matches}"
    url = site+endpoint
    logging.debug(f"Getting hero synergies from full url: {url}")
    
    response = requests.get(url)
    if response.status_code == 200:
        hero_synergies = pd.DataFrame(response.json())
    else:
        logging.debug(f"Failed to retrieve data: {response.status_code}")
    if hero_synergies is None:
        logging.error(f"hero_synergies is None in fetch_hero_synergies")
    if hero_synergies.empty:
        logging.error(f"hero_synergies is empty in fetch_hero_synergies")
    
    return hero_synergies

def fetch_hero_counter_trends(days=60, min_average_badge=85, min_matches=50):
    """Fetches hero counters for a given number of days and minimum average badge"""

    min_unix_timestamp = str(u.get_unix_time(days))

    site = "https://api.deadlock-api.com"
    endpoint = f"/v1/analytics/hero-counter-stats?min_unix_timestamp={min_unix_timestamp}&min_average_badge={min_average_badge}&min_matches={min_matches}"
    url = site+endpoint
    logging.debug(f"Getting hero counters from full url: {url}")
    
    response = requests.get(url)
    if response.status_code == 200:
        hero_counters = pd.DataFrame(response.json())
    else:
        logging.debug(f"Failed to retrieve data: {response.status_code}")
    if hero_counters is None:
        logging.error(f"hero_counters is None in fetch_hero_counters")
    if hero_counters.empty:
        logging.error(f"hero_counters is empty in fetch_hero_counters")
    
    return hero_counters

def fetch_player_hero_stats(p_id,h_id=None):
    """Fetches players hero-stats, then filters by h_id if provided"""

    # fetch player_hero from API on player_id
    site = "https://api.deadlock-api.com"
    endpoint = f"/v1/players/{p_id}/hero-stats"
    url = site+endpoint
    logging.debug(f"Getting player hero stats from full url: {url}")
    response = requests.get(url)
    if response.status_code == 200:
        p_h_data = (response.json()) #

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

def fetch_hero_info():
    """Returns base information for heros, i.e. name, background info, etc."""
    
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