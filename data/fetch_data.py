
# imports
import requests
import json
import os
import sys
import pandas as pd
import logging
from urllib.parse import urlencode
import time
from datetime import timedelta, datetime


""" About this script
This script fetches data from various sources and prepares it for analysis.
"""

# logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def get_unix_time(days_ago=0):   
    """
    Convert days_ago to a unix timestamp
    
    Parameters:
    days_ago (int): Number of days in the past
    
    Returns:
    int: Unix timestamp for the date that was 'days_ago' days ago
    """
    current_time = int(time.time())
    seconds_ago = int(timedelta(days=days_ago).total_seconds())
    return current_time - seconds_ago

def get_time_delta(min_unix_time,max_time):
    """Returns string for url if short=False, else just the int"""
    min_unix_time = int(time.time()) #current time
    return (int(min_unix_time - timedelta(days=max_time).total_seconds()))

# fetch active matches for prediction

# fetch specific set of matches for analysis, training supplement, or prediction of a specific period

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
    - min_unix_timestamp: Oldest time to filter matches. i.e. matches 3 months ago (90) -> max time
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

        response = requests.get(full_url)

        if response.status_code != 200:
            logging.error(f"Error: API request failed with status code {response.status_code}")
            logging.error(f"URL: {full_url}")
            return {"error": f"API request failed with status code {response.status_code}"}
        return response.json()

    # Bulk-metadata endpoint
    path = f"{base}/metadata"
    params: dict[str, str] = {}

    if include_player_info:
        params["include_player_info"] = "true"
    
    # Convert days to unix timestamps - ensure max is newer (smaller days_ago) than min
    if min_unix_timestamp is not None:
        older_time = get_unix_time(min_unix_timestamp)
        params["min_unix_timestamp"] = str(older_time)
    if max_unix_timestamp is not None:
        newer_time = get_unix_time(max_unix_timestamp)
        params["max_unix_timestamp"] = str(newer_time)
        
        # Debug info for timestamps
        logging.debug(f"Time range: {max_unix_timestamp} days ago to {min_unix_timestamp} days ago")
        logging.debug(f"Unix timestamps: {newer_time} to {older_time}")

    if min_average_badge is not None:
        params["min_average_badge"] = str(min_average_badge)
    if limit is not None:
        params["limit"] = str(limit)

    query = urlencode(params)
    full_url = f"{path}?{query}" if query else path
    
    logging.info(f"Making request to: {full_url}")
    response = requests.get(full_url)
    if response.status_code != 200:
        logging.error(f"Error: API request failed with status code {response.status_code}")
        logging.error(f"URL: {full_url}")
        return {"error": f"API request failed with status code {response.status_code}"}
    
    return response.json()

def bulk_fetch_matches(max_days_fetch=90, min_days=3, max_days=0)->list:
    """fetches a batch of matches, 1 day per pull, list of jsons, 1 element per batch.

    batch is unnormalized, 'players' contains a df of each matches 'players'
    
    limit = max matches within a day to pull
    min_days = Oldest time barrier (more days ago)
    max_days = Newest time barrier (fewer days ago)
    max_days_fetch = max days to fetch, starting from max_days
    
    example:
    bulk_fetch_matches(max_days_fetch=30, min_days=7, max_days=0)
    will fetch data in one-day increments, from today back to 7 days ago,
    or until 30 days of data have been fetched.
    """

    limit = 500
    batch_matches = []
    
    # Calculate the starting day (defaults to today)
    current_max = max_days      # Newer boundary (fewer days ago)
    current_min = current_max + 1  # Older boundary (more days ago)
    
    for batch in range(max_days_fetch):
        logging.debug(f"\nBatch {batch}: fetching day from {current_max} to {current_min} days ago")
        print(f"DEBUG: Fetching matches for day {batch + 1} from {current_max} to {current_min} days ago")
        
        # Note: API expects min_unix_timestamp to be OLDER than max_unix_timestamp
        fetched_matches = fetch_match_data(
            min_unix_timestamp=current_min,  # Older timestamp (more days ago)
            max_unix_timestamp=current_max,  # Newer timestamp (fewer days ago)
            limit=limit
        )
        
        # Check if there was an error in the API response
        if "error" in fetched_matches:
            print(f"Error encountered during batch {batch+1}. Skipping this batch.")
        else:
            batch_matches.append(fetched_matches)
            
        # Move backward in time by one day
        current_max += 1  # Increase days ago for newer boundary
        current_min += 1  # Increase days ago for older boundary
        
        # Stop if we've reached the minimum days boundary
        if current_max >= min_days:
            print(f"Reached configured minimum day boundary ({min_days} days ago)")
            break

    return batch_matches

