import pandas as pd
import duckdb
import os
import json
import sys
from pathlib import Path
from datetime import datetime, timezone
import services.fetch_data as fd
import services.database_functions as dbf
import services.utility_functions as u
import services.transform_and_load as tal

def run_etl_bulk_matches():
    """ETL for bulk match data, fetches, normalizes and loads into db"""
    # Fetch data
    print(f"*INFO* ETL: Fetching match data")
    matches_grouped_by_day = fd.bulk_fetch_matches(max_days_fetch=90,min_days=0,max_days=1)
    print(f"*INFO* ETL: Data fetched")
    
    # Normalize data
    print(f"*INFO* ETL: Normalizing data")
    norm_matches, norm_players = tal.normalize_bulk_matches(matches_grouped_by_day)
    print(f"*INFO* ETL: Data normalized")
    
    # Load data into database
    print(f"*INFO* ETL: Loading data into database")
    dbf.load_bulk_matches(normalized_data)
    print(f"*INFO* ETL: Data loaded into database")