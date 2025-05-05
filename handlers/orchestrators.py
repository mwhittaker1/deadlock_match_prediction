import pandas as pd
import logging
import services.fetch_data as fd
import services.transform_and_load as tal

def run_etl_bulk_matches(max_days_fetch=3):
    """ETL for bulk match data, fetches, normalizes and loads into db"""
    # Fetch data
    logging.info(f"*INFO* ETL: Fetching data")
    matches_grouped_by_day = fd.bulk_fetch_matches(max_days_fetch,min_days=1,max_days=0)
    assert matches_grouped_by_day, "ETL ERROR: No matches fetched – matches_grouped_by_day is empty"
    logging.info(f"*INFO* ETL: Data fetched")
    
    # Normalize data
    logging.info(f"*INFO* ETL: Normalizing data")
    normalized_matches, normalized_players  = tal.normalize_bulk_matches(matches_grouped_by_day)
    assert normalized_matches is not None and not normalized_matches.empty, (
    "ETL ERROR: normalized_matches DataFrame is empty")
    assert normalized_players is not None and not normalized_players.empty, (
    "ETL ERROR: normalized_players DataFrame is empty")
    logging.info(f"*INFO* ETL: Data normalized")
    
    # Load data into database
    logging.info(f"*INFO* ETL: Loading data into database")
    tal.load_bulk_matches(normalized_matches, normalized_players)
    logging.info(f"*INFO* ETL: Data loaded into database")

if __name__ == "__main__":
    run_etl_bulk_matches()
    