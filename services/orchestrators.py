import pandas as pd
import logging
import duckdb
from services import database_functions as dbf
from services import fetch_data as fd
from services import transform_and_load as tal


def run_etl_bulk_matches(max_days_fetch=3):
    """ETL for bulk match data, fetches, normalizes and loads into db"""
    # Fetch data
    logging.info(f"*INFO* ETL: Fetching data")
    matches_grouped_by_day = fd.bulk_fetch_matches(max_days_fetch,min_days=1,max_days=0)
    logging.info(f"*INFO* ETL: Data fetched")
    
    # Normalize data
    logging.info(f"*INFO* ETL: Normalizing data")
    normalized_matches, normalized_players  = tal.normalize_bulk_matches(matches_grouped_by_day)
    logging.info(f"*INFO* ETL: Data normalized")
    
    # Load data into database
    logging.info(f"*INFO* ETL: Loading data into database")
    tal.load_bulk_matches(normalized_matches, normalized_players)
    logging.info(f"*INFO* ETL: Data loaded into database")

def run_etl_hero_trends():
    """ETL for 7-day and 30-day hero trends"""
    logging.info("starting run_etl_hero_trends ETL without critical errors.")

    #ETL 7 day hero trends
    trend_window = 7
    logging.info(f"*INFO* ETL: Fetching hero trends for {trend_window} days")
    raw_hero_trends_7d = fd.fetch_hero_trends(trend_window)
    hero_trends_7d = tal.transform_hero_trends(trend_window,raw_hero_trends_7d)
    tal.load_hero_trends(hero_trends_7d)

    #ETL 30d hero trends
    trend_window = 30
    logging.info(f"*INFO* ETL: Fetching hero trends for {trend_window} days")
    raw_hero_Trends_30d = fd.fetch_hero_trends(trend_window)
    hero_trends_30d = tal.transform_hero_trends(trend_window,raw_hero_Trends_30d)
    tal.load_hero_trends(hero_trends_30d)
    logging.info("completed 7d and 30d hero trends ETL without critical errors.")

if __name__ == "__main__":
    pass
    