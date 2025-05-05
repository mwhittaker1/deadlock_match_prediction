import pandas as pd
import logging
import duckdb
import services.database_functions as dbf
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

def run_etl_hero_trends():
    """ETL for 7-day and 30-day hero trends"""
    logging.info("starting run_etl_hero_trends ETL without critical errors.")
    #extract 7d hero trends
    trend_window = 7
    raw_hero_trends_7d = fd.fetch_hero_trends(trend_window)
    assert raw_hero_trends_7d is not None and not raw_hero_trends_7d.empty, (
    "ETL ERROR: hero_trends_7d DataFrame is empty")

    #transform 7d hero trends
    hero_trends_7d = tal.transform_hero_trends(trend_window,raw_hero_trends_7d)
    assert hero_trends_7d is not None and not hero_trends_7d.empty, (
    "ETL ERROR: transformed hero_trends_7d DataFrame is empty")
    
    #load 7d hero trends
    tal.load_hero_trends(hero_trends_7d)

    trend_window = 30
    #30d hero trends
    raw_hero_Trends_30d = fd.fetch_hero_trends(trend_window)
    assert raw_hero_Trends_30d is not None and not raw_hero_Trends_30d.empty, (
    "ETL ERROR: hero_trends_30d DataFrame is empty")

    #transform 30d hero trends
    hero_trends_30d = tal.transform_hero_trends(trend_window,raw_hero_Trends_30d)
    assert hero_trends_30d is not None and not hero_trends_30d.empty, (
    "ETL ERROR: transformed hero_trends_30d DataFrame is empty")

    #load 30d hero trends
    tal.load_hero_trends(hero_trends_7d)
    logging.info("completed 7d and 30d hero trends ETL without critical errors.")

if __name__ == "__main__":
    con = duckdb.connect("c:/Code/Local Code/deadlock_match_prediction/data/deadlock.db")
    dbf.reset_all_tables(con)
    run_etl_hero_trends()
    