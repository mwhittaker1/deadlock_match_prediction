import duckdb
import pandas as pd
import logging

def fetch_test_matches(con, n=1000):
    """Fetch n matches from the matches table"""
    logging.info(f"Fetching {n} matches from the matches table")
    
    # fetching matches
    matches_query = """
        SELECT match_id, winning_team
        FROM matches
        ORDER BY start_time DESC
        LIMIT ?
        """
    