import pandas as pd
import duckdb
import logging
import function_tools as u

def run_checks(con):
    """run sql queries to check data integrity"""
    logging.info("Running data integrity checks")

    #check if tables exist
    tables = con.execute("""
        SELECT name FROM sqlite_master WHERE type='table';
        """).fetchall()
    print(f"tables: {tables}\n")

    match_table_columns = con.execute("""
        PRAGMA table_info('matches');
        """).fetchall()
    print(f"match_table_columns: {match_table_columns}\n"
          )
    
    player_table_columns = con.execute("""
        PRAGMA table_info('player_matches');
        """).fetchall()
    print(f"player_table_columns: {player_table_columns}\n")

    hero_trends_table_columns = con.execute("""
        PRAGMA table_info('hero_trends');
        """).fetchall()
    print(f"hero_trends_table_columns: {hero_trends_table_columns}\n")

    player_matches_table_columns = con.execute("""
        PRAGMA table_info('player_matches');
        """).fetchall()
    print(f"player_matches_table_columns: {player_matches_table_columns}\n")

    match_count = con.execute("""
        SELECT COUNT(match_id)
        FROM matches
    """).fetchall()
    print(f"match_count: {match_count}\n")

    player_count = con.execute("""
        SELECT count(account_id)
        FROM player_matches
    """).fetchall()
    print(f"player_count: {player_count}\n")

    player_match_count = con.execute("""
        SELECT count(account_id)
        FROM player_matches
    """).fetchall()
    print(f"player_match_count: {player_match_count}\n")  

    player_match_d_count = con.execute("""
        SELECT count(DISTINCT account_id)
        FROM player_matches
    """).fetchall()
    print(f"player_match_d_count: {player_match_d_count}\n")  

    hero_trend_count = con.execute("""
        SELECT count(hero_id)
        FROM hero_trends
    """).fetchone()
    print(f"hero_trend_count, expection 26 or 52, actual is: {hero_trend_count}")

    rolling_trend = con.execute("""
        SELECT count(match_id)
        FROM player_rolling_stats
    """).fetchone()
    print(f"rolling_trend count match_id: {rolling_trend} should match player_match_count: {player_match_count}\n\n")

    count_players_in_trend = con.execute("""
        SELECT count(DISTINCT account_id)
        FROM player_trends
    """).fetchall()
    print(f"count_players_in_trend: {count_players_in_trend}\n")
        
if __name__ == "__main__":
    con = duckdb.connect("c:/Code/Local Code/deadlock_match_prediction/data/deadlock.db")
    run_checks(con)
    logging.info("\n\nData integrity checks completed")