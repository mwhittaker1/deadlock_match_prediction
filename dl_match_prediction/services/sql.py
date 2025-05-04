import pandas as pd
import duckdb
import logging
import db
import function_tools as u
u.setup_logger()
logging.info("Logger initialized.")


def run_checks():
    """run sql queries to check data integrity"""
    logging.info("Running data integrity checks")

    #check if tables exist
    tables = db.con.execute("""
        SELECT name FROM sqlite_master WHERE type='table';
        """).fetchall()
    print(f"tables: {tables}\n")

    match_table_columns = db.con.execute("""
        PRAGMA table_info('matches');
        """).fetchall()
    print(f"match_table_columns: {match_table_columns}\n"
          )
    
    player_table_columns = db.con.execute("""
        PRAGMA table_info('player_matches');
        """).fetchall()
    print(f"player_table_columns: {player_table_columns}\n")

    match_count = db.con.execute("""
        SELECT COUNT(match_id)
        FROM matches
    """).fetchall()
    print(f"match_count: {match_count}\n")

    player_count = db.con.execute("""
        SELECT count(account_id)
        FROM player_matches
    """).fetchall()
    print(f"player_count: {player_count}\n")

if __name__ == "__main__":
    run_checks()
    logging.info("\n\nData integrity checks completed")