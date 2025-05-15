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
    print(f"matches_table_columns: {match_table_columns}\n"
          )
    
    player_table_columns = con.execute("""
        PRAGMA table_info('player_matches');
        """).fetchall()
    print(f"player_matches_table_columns: {player_table_columns}\n")

    hero_trends_table_columns = con.execute("""
        PRAGMA table_info('hero_trends');
        """).fetchall()
    print(f"hero_trends_table_columns: {hero_trends_table_columns}\n")

    player_matches_table_columns = con.execute("""
        PRAGMA table_info('player_matches');
        """).fetchall()
    print(f"player_matches_table_columns: {player_matches_table_columns}\n")

    player_trends_table_columns = con.execute("""
        PRAGMA table_info('player_trends');
        """).fetchall()
    print(f"player_trends_table_columns: {player_trends_table_columns}\n")

    player_matches_history_columns = con.execute("""
        PRAGMA table_info('player_matches_history');
        """).fetchall()
    print(f"player_matches_history_columns: {player_matches_history_columns}\n")
    """            **        Counts     **          """


    d_match_count = con.execute("""
        SELECT COUNT(distinct match_id)
        FROM matches
    """).fetchone()[0]
    print(f"distinct match_count in matches: {d_match_count}\n")

    d_match_player_matches = con.execute("""
        SELECT COUNT(DISTINCT match_id)
        FROM player_matches
    """).fetchone()[0]
    print(f"distinct_match_count in player_matches (should match in matches): {d_match_player_matches}\n")

    match_count = con.execute("""
        SELECT COUNT(match_id)
        FROM matches
    """).fetchone()[0]
    print(f"match_count: {match_count}\n")

    non_normal_account_count = con.execute("""
        SELECT COUNT(account_id)
        from player_matches""").fetchall()
    print(f"non_normal_account_count: {non_normal_account_count}\n")

    player_count = con.execute("""
        SELECT COUNT(DISTINCT account_id)
        FROM player_matches
    """).fetchone()[0]
    print(f"distinct_player_count from player_matches: {player_count}\n") 

    hero_trend_count = con.execute("""
        SELECT count(hero_id)
        FROM hero_trends
    """).fetchone()[0]
    print(f"hero_trend_count, expection 26 or 52, actual is: {hero_trend_count}")

    rolling_trend = con.execute("""
        SELECT COUNT(DISTINCT account_id)
        FROM player_rolling_stats
    """).fetchone()[0]
    print(f"rolling_trend total player account count: {rolling_trend}\n\n")

    count_d_players_in_trend = con.execute("""
        SELECT count(DISTINCT account_id)
        FROM player_trends
    """).fetchone()[0]
    print(f"count_distinct_players_in_trend: {count_d_players_in_trend}\n")

    min_max_start_time = con.execute("""
        SELECT MIN(start_time), MAX(start_time)
        FROM matches
    """).fetchall()
    print(f"min_max_start_time: {min_max_start_time}\n")

    match_id_min_max = con.execute("""SELECT match_id, start_time
    FROM matches
    WHERE start_time = (SELECT MIN(start_time) FROM matches)
   OR start_time = (SELECT MAX(start_time) FROM matches)""").fetchall()
    print(f"match_id_min_max: {match_id_min_max}\n")

    print("""         matching data for expectations        """)
    pmptdiff = player_count - count_d_players_in_trend
    rdpddiff = rolling_trend - count_d_players_in_trend
    print(
        f"count distinct account_id in player_matches should match distinct account_id count in player trends,\n"
        f"distinct_acccount_id in player_matches: {player_count},\n"
        f"distinct_acccount_id in player_trends: {count_d_players_in_trend}\n"
        f"difference = {pmptdiff}\n"
    )
    print(
        f"count distinct account_id in rolling_trend should match distinct account_id in player_trends,\n"
        f"distinct_acccount_id in rolling_trend: {rolling_trend},\n"
        f"distinct_acccount_id in player_trends: {count_d_players_in_trend}\n"
        f"difference = {rdpddiff}\n"
    )
 
def check_newer_matches_in_raw(con):
    """for each match_id, check if player count is correct"""

def compare_raw_to_current():
    con = duckdb.connect(r"C:\Code\Local Code\deadlock_match_prediction\\match_player_raw.duckdb")
    matches_df = duckdb.connect("c:/Code/Local Code/deadlock_match_prediction/data/deadlock.db") \
                       .execute("SELECT DISTINCT account_id FROM player_matches").fetchdf()

    con.register("matches_df", matches_df)  # Register as temp table

    result = con.execute("""
        SELECT 
            CASE 
                WHEN s.account_id IS NOT NULL THEN 'matched'
                ELSE 'unmatched'
            END AS status,
            COUNT(*) AS count
        FROM matches_df m
        LEFT JOIN staging_cleaned s
        ON m.account_id = s.account_id
        WHERE s.match_id > 34728095
        GROUP BY status
        
    """).fetchdf()

    print(result)

def test1():
    con = duckdb.connect(r"C:\Code\Local Code\deadlock_match_prediction\\match_player_raw.duckdb")
    print(str(con))
    print(con.execute("SHOW TABLES").fetchdf())


if __name__ == "__main__":
    con = duckdb.connect(r"C:\Code\Local Code\deadlock_match_prediction\\data\deadlock.db")
    compare_raw_to_current()
    #run_checks(con)
    #compare_raw_to_current()
    #logging.info("\n\nData integrity checks completed")
