import duckdb
import pandas as pd

def drop_tables(con):
    con.execute("DROP TABLE IF EXISTS matches")
    con.execute("DROP TABLE IF EXISTS player_matches")
    con.execute("DROP TABLE IF EXISTS player_trends")
    con.execute("DROP TABLE IF EXISTS hero_trends")

def create_tables(con):
    con.execute("""
    CREATE TABLE matches (
    match_id BIGINT,
    account_id BIGINT,
    start_time TIMESTAMP,
    game_mode VARCHAR,
    match_mode VARCHAR,
    duration_s INTEGER,
    winning_team VARCHAR,
    PRIMARY KEY (match_id, account_id)
    )
    """)

    con.execute("""
    CREATE TABLE player_matches (
    account_id BIGINT,
    match_id BIGINT,
    hero_id INTEGER,
    hero_level INTEGER,
    player_team INTEGER,
    player_kills INTEGER,
    player_deaths INTEGER,
    player_assists INTEGER,
    denies INTEGER,
    net_worth BIGINT,
    last_hits INTEGER,
    team_abandoned BOOLEAN,
    abandoned_time_s INTEGER,
    won BOOLEAN,
    p_total_kills BIGINT,
    p_total_deaths BIGINT,
    p_avg_kd FLOAT,
    p_total_matches BIGINT,
    p_h_total_matches BIGINT,
    p_h_pick_pct FLOAT,
    PRIMARY KEY (account_id, match_id)
    )
    """)

    con.execute("""
    CREATE TABLE player_trends (
    account_id BIGINT,
    match_id BIGINT,
    hero_id INTEGER,
    p_win_pct_3 FLOAT,
    p_win_pct_5 FLOAT,
    p_streak_3 VARCHAR,
    p_streak_5 VARCHAR,
    h_win_pct_3 FLOAT,
    h_win_pct_5 FLOAT,
    h_streak_3 VARCHAR,
    h_streak_5 VARCHAR,
    PRIMARY KEY (account_id, match_id, hero_id)
    )
    """)
    
    con.execute("""
    CREATE TABLE hero_trends (
    hero_id INTEGER,
    trend_start_date DATE,
    trend_end_date DATE,
    trend_date DATE,
    trend_window_days INTEGER,
    pick_rate FLOAT,
    win_rate FLOAT,
    average_kills FLOAT,
    average_deaths FLOAT,
    average_assists FLOAT,

    PRIMARY KEY (hero_id, trend_start_date, trend_end_date, trend_window_days)
    )
    """)

def create_test_subset(con):
    con.execute("""
    CREATE TABLE match_holdout AS
    SELECT *
    FROM player_matches
    WHERE match_id IN (
        SELECT match_id
        FROM player_matches
        GROUP BY match_id
        ORDER BY RANDOM()
        LIMIT 5000
    );""")

def create_player_profile(con):
    con.execute("""
    CREATE TABLE player_profiles (
    account_id BIGINT PRIMARY KEY,
    
    avg_p_win_pct_3 FLOAT,
    avg_p_win_pct_5 FLOAT,
    avg_h_win_pct_3 FLOAT,
    avg_h_win_pct_5 FLOAT,
    
    total_matches INTEGER,

    norm_avg_p_win_pct_3 FLOAT,  
    norm_avg_p_win_pct_5 FLOAT,
    norm_avg_h_win_pct_3 FLOAT,
    norm_avg_h_win_pct_5 FLOAT
    )
    """)
def alter_table():
    con.execute("ALTER TABLE player_trends ADD COLUMN p_total_kills int;")
    con.execute("ALTER TABLE player_trends ADD COLUMN p_total_deaths int;")
    con.execute("ALTER TABLE player_trends ADD COLUMN p_avg_kills float;")

def reset_all_tables():
    con = duckdb.connect("c:/Code/Local Code/Deadlock Database/Deadlock_Match_Prediction/deadlock.db")
    drop_tables(con)
    create_tables(con)
    #create_player_profile(con)

def manage_tbl_temp_p_m_history(df,insert=False, create=False, clear=False):
    if clear:
        con.execute("DROP TABLE IF EXISTS temp_player_match_history;")
    if create:
        con.execute("""
            CREATE TABLE temp_player_match_history (
              account_id BIGINT,
              match_id BIGINT,
              hero_id INTEGER,
              player_team INTEGER,
              won BOOLEAN,
              last4_matches_win_pct DOUBLE,
              last4_hero_matches_win_pct DOUBLE
        );""")
    if insert:
        con.execute("""INSERT OR IGNORE INTO 
        temp_player_match_history
        SELECT * FROM df""")
    


if __name__ == "__main__":
    con = duckdb.connect("c:/Code/Local Code/Deadlock Database/Deadlock_Match_Prediction/deadlock.db")
    reset_all_tables()
    #con.execute("ALTER TABLE player_trends drop COLUMN p_h_pick_per;")
    #con.execute("ALTER TABLE player_matches ADD COLUMN p_total_kills FLOAT;")

    #create_test_subset(con)