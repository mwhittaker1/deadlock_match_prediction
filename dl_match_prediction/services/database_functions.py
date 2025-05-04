import duckdb
import pandas as pd
import duckdb
import pandas as pd

con = duckdb.connect("c:/Code/Local Code/Deadlock Database/dl_match_prediction/deadlock.db")

def drop_all_tables(con):
    tables_to_drop = {
        "matches",
        "player_matches",
        "player_trends",
        "player_hero_trends",
        "hero_trends"
        }
    for table in tables_to_drop:
        con.execute(f"DROP TABLE IF EXISTS {table}")
    existing_tables = set(t[0] for t in con.execute("SHOW TABLES;").fetchall())
    remaining = [t for t in tables_to_drop if t in existing_tables]
    assert not remaining, f"***ERROR*** Tables not dropped: {remaining}"
    print(f"\n*INFO* all tables dropped")

def create_all_tables(con):
    print(f"\n*INFO* creating all tables, expecting:\n\ncreate_matches_table\ncreate_player_matches_table\ncreate_player_trends_table\ncreate_player_hero_trends\ncreate_hero_trends_table")
    expected_tables = {
        "matches",
        "player_matches",
        "player_trends",
        "player_hero_trends",
        "hero_trends"
        }
    
    create_matches_table(con)
    create_player_matches_table(con)
    create_player_trends_table(con)
    create_player_hero_trends(con)
    create_hero_trends_table(con)
    
    result = set(t[0] for t in con.execute("SHOW TABLES;").fetchall())
    assert expected_tables.issubset(result), f"Missing tables: {expected_tables - result}"
    print(f"\n*INFO* all tables created")

def create_matches_table(con):
    con.execute("""
    CREATE TABLE matches (
    match_id BIGINT,
    start_time TIMESTAMP,
    game_mode VARCHAR,
    match_mode VARCHAR,
    duration_s INTEGER,
    winning_team VARCHAR,
    PRIMARY KEY (match_id)
    )
    """)

def create_player_matches_table(con):
    con.execute("""
    CREATE TABLE player_matches (
    account_id BIGINT,
    match_id BIGINT,
    hero_id INTEGER,
    player_team INTEGER,
    player_kills INTEGER,
    player_deaths INTEGER,
    player_assists INTEGER,
    denies INTEGER,
    net_worth BIGINT,
    team_abandoned BOOLEAN,
    PRIMARY KEY (account_id, match_id)
    )
    """)

def create_player_trends_table(con):
    con.execute("""
    CREATE TABLE player_trends (
    account_id BIGINT,
    p_total_kills BIGINT,
    p_total_deaths BIGINT,
    p_avg_kd FLOAT,
    p_total_matches BIGINT,
    p_win_pct_3 FLOAT,
    p_win_pct_5 FLOAT,
    PRIMARY KEY (account_id)
    )
    """)

def create_player_hero_trends(con):
    con.execute("""
    CREATE TABLE player_hero_trends (
    account_id BIGINT,
    hero_id INTEGER,
    trend_start_date DATE,
    trend_end_date DATE,
    trend_window_days INTEGER,
    p_h_total_matches BIGINT,
    p_h_pick_pct FLOAT,
    p_h_win_pct_3 FLOAT,
    p_h_win_pct_5 FLOAT,
    p_h_streak_3 VARCHAR,
    p_h_streak_5 VARCHAR,
    PRIMARY KEY (account_id, hero_id, trend_start_date, trend_end_date, trend_window_days)
    )
    """)

def create_hero_trends_table(con):  
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
    average_kd FLOAT,
    PRIMARY KEY (hero_id, trend_start_date, trend_end_date, trend_window_days)
    )
    """)

def reset_all_tables(con):
    drop_all_tables(con)
    create_all_tables(con)
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
    #reset_all_tables()
    #con.execute("ALTER TABLE player_trends drop COLUMN p_h_pick_per;")
    #con.execute("ALTER TABLE player_matches ADD COLUMN average_kd FLOAT;")

    #create_test_subset(con)