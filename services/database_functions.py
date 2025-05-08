import duckdb
import pandas as pd
import logging
from data import db
from services import function_tools as u

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
        logging.info(f"Dropped table: {table}")
    existing_tables = set(t[0] for t in con.execute("SHOW TABLES;").fetchall())
    remaining = [t for t in tables_to_drop if t in existing_tables]
    assert not remaining, f"***ERROR*** Tables not dropped: {remaining}"
    logging.info(f"All tables dropped: {tables_to_drop}")

def create_all_tables(con):
    logging.info("Creating all tables")
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
    create_player_rolling_stats(con)
    
    result = set(t[0] for t in con.execute("SHOW TABLES;").fetchall())
    logging.info(f"Resulting tables: {result}")
    assert expected_tables.issubset(result), f"Missing tables: {expected_tables - result}"
    
    logging.info(f"All tables created: {expected_tables}")

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
    team VARCHAR,
    kills INTEGER,
    deaths INTEGER,
    assists INTEGER,
    denies INTEGER,
    net_worth BIGINT,
    PRIMARY KEY (account_id, match_id)
    )
    """)

def create_player_trends_table(con):
    con.execute("""
    CREATE TABLE player_trends (
    account_id BIGINT,
    p_average_kills FLOAT,
    p_average_deaths FLOAT,
    p_avg_kd FLOAT,
    p_total_matches BIGINT,
    p_win_rate FLOAT,
                
    -- Player hero trends
    p_h_total_matches BIGINT,    
    p_v_h_pick_pct FLOAT,
    p_v_h_win_pct FLOAT,
    p_v_h_kd_pct FLOAT,

    -- Win streaks
    win_streaks_avg FLOAT,
    win_streaks_2plus INTEGER,
    win_streaks_3plus INTEGER,
    win_streaks_4plus INTEGER,
    win_streaks_5plus INTEGER,

    -- Loss streaks
    loss_streaks_avg FLOAT,
    loss_streaks_2plus INTEGER,
    loss_streaks_3plus INTEGER,
    loss_streaks_4plus INTEGER,
    loss_streaks_5plus INTEGER,

    -- Recent win streaks (within 8 hours)
    win_recency_2plus INTEGER,
    win_recency_3plus INTEGER,
    win_recency_4plus INTEGER,
    win_recency_5plus INTEGER,

    -- Recent loss streaks (within 8 hours)
    loss_recency_2plus INTEGER,
    loss_recency_3plus INTEGER,
    loss_recency_4plus INTEGER,
    loss_recency_5plus INTEGER,
                
    p_win_streak_avg FLOAT,
    p_loss_streak_avg FLOAT,
    PRIMARY KEY (account_id)
    )
    """)

def create_player_hero_trends(con):
    con.execute("""
    CREATE TABLE player_hero_trends (
    account_id BIGINT,
    hero_id INTEGER,

    p_h_total_matches BIGINT,
    p_h_pick_pct FLOAT,
    p_h_win_pct_3 FLOAT,
    p_h_win_pct_5 FLOAT,
    p_h_avg_kd FLOAT,
    p_h_average_kills FLOAT,
    p_h_average_deaths FLOAT,
    p_h_average_assists FLOAT,

    trend_start_date DATE,
    trend_end_date DATE,
    trend_window_days INTEGER,
    last_updated TIMESTAMP,

    PRIMARY KEY (account_id, hero_id, trend_start_date, trend_end_date, trend_window_days)
    )
    """)

def create_player_rolling_stats(con):
    con.execute("""
    CREATE TABLE player_rolling_stats (
    account_id BIGINT,
    match_id BIGINT,
    start_time TIMESTAMP,
    p_win_pct_2 FLOAT,
    p_win_pct_3 FLOAT,
    p_win_pct_4 FLOAT,
    p_win_pct_5 FLOAT,
    p_loss_pct_2 FLOAT,
    p_loss_pct_3 FLOAT,
    p_loss_pct_4 FLOAT,
    p_loss_pct_5 FLOAT,
    PRIMARY KEY (account_id, match_id),
    FOREIGN KEY (account_id, match_id) REFERENCES player_matches(account_id, match_id)
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
    logging.info("Resetting all tables")
    drop_all_tables(con)
    create_all_tables(con)

def pull_trend_players_from_db(con):
    """pulls players from player_matches table to trend"""

    query = """
    SELECT DISTINCT account_id
    FROM player_matches
    LIMIT 50
    """
    
    players = con.execute(query).fetchdf()
    print(f"players: {players}")
    logging.info(f"Pulled {len(players)} players to trend")
    return players



if __name__ == "__main__":
    #reset_all_tables(db.con)
    con = db.con
    con.execute(create_player_rolling_stats(con))
    #con.execute("ALTER TABLE player_trends drop COLUMN p_h_pick_per;")
    #con.execute("ALTER TABLE player_matches ADD COLUMN average_kd FLOAT;")

    #create_test_subset(con)