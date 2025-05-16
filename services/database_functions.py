import duckdb
import pandas as pd
import logging

def drop_all_tables(con):
    
    con.execute(f"DROP TABLE IF EXISTS player_rolling_stats;")
    con.execute(f"DROP TABLE IF EXISTS player_hero_trends;")
    con.execute(f"DROP TABLE IF EXISTS player_trends;")
    con.execute(f"DROP TABLE IF EXISTS player_matches;")
    con.execute(f"DROP TABLE IF EXISTS matches;")
    con.execute(f"DROP TABLE IF EXISTS hero_trends;")
    con.execute(f"DROP TABLE IF EXISTS player_matches_history;")

def create_all_tables(con):
    logging.info("Creating all tables")
    expected_tables = {
        "matches",
        "player_matches",
        "player_trends",
        "player_hero_trends",
        "hero_trends",
        "player_rolling_stats"
        }
    
    create_matches_table(con)
    create_player_matches_table(con)
    create_player_trends_table(con)
    create_player_hero_trends(con)
    create_hero_trends_table(con)
    create_player_rolling_stats(con)
    create_player_matches_history(con)
    
    result = set(t[0] for t in con.execute("SHOW TABLES;").fetchall())
    logging.info(f"Resulting tables: {result}")
    assert expected_tables.issubset(result), f"Missing tables: {expected_tables - result}"
    
    logging.info(f"All tables created: {expected_tables}")

# pull matches to use for predictions or for training model
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

# normalized matches for player_match in matches 
# distinct account_id = players to pull match history and trends fo
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
    won INTEGER,
    PRIMARY KEY (account_id, match_id)
    )
    """)

# player history for each distinct account_id in player_matches
# used for trends, rolling stats, and match recency information
def create_player_matches_history(con):
    con.execute("""
    CREATE TABLE player_matches_history (
    account_id BIGINT,
    match_id BIGINT,
    start_time TIMESTAMP,
    hero_id INTEGER,
    team VARCHAR,
    kills INTEGER,
    deaths INTEGER,
    assists INTEGER,
    denies INTEGER,
    net_worth BIGINT,
    won INTEGER,
    prior_win_loss_streak VARCHAR,
    PRIMARY KEY (account_id, match_id)
    )
    """)

# for distinct account_id in player_matches, trends for each player
# trends are for each player for each match in matches.
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
    p_v_h_kd_pct FLOAT,

    -- Win streaks
    win_streaks_2plus INTEGER,
    win_streaks_3plus INTEGER,
    win_streaks_4plus INTEGER,
    win_streaks_5plus INTEGER,

    -- Loss streaks
    loss_streaks_2plus INTEGER,
    loss_streaks_3plus INTEGER,
    loss_streaks_4plus INTEGER,
    loss_streaks_5plus INTEGER,
                
    p_win_streak_avg FLOAT,
    p_loss_streak_avg FLOAT,
    PRIMARY KEY (account_id)
    )
    """)

# for each player, creates player_hero specific trends.
# currently not in use.
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

# for match in player_matches, calculate each matches prior win/loss % for 2-5 matches.
# match 1 = win, match 2 = loss, match 3 == match 4: win_pct_3 = 0.67
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
    prior_win_loss_streak VARCHAR,      
    PRIMARY KEY (account_id, match_id),
    )         
    """)

# hero trends are all inclusive (100 min badge) to match against player stats
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
    """
    
    players = con.execute(query).fetchdf()
    logging.info(f"Pulled {len(players)} players to trend")
    return players

def test_pull_trend_players_from_db(con):
    """pulls players from player_matches table to trend"""
    logging.info(f"Pulling players from player_matches table to trend")
    query = """
    SELECT DISTINCT account_id
    FROM player_matches
    LIMIT 5
    """
    
    players = con.execute(query).fetchdf()
    logging.info(f"Pulled {len(players)} players to trend")
    return players

def pull_hero_trends_from_db(con,trend_window_days,trend_start_date=None,):
    """pulls hero trends from hero_trends table to trend"""
    logging.info(f"Pulling hero trends from db for {trend_window_days} days")
    try: 
        if trend_start_date is None:
            query = f"""
            SELECT *
            FROM hero_trends
            WHERE trend_window_days = {trend_window_days}
            """
        else:
            query = f"""
            SELECT *
            FROM hero_trends
            WHERE trend_start_date = '{trend_start_date}'
            AND trend_window_days = {trend_window_days}
            """
        
    
        heroes = con.execute(query).fetchdf()
        if len(heroes) != 26:
            logging.warning("Query length is not 26 characters, expect 26 heros.")
    
    except Exception as e:
            logging.error(f"Error pulling hero trends from DB: {e}")
            return None
    
    return heroes

def pull_player_match_history_from_db(account_id, con):
    """pulls player match history from player_matches_history table"""
    logging.debug(f"Pulling match history for player from db for: {account_id}")
    try:
        query = f"""
        SELECT *
        FROM player_matches_history
        WHERE account_id = '{account_id}'
        """
        
        player_match_history = con.execute(query).fetchdf()
        if player_match_history.empty:
            logging.warning(f"No match history found for player {account_id}")
            return None
        
    except Exception as e:
        logging.error(f"Error pulling player match history from DB: {e}")
        return None
    
    return player_match_history

if __name__ == "__main__":
    #reset_all_tables(db.con)
    con = duckdb.connect(r"C:\Code\Local Code\deadlock_match_prediction\\data\deadlock.db")
    #con.execute(create_player_rolling_stats(con))
    #con.execute(create_player_matches_history(con))
    con.execute("ALTER TABLE player_matches_history ADD COLUMN prior_win_loss_streak VARCHAR;")
    #con.execute("ALTER TABLE player_matches ADD COLUMN average_kd FLOAT;")

    #create_test_subset(con)