import duckdb
import pandas as pd
import logging
import services.function_tools as u
from data import db
u.setup_logger()
logging.info("Logger initialized.")

def normalize_bulk_matches(
        matches_grouped_by_day: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Normalizes bulk match data into two dataframes: matches and players."""
    logging.info("Normalizing bulk match data")
    matches = []
    players = []
    if not matches_grouped_by_day:
        logging.warning("No match data found — matches_grouped_by_day is empty.")
        return pd.DataFrame(), pd.DataFrame()
    for day_idx, day_matches in enumerate(matches_grouped_by_day): #day = key, match = value
        logging.info(f"Processing day #{day_idx} with {len(day_matches)} matches")
        
        for match in day_matches: # match: day = key: value | match_id: 7432551
            try:
                match_id = match["match_id"]
                start_time = match["start_time"]
                game_mode = match["game_mode"]
                match_mode = match["match_mode"]
                duration_s = match["duration_s"]
                winning_team = match["winning_team"]
            except KeyError as e:
                logging.error(f"Match missing key {e}: {match.get('match_id', 'unknown')}", exc_info=True)
                continue

            # Append to matches list
            matches.append({
                "match_id": match_id, # PK
                "start_time": start_time,
                "game_mode": game_mode,
                "match_mode": match_mode,
                "duration_s": duration_s,
                "winning_team": winning_team
            })
            
            # Append each player to players list
            if "players" not in match or len(match["players"]) != 12:
                logging.error(f"Match {match.get('match_id', 'unknown')} has invalid player count: {len(match.get('players', []))}")
                continue
            for player in match["players"]: # player: match["players"] = key: value | player_id: 1234567
                try:
                    players.append({
                        "account_id": player["account_id"],
                        "match_id": match_id,
                        "team": player["team"],
                        "hero_id": player["hero_id"],
                        "kills": player["kills"],
                        "deaths": player["deaths"],
                        "assists": player["assists"],
                        "denies": player["denies"],
                        "net_worth": player["net_worth"],
                    })
                except KeyError as e:
                    logging.error(f"Player missing key {e}: {player.get('account_id', 'unknown')}", exc_info=True)
                    continue

    # Convert lists to DataFrames
    df_matches = pd.DataFrame(matches)
    df_players = pd.DataFrame(players)
    if not matches:
        logging.warning("No matches appended — matches list is empty.")
    if not players:
        logging.warning("No players appended — players list is empty.")

    return df_matches, df_players

def load_bulk_matches(
        match_df: pd.DataFrame, 
        player_df: pd.DataFrame) -> None:
    """Loads normalized match and player data into the database."""
    # expected schema
    MATCH_COLUMNS = {
        "match_id",
        "start_time",
        "game_mode",
        "match_mode",
        "duration_s",
        "winning_team",
    }
    PLAYER_COLUMNS = {
        "account_id",
        "match_id",
        "hero_id",
        "team",
        "kills",
        "deaths",
        "assists",
        "denies",
        "net_worth",
    }
    missing = MATCH_COLUMNS - set(match_df.columns)
    extra   = set(match_df.columns) - MATCH_COLUMNS
    #check match_df
    if missing:
        logging.error(
            "Match DataFrame schema mismatch: "
            f" missing={missing or None}, extra={extra or None}"
        )
        raise ValueError("match_df columns do not align with matches schema, missing",missing)
    if extra:
        logging.info("Extra columns in match_df, extras:", extra)

    missing = PLAYER_COLUMNS - set(player_df.columns)
    extra   = set(player_df.columns) - PLAYER_COLUMNS
    if missing:
        logging.error(
            "Player DataFrame schema mismatch: "
            f" missing={missing or None}, extra={extra or None}"
        )
        raise ValueError("player_df columns do not align with matches schema, missing",missing)
    if extra:
        logging.info("Extra columns in player_df, extras:", extra)


    try:
        with duckdb.connect(database=db.DB_PATH) as con:
            logging.info("connected to database: %s", db.DB_PATH)
            con.register("df_matches", match_df)
            con.register("df_players", player_df)

            con.execute("""
            INSERT OR IGNORE INTO matches (
                match_id, start_time, game_mode, match_mode,
                duration_s, winning_team
            )
            SELECT
                match_id, start_time, game_mode, match_mode,
                duration_s, winning_team
            FROM df_matches
            """)
            con.execute("""
            INSERT OR IGNORE INTO player_matches (
                account_id, match_id, hero_id, team,
                kills, deaths, assists, denies, net_worth
            )
            SELECT
                account_id, match_id, hero_id, team,
                kills, deaths, assists, denies, net_worth
            FROM df_players
            """)
            logging.info("Bulk load complete.")
    except Exception:
        logging.exception("Failed to load bulk match data")
        raise

def transform_hero_trends(
        trend_range: int,
        hero_trends: pd.DataFrame) -> pd.DataFrame:
    """Transforms hero trends data into a format suitable for loading into the database."""
    
    logging.info("Transforming hero trends data")
    
    # Check if the DataFrame is empty
    if hero_trends.empty:
        logging.warning("Hero trends DataFrame is empty.")
        return pd.DataFrame()

    # Create trend timestamps
    current_time = u.get_unix_time(0)
    trend_start_time = u.get_unix_time(-trend_range)
    trend_end_time = u.get_unix_time(0)
    trend_window_days = f"{trend_range}"

    logging.info(
    f"Setting trend window: start={trend_start_time}, "
    f"end={trend_end_time}, window_days={trend_window_days}"
    )

    hero_trends = hero_trends.assign(
        trend_start_date = trend_start_time,
        trend_end_date = trend_end_time,
        trend_date = current_time,
        trend_window_days=trend_window_days
        )
    
    # calculate pick rate, win rate, average kills, deaths, assists, and K/D ratio
    match_total = hero_trends['matches'].sum()

    hero_trends['pick_rate'] = (
        hero_trends['matches'].replace(0,1)
        /match_total*100).round(2)
    hero_trends['win_rate'] = (
        hero_trends['wins'].replace(0,1)
        /hero_trends['matches'].replace(0,1)*100).round(2)
    hero_trends['average_kills'] = (
        hero_trends['total_kills'].replace(0,1)
        /hero_trends['matches'].replace(0,1)*100).round(2)
    hero_trends['average_deaths'] = (
        hero_trends['total_deaths'].replace(0,1)
        /hero_trends['matches'].replace(0,1)*100).round(2)
    hero_trends['average_assists'] = (
        hero_trends['total_assists'].replace(0,1)
        /hero_trends['matches'].replace(0,1)*100).round(2)
    hero_trends['average_kd'] = (
        hero_trends['total_kills'].replace(0,1)
        /hero_trends['total_deaths'].replace(0,1)).round(2)

    return hero_trends

def load_hero_trends(
        hero_trends: pd.DataFrame) -> None:
    """Loads hero trends data into the database."""
    
    # expected schema
    HERO_TRENDS_COLUMNS = {
        "hero_id",
        "trend_start_date",
        "trend_end_date",
        "trend_date",
        "trend_window_days",
        "pick_rate",
        "win_rate",
        "average_kills",
        "average_deaths",
        "average_assists",
        "average_kd"
    }
    missing = HERO_TRENDS_COLUMNS - set(hero_trends.columns)
    extra   = set(hero_trends.columns) - HERO_TRENDS_COLUMNS
    
    #check match_df
    if missing:
        logging.error(
            "Hero Trends DataFrame schema mismatch: "
            f" missing={missing or None}, extra={extra or None}"
        )
        raise ValueError("hero_trends columns do not align with matches schema, missing",missing)
    if extra:
        logging.info("Extra columns in hero_trends, extras:", extra)

    try:
        with duckdb.connect(database=db.DB_PATH) as con:
            logging.info("connected to database: %s", db.DB_PATH)
            con.register("df_hero_trends", hero_trends)
            before = con.execute("SELECT COUNT(*) FROM hero_trends").fetchone()[0]
            con.execute("""
            INSERT OR IGNORE INTO hero_trends (
                hero_id, trend_start_date, trend_end_date,
                trend_date, trend_window_days,
                pick_rate, win_rate,
                average_kills, average_deaths,
                average_assists, average_kd
            )
            SELECT
                hero_id, trend_start_date, trend_end_date,
                trend_date, trend_window_days,
                pick_rate, win_rate,
                average_kills, average_deaths,
                average_assists, average_kd
            FROM df_hero_trends
            """)
            after = con.execute("SELECT COUNT(*) FROM hero_trends").fetchone()[0]
            logging.info(f"Bulk load complete. rows inserted: {after}-{before}")
    except Exception:
        logging.exception("Failed to load bulk match data")
        raise