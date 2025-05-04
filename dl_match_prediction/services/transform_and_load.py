import duckdb
import pandas as pd
import logging
import dl_match_prediction.services.function_tools as u
from services import db
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
            INSERT INTO matches (
                match_id, start_time, game_mode, match_mode,
                duration_s, winning_team
            )
            SELECT
                match_id, start_time, game_mode, match_mode,
                duration_s, winning_team
            FROM df_matches
            """)
            con.execute("""
            INSERT INTO player_matches (
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