import duckdb
import pandas as pd
import logging
import services.function_tools as u
import services.database_functions as dbf
import function_tests as ft
from data import db

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

def save_bulk_matches_to_db(
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

def compute_hero_metrics(
        hero_trends: pd.DataFrame) -> pd.DataFrame:
    """Calculates additional hero_trend statistics."""
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

def build_hero_trends(
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
    
    hero_trends['trend_start_date'] = pd.to_datetime(
        hero_trends["trend_start_date"],unit="s",utc=True
    )
    hero_trends['trend_end_date'] = pd.to_datetime(
        hero_trends['trend_end_date'],unit="s",utc=True
    )
    hero_trends['trend_date'] = pd.to_datetime(
        hero_trends['trend_date'],unit="s",utc=True
    )

    # calculate pick rate, win rate, average kills, deaths, assists, and K/D ratio
    hero_trends = compute_hero_metrics(hero_trends)

    return hero_trends

def save_hero_trends_to_db(
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
    
    #check hero_trends
    if missing:
        logging.error(
            "Hero Trends DataFrame schema mismatch: "
            f" missing={missing or None}, extra={extra or None}"
        )
        raise ValueError("hero_trends columns do not align with matches schema, missing",missing)
    
    if extra:
        logging.info(f"Extra columns in hero_trends, extras: {extra}")

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

def calculate_won_column(df)->pd.DataFrame:
    """Calculates the 'won' column based on 'match_result' and 'player_team'."""
    logging.info("Calculating 'won' column")
    df['won'] = df['match_result'] == df['player_team']
    return df

def computer_player_stats(
        player_history: pd.DataFrame) -> pd.DataFrame:
    """Calculates additional hero_trend statistics."""
    
    p_stats = pd.Series()
    match_total = player_history['match_id'].nunique()
    total_kills = player_history['player_kills'].sum()
    total_deaths = player_history['player_deaths'].sum()
    logging.debug("stats for player %s", player_history['account_id'])
    logging.debug("total_kills: %s", total_kills)
    logging.debug("total_deaths: %s", total_deaths)
    logging.debug("match_total: %s", match_total)
    p_stats['account_id'] = player_history['account_id'].iloc[0]
    player_history['won']= (
        player_history['player_team'] == player_history['match_result']
        )
    p_stats['p_win_rate'] = (
        player_history['won'].sum()
        /match_total*100).round(2)
    p_stats['average_kills'] = (
        total_kills/match_total).round(2)
    p_stats['average_deaths'] = (
        total_deaths/match_total).round(2)
    p_stats['average_kd'] = (total_kills/total_deaths).round(2)
    p_stats['p_total_matches'] = match_total

    return p_stats

def count_player_streaks(player_history: pd.DataFrame)->pd.DataFrame:
    """counts player streaks from match history"""
    
    logging.info("Counting player streaks")
    # Check if the player history is empty
    if player_history.empty:
        logging.warning("Player history is empty")
        return None
    
    #calculate win from match_result and player_team
    player_history = player_history.sort_values(by=['start_time'])
    #creates unique identifier for each streak
    player_history['won_int'] = player_history['won'].astype(int)
    player_history['streak_change'] = (player_history['won'] != player_history['won']
                                    .shift()).astype(int)
    player_history['streak_id'] = player_history['streak_change'].cumsum()

    #group by streak_id and win to count streaks
    streaks = player_history.groupby('streak_id').agg(
        streak_len=('won', 'size'),
        won=('won', 'first'))

    # Win streak stats
    win_streaks = streaks[streaks['won'] == True]['streak_len']
    win_avg = win_streaks.mean()
    win_2 = (win_streaks >= 2).sum()
    win_3 = (win_streaks >= 3).sum()
    win_4 = (win_streaks >= 4).sum()
    win_5 = (win_streaks >= 5).sum()

     # Loss streak stats
    loss_streaks = streaks[streaks['won'] == False]['streak_len']
    loss_avg = loss_streaks.mean()
    loss_2 = (loss_streaks >= 2).sum()
    loss_3 = (loss_streaks >= 3).sum()
    loss_4 = (loss_streaks >= 4).sum()
    loss_5 = (loss_streaks >= 5).sum()

    player_history = player_history.drop(columns=['won_int', 'streak_change', 'streak_id'])
    player_history = player_history.assign(
        win_streaks_2plus=win_2,
        win_streaks_3plus=win_3,
        win_streaks_4plus=win_4,
        win_streaks_5plus=win_5,
        loss_streaks_2plus=loss_2,
        loss_streaks_3plus=loss_3,
        loss_streaks_4plus=loss_4,
        loss_streaks_5plus=loss_5,
        win_streaks_avg=win_avg,
        loss_streaks_avg=loss_avg
        )
    return player_history

def compute_player_rolling_stats(player_history: pd.DataFrame)->pd.DataFrame:
    """Calculates player streak trends from player match history"""
    logging.info("Calculating player streak trends.")
    # Check if the player history is empty
    if player_history.empty:
        logging.warning("Player history is empty")
        return None

    player_history = player_history.sort_values(by='start_time', ascending=False)
    
    # calculates rolling player win percentage
    for w in range(2, 7):
        player_history[f'p_win_pct_{w}'] = player_history['won'].rolling(window=w).mean()*100
        player_history[f'p_loss_pct_{w}'] = (1 - player_history['won'].rolling(window=w).mean())*100
    logging.debug(f"Calculated player streak trends for player {player_history['account_id'].iloc[0]}")
    
    return player_history

def compute_player_stats(player_match_history) -> pd.DataFrame:
    """Calculates rolling win/loss % stats and inserts to player_rolling_stats table."""
    if player_match_history.empty:
        logging.warning("Player history DataFrame is empty.")
        return pd.DataFrame()
    
    # calculte player trend stats
    try:
        player_stats = computer_player_stats(player_match_history)
        if player_stats is None or player_stats.empty:
            logging.warning(f"No player stats calculated for player {player_match_history['account_id']}")

    except Exception as e:
        logging.error(f"Error calculating player stats for {player_match_history['account_id']}: {e}")       

    #calculate player streak counts and averages
    try:
        player_trends = count_player_streaks(player_match_history)
        if player_trends is None or player_trends is None:
            logging.warning(f"No player streaks calculated for player {player_match_history['account_id']}")

    except Exception as e:
        logging.error(f"Error calculating player streaks for {player_match_history['account_id']}: {e}")

    #combine player stats and trends
    player_trends_and_streaks = player_trends.merge(
        player_stats, on='account_id', how='left')

    return player_trends_and_streaks

def process_player_hero_stats(player_stats, hero_trends) -> pd.DataFrame:
    """Calculates player_hero trends and inserts to player_hero_trends table."""
    if player_stats.empty or hero_trends.empty:
        logging.warning("Player history or hero trends DataFrame is empty.")
        return pd.DataFrame()
    
    #calculate player_hero trends
    try:
        player_stats['p_v_h_kd_pct'] = player_stats['p_avg_kd'] - hero_trends['average_kd']
        player_stats['p_v_h_pick_pct'] = player_stats['p_h_total_matches'] - hero_trends['matches']
        player_stats['p_v_h_win_pct'] = player_stats['p_win_rate'] - hero_trends['win_rate']    
    except Exception as e:
        logging.error(f"Error calculating player_hero trends for {player_stats['account_id']}: {e}")
    
    return player_stats

def save_player_trends_to_db(
        player_trends: pd.DataFrame) -> None:
    """Loads hero trends data into the database."""
    
    # expected schema
    PLAYER_TRENDS_COLUMNS = {
        "account_id",
        "p_average_kills",
        "p_average_deaths",
        "p_avg_kd",
        "p_total_matches",
        "p_win_rate",
        "p_v_h_pick_pct",
        "p_v_h_win_pct",
        "p_v_h_kd_pct",
        "win_streaks_avg",
        "win_streaks_2plus",
        "win_streaks_3plus",
        "win_streaks_4plus",
        "win_streaks_5plus",
        "loss_streaks_avg",
        "loss_streaks_2plus",
        "loss_streaks_3plus",
        "loss_streaks_4plus",
        "loss_streaks_5plus",
        "p_win_streak_avg",
        "p_loss_streak_avg",
    }

    missing = PLAYER_TRENDS_COLUMNS - set(player_trends.columns)
    extra   = set(player_trends.columns) - PLAYER_TRENDS_COLUMNS
    
    #check player_trends
    if missing:
        logging.error(
            "Player Trends DataFrame schema mismatch: "
            f" missing={missing or None}, extra={extra or None}"
        )
        raise ValueError("player_trends columns do not align with matches schema, missing",missing)
    
    if extra:
        logging.info(f"Extra columns in player_trends, extras: {extra}")

    try:
        with duckdb.connect(database=db.DB_PATH) as con:
            logging.info("connected to database: %s", db.DB_PATH)
            con.register("df_player_trends", player_trends)
            before = con.execute("SELECT COUNT(*) FROM player_trends").fetchone()[0]
            con.execute("""
            INSERT OR IGNORE INTO player_trends (
                account_id, p_average_kills,p_average_deaths,
                p_avg_kd, p_total_matches, p_win_rate,
                p_v_h_pick_pct, p_v_h_win_pct, p_v_h_kd_pct,
                win_streaks_avg, win_streaks_2plus, win_streaks_3plus,
                win_streaks_4plus, win_streaks_5plus,
                loss_streaks_avg, loss_streaks_2plus, loss_streaks_3plus,
                loss_streaks_4plus, loss_streaks_5plus,
                p_win_streak_avg, p_loss_streak_avg
            )              
            SELECT
                account_id, p_average_kills,p_average_deaths,
                p_avg_kd, p_total_matches, p_win_rate,
                p_v_h_pick_pct, p_v_h_win_pct, p_v_h_kd_pct,
                win_streaks_avg, win_streaks_2plus, win_streaks_3plus,
                win_streaks_4plus, win_streaks_5plus,
                loss_streaks_avg, loss_streaks_2plus, loss_streaks_3plus,
                loss_streaks_4plus, loss_streaks_5plus,
                p_win_streak_avg, p_loss_streak_avg
            FROM df_player_trends
            """)
            after = con.execute("SELECT COUNT(*) FROM player_trends").fetchone()[0]
            logging.info(f"Bulk load complete. rows inserted: {after}-{before}")
            
    except Exception:
        logging.exception("Failed to load bulk match data")
        raise
    
def save_player_rolling_stats_to_db(
        roll_stats: pd.DataFrame) -> None:
    """Loads player rolling stats data into the database."""
    
    # expected schema
    ROLL_STATS_COLUMNS = {
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
    missing = ROLL_STATS_COLUMNS - set(roll_stats.columns)
    extra   = set(roll_stats.columns) - ROLL_STATS_COLUMNS
    
    #check roll_stats
    if missing:
        logging.error(
            "roll_stats df schema mismatch: "
            f" missing={missing or None}, extra={extra or None}"
        )
        raise ValueError("roll_stats columns do not align with matches schema, missing",missing)
    
    if extra:
        logging.info(f"Extra columns in roll_stats, extras: {extra}")

    try:
        with duckdb.connect(database=db.DB_PATH) as con:
            logging.info("connected to database: %s", db.DB_PATH)
            con.register("df_roll_stats", roll_stats)
            before = con.execute("SELECT COUNT(*) FROM roll_stats").fetchone()[0]
            con.execute("""
            INSERT OR IGNORE INTO roll_stats (
            account_id, match_id, start_time,
            p_win_pct_2, p_win_pct_3, p_win_pct_4, p_win_pct_5,
            p_loss_pct_2, p_loss_pct_3, p_loss_pct_4, p_loss_pct_5
            )
            SELECT
                account_id, match_id, start_time,
                p_win_pct_2, p_win_pct_3, p_win_pct_4, p_win_pct_5,
                p_loss_pct_2, p_loss_pct_3, p_loss_pct_4, p_loss_pct_5
            FROM df_roll_stats
            """)
            after = con.execute("SELECT COUNT(*) FROM roll_stats").fetchone()[0]
            logging.info(f"Bulk load complete. rows inserted: {after}-{before}")
            
    except Exception:
        logging.exception("Failed to load bulk match data")
        raise

