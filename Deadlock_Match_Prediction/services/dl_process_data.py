import pandas as pd
import numpy as np
import duckdb
import time
from services.utility_functions import to_csv
from services.dl_fetch_data import fetch_player_match_history


# gets @total_matches, @hero_pickrate, @hero_win_percentage from pd.DataFrame(hero_stats)
def calculate_hero_stats(m_hero_df):
    #returns total matches
    def get_total_matches(df):
        total_matches = df['matches'].sum()
        #print(f"sum matches = :{total_matches}")
        return total_matches/12
    
    #adds pickrate to df
    def get_hero_pickrate(matches, df):
        df['pick_rate'] = ((df['matches'])/matches*100).round(2)
        return df

    #adds win_percentage to df
    def hero_percentages(df)-> pd.DataFrame:
        df['win_rate'] = (df['wins'].replace(0,1)/df['matches'].replace(0,1)*100).round(2)
        df['average_kills'] = (df['total_kills'].replace(0,1)/df['matches'].replace(0,1)*100).round(2)
        df['average_deaths'] = (df['total_deaths'].replace(0,1)/df['matches'].replace(0,1)*100).round(2)
        df['average_assists'] = (df['total_assists'].replace(0,1)/df['matches'].replace(0,1)*100).round(2)
        df['average_kd'] = df['total_kills'].replace(0,1)/df['total_deaths'].replace(0,1).round(2)
        return df

    sum_matches = get_total_matches(m_hero_df) 
    m_hero_df = get_hero_pickrate(sum_matches, m_hero_df)
    m_hero_df = hero_percentages(m_hero_df)
    return m_hero_df

def batch_get_players_from_matches(con, df_matches, batch_size=500):
    """batch feeds player_ids for player_match_histories"""

    all_ids = df_matches['account_id'].tolist()

    for i in range(0, len(all_ids), batch_size):
        print(f"**INFO** Processing chunk {i}-{i + batch_size} of {len(all_ids)}")
        chunk_ids = all_ids[i:i + batch_size]
        chunk_df = df_matches[df_matches['account_id'].isin(chunk_ids)]

        try:
            df_player_hist = get_players_from_matches(chunk_df)
            
            split_df = split_dfs_for_insertion(con, df_player_hist)
            match_df = split_df.get('match_columns')
            player_df = split_df.get('player_columns')
            trends_df = split_df.get('trend_columns')

            if any(df is not None and not df.empty 
                   for df in [match_df, player_df, trends_df]):
                insert_dataframes(con, match_df, player_df, trends_df)
            else:
                print(f"\n\n**ERROR**Chunk {i}-{i + batch_size} produced no valid data***")
        except Exception as e:
            print(f"\n\n**ERROR** on chunk {i}-{i + batch_size}: {e}")

def insert_dataframes(con, match_df=None, player_df=None, trends_df=None, hero_trends_df=None):
    """
    Inserts available DataFrames into their corresponding DuckDB tables.
    Only non-None DataFrames are inserted.
    
    Parameters: #Run through split_dfs_for_insertion first.
    - con: active DuckDB connection
    - match_df: DataFrame for 'matches' table
    - player_df: DataFrame for 'player_matches' table
    - trends_df: DataFrame for 'player_trends' table
    - hero_trends_df: DataFrame for 'hero_trends' table
    """
    #print(f"*DEBUG* - insert_dataframes started!")

    #print(f"\n\n*Debug*\n match_df headers are: {match_df.head()} and columns are: {match_df.columns.tolist()}")
    stats = {}

    if match_df is not None:
        cols = ', '.join(match_df.columns)
        query = f"INSERT OR IGNORE INTO matches ({cols}) SELECT * FROM match_df"
        to_csv(match_df, "match_df")
        before = con.execute("SELECT COUNT(*) FROM matches").fetchone()[0]
        con.execute(query)
        after = con.execute("SELECT COUNT(*) FROM matches").fetchone()[0]
        stats['matches_inserted'] = after - before
        print(f"\n**INFO** Inserted {stats['matches_inserted']} new rows into matches")

    if player_df is not None:
        cols = ', '.join(player_df.columns)
        query = f"INSERT OR IGNORE INTO player_matches ({cols}) SELECT * FROM player_df"
        to_csv(player_df, "player_df")
        before = con.execute("SELECT COUNT(*) FROM player_matches").fetchone()[0]
        con.execute(query)
        after = con.execute("SELECT COUNT(*) FROM player_matches").fetchone()[0]
        stats['player_inserted'] = after - before
        print(f"\n**INFO**Inserted {stats['player_inserted']} new rows into player_matches")

    if trends_df is not None:
        cols = ', '.join(trends_df.columns)
        query = f"INSERT OR IGNORE INTO player_trends ({cols}) SELECT * FROM trends_df"
        to_csv(trends_df, "trends_df")
        before = con.execute("SELECT COUNT(*) FROM player_trends").fetchone()[0]
        con.execute(query)
        after = con.execute("SELECT COUNT(*) FROM player_trends").fetchone()[0]
        stats['trends_inserted'] = after - before
        print(f"\n**INFO**Inserted {stats['trends_inserted']} new rows into player_trends")

    if hero_trends_df is not None:
        cols = ', '.join(hero_trends_df.columns)
        query = f"INSERT OR IGNORE INTO hero_trends ({cols}) SELECT * FROM hero_trends_df"
        to_csv(hero_trends_df, "hero_trends_df")
        before = con.execute("SELECT COUNT(*) FROM hero_trends").fetchone()[0]
        con.execute(query)
        after = con.execute("SELECT COUNT(*) FROM hero_trends").fetchone()[0]
        stats['hero_trends_inserted'] = after - before
        print(f"\n**INFO**Inserted {stats['hero_trends_inserted']} new rows into hero_trends")
        
    total_inserted = sum(stats.values())
    print(f"\n**INFO**Inserted {total_inserted} new rows across tables: {stats}")

def split_dfs_for_insertion(con, full_df):
    """
    Reviews if columns in full_df match any of the sets in schema_map, then splits into a df.

    match = can insert to matches table
    player = can insert into match_player table
    trend = player_trends table
    """

    schema_map = {   
        "match_columns" : [
        'match_id', 'account_id','start_time', 'game_mode', 'match_mode',
        'duration_s', 'winning_team'
    ],

    "player_columns" : [
        'account_id', 'match_id', 'hero_id', 'hero_level', 'player_team',
        'player_kills', 'player_deaths', 'player_assists', 'denies',
        'net_worth', 'last_hits', 'team_abandoned', 'abandoned_time_s', 'won',
        'p_total_kills','p_total_deaths','p_avg_kd','p_total_matches','p_h_total_matches',
        'p_h_pick_pct'
    ],

    "trend_columns" : [
        'account_id', 'match_id', 'hero_id',
        'p_win_pct_3', 'p_win_pct_5', 'p_streak_3', 'p_streak_5',
        'h_win_pct_3', 'h_win_pct_5', 'h_streak_3', 'h_streak_5',
        
    ]
    }
    #print(f"\n\n*Debug* in split_dfs_for_insertion, full_df columns are: {list(full_df.columns)}")
    split_dfs = {}
    
    for table_name, required_cols in schema_map.items():
        # Check if all required columns exist
        missing_cols = [col for col in required_cols if col not in full_df.columns]
        
        if missing_cols:
            print(f"Warning: Skipping '{table_name}' split - missing columns: {missing_cols}")
            continue
        
        # Create a copy with only the needed columns
        split_dfs[table_name] = full_df[required_cols].copy()
        print(f"\n\n**INFO** Created '{table_name}' DataFrame with {len(split_dfs[table_name])} rows and {len(required_cols)} columns")
    
    return split_dfs

def normalize_match_json(json):
    return pd.json_normalize(json, record_path="players", meta=["match_id", "winning_team","start_time","game_mode","match_mode","duration_s"])

def match_data_outcome_add(df)-> pd.DataFrame:
    """compares winning team to team, calcs "won" bool
    
    requires normalized match_player data
    i.e. example[winning_team: team0, team: team1] = example[won: False]"""
    #print(f"Match_data_outcome_add =\n\n\n {df}")
    df["won"] = df["team"] == df["winning_team"]
    return df

def get_distinct_matches(con)->pd.DataFrame:
    con = duckdb.connect("c:/Code/Local Code/Deadlock Database/Deadlock_Match_Prediction/deadlock.db")
    match_account_ids = con.execute("SELECT DISTINCT m.account_id FROM matches AS m LEFT JOIN player_matches AS mp USING(account_id) WHERE mp.account_id IS NULL;").fetchdf()
    print(f"\n\n*INFO* count of distinct account_ids = {len(match_account_ids)}")
    return match_account_ids

def get_players_from_matches(match_players_df=pd.DataFrame)->pd.DataFrame:
    """
    from normalized list of matches, cycles p_id
    """
    id_count = len(match_players_df)
    print(f"\n*INFO* Starting p_m_history fetches from matches unique IDS total: {id_count}\n") 
    count_calculated=0
    p_m_history_chunk = []

    for p_id in match_players_df['account_id']:
        count_calculated+=1
        attempts = 0
        success = False

        while attempts < 5 and not success:
            try:
                p_m_history = calculate_p_m_history_stats(p_id)  # replace with your actual function
                success = True
            except Exception as e:
                print(f"\n**WARNING** Fetch failed for {p_id}, attempt {attempts+1}/5: {e}")
                attempts += 1
                time.sleep(10)
        if not success:
            print(f"\n\n**ERROR**Failed to fetch {p_id} after 5 attempts.")
        
        p_m_history_chunk.append(p_m_history)

        if count_calculated %10 == 0:
            print(f"\n\n*INFO* current count: {count_calculated} of {id_count}")
        #print(f"*DEBUG* p_m_history_chunk item type = {type(p_m_history_chunk[0])} data = \n\n {p_m_history_chunk[0]}")
    
    match_players_histories = pd.concat(p_m_history_chunk, ignore_index=True)

    return match_players_histories

def calculate_p_m_history_stats(p_id):
    """for player, calculate player stats."""

    #print(f"\n\n***DEBUG GET_PLAYER_MATCH_STATS - p_id = {p_id}")
    player_m_history = fetch_player_match_history(p_id)
    #print(f"\n\n***DEBUG GET_PLAYER_MATCH_STATS match_history_outcome_add - player_m_history =\n\n {player_m_history}")
    player_m_history = match_history_outcome_add(player_m_history)
    #print(f"\n\n***DEBUG GET_PLAYER_MATCH_STATS win_loss_history - player_m_history =\n\n {player_m_history}")
    player_m_history = win_loss_history(player_m_history)
    #print(f"\n\n***DEBUG GET_PLAYER_MATCH_STATS  calculate_player_stats- player_m_history =\n\n {player_m_history}")
    player_m_history = calculate_player_stats(player_m_history)
    #print(f"\n\n***DEBUG GET_PLAYER_MATCH_STATS - done =\n\n {player_m_history}")
    return player_m_history

def match_history_outcome_add(df)-> pd.DataFrame:
    """calculates and adds which matches the player won in df, adds as new column"""
    df["won"] = df["player_team"] == df["match_result"]
    return df

def calculate_player_stats(df)-> pd.DataFrame:
    p_kills_total = df['player_kills'].sum()
    df['p_total_kills'] = p_kills_total
    p_deathes_total = df['player_deaths'].sum()
    df["p_total_deaths"] = p_deathes_total
    df["p_avg_kd"] = df["p_total_kills"].mean()/df["p_total_deaths"].mean()
    
    total_matches = df["match_id"].count()
    df["p_total_matches"] = total_matches

    df["p_h_total_matches"] = df.groupby('hero_id')['match_id'].transform('count')
    df['p_h_pick_pct'] = ((df['p_h_total_matches'] / df['p_total_matches'])*100).round(2)

    return df

def win_loss_history(df: pd.DataFrame) -> pd.DataFrame:
    """
    Given a DataFrame with:
      - a boolean 'won' column
      - a 'hero_id' column
    returns a new DataFrame with:
      - p_win_pct_3, p_win_pct_5: player rolling win-% (3/5 matches)
      - p_streak_3, p_streak_5: player streak labels
      - h_win_pct_3, h_win_pct_5: hero-specific rolling win-% (3/5 matches)
      - h_streak_3, h_streak_5: hero-specific streak labels
    """
    df = df.copy()
    df = df.sort_values(by=['account_id', 'start_time'])
    # --- player-level rolling win-% ---
    df['p_win_pct_3'] = df.groupby('account_id')['won'].transform(
        lambda x: x.rolling(window=3, min_periods=3).mean() * 100)
    df['p_win_pct_5'] = df.groupby('account_id')['won'].transform(
        lambda x: x.rolling(window=5, min_periods=5).mean() * 100)

    # player streak labels
    labels3 = ['major_loss', 'loss_streak', 'win_streak', 'major_win']
    conds3_p = [
        df['p_win_pct_3'] ==   0,
        df['p_win_pct_3'] ==  33.33333333333333,
        df['p_win_pct_3'] ==  66.66666666666666,
        df['p_win_pct_3'] == 100
    ]
    df['p_streak_3'] = np.select(conds3_p, labels3, default="insufficient_data")

    labels5 = ['major_loss', 'loss_streak', 'neutral',
               'win_streak', 'strong_win', 'major_win']
    conds5_p = [
        df['p_win_pct_5'] ==    0,
        df['p_win_pct_5'] ==   20,
        df['p_win_pct_5'] ==   40,
        df['p_win_pct_5'] ==   60,
        df['p_win_pct_5'] ==   80,
        df['p_win_pct_5'] ==  100
    ]
    df['p_streak_5'] = np.select(conds5_p, labels5, default="insufficient_data")

    df = df.sort_values(by=['hero_id', 'start_time'])
    
    # --- hero-level rolling win-% (per hero_id group) ---
    df['h_win_pct_3'] = (
        df.groupby(['account_id','hero_id'])['won']
        .transform(lambda x: x.rolling(window=3, min_periods=3).mean() * 100))
    df['h_win_pct_5'] = (
        df.groupby(['account_id','hero_id'])['won']
        .transform(lambda x: x.rolling(window=5, min_periods=5).mean() * 100))

    # hero streak labels
    conds3_h = [
        df['h_win_pct_3'] ==   0,
        df['h_win_pct_3'] ==  33.33333333333333,
        df['h_win_pct_3'] ==  66.66666666666666,
        df['h_win_pct_3'] == 100
    ]
    df['h_streak_3'] = np.select(conds3_h, labels3, default="insufficient_data")

    conds5_h = [
        df['h_win_pct_5'] ==    0,
        df['h_win_pct_5'] ==   20,
        df['h_win_pct_5'] ==   40,
        df['h_win_pct_5'] ==   60,
        df['h_win_pct_5'] ==   80,
        df['h_win_pct_5'] ==  100
    ]
    df['h_streak_5'] = np.select(conds5_h, labels5, default="insufficient_data")
    df = df.sort_values(by=['account_id', 'start_time'])
    return df

def get_distinct_incomplete_matches(con) -> pd.DataFrame:
    """
    Returns every distinct account_id from any match
    that does NOT have exactly 12 players recorded.
    """
    query = con.execute("""
            WITH incomplete_matches AS (
            SELECT
                match_id
            FROM player_matches
            GROUP BY match_id
            HAVING COUNT(account_id) < 12
            )

            SELECT 
            m.account_id
            FROM matches m
            JOIN incomplete_matches im ON m.match_id = im.match_id
            WHERE NOT EXISTS (
            SELECT 1 
            FROM player_matches pm 
            WHERE pm.match_id = m.match_id 
            AND pm.account_id = m.account_id
                        limit 500
            )
            """).fetchall()
    df = con.execute(query).fetchdf()
    print(f"*INFO* found {len(df)} distinct account_ids from incomplete matches")
    return df