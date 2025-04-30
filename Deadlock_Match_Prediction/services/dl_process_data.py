import pandas as pd
import numpy as np
from services.utility_functions import to_csv
from services.dl_fetch_data import fetch_player_match_history


# gets @total_matches, @hero_pickrate, @hero_win_percentage from pd.DataFrame(hero_stats)
def calculate_hero_stats(m_hero_df):
    #returns total matches
    def get_total_matches(df):
        total_matches = df['matches'].sum()
        total_matches = total_matches/12
        #print(f"sum matches = :{total_matches}")
        return total_matches
    
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
        return df

    sum_matches = get_total_matches(m_hero_df) 
    m_hero_df = get_hero_pickrate(sum_matches, m_hero_df)
    m_hero_df = hero_percentages(m_hero_df)
    return m_hero_df

def insert_dataframes(con, match_df=None, player_df=None, trends_df=None, hero_trends_df=None):
    """
    Inserts available DataFrames into their corresponding DuckDB tables.
    Only non-None DataFrames are inserted.
    
    Parameters:
    - con: active DuckDB connection
    - match_df: DataFrame for 'matches' table
    - player_df: DataFrame for 'player_matches' table
    - trends_df: DataFrame for 'player_trends' table
    - hero_trends_df: DataFrame for 'hero_trends' table
    """
    #print(f"*DEBUG* - insert_dataframes started!")
    stats = {}
    #print(f"\n\n*Debug*\n match_df headers are: {match_df.head()} and columns are: {match_df.columns.tolist()}")
    if match_df is not None:
        to_csv(match_df, "match_df")
        before = con.execute("SELECT COUNT(*) FROM matches").fetchone()[0]
        con.execute("INSERT OR IGNORE INTO matches SELECT * FROM match_df")
        after = con.execute("SELECT COUNT(*) FROM matches").fetchone()[0]
        stats['matches_inserted'] = after - before
        print(f"\n\nInserted {stats['matches_inserted']} new rows across matches\n\n")

    if player_df is not None:
        to_csv(player_df, "player_df")
        before = con.execute("SELECT COUNT(*) FROM player_matches").fetchone()[0]
        con.execute("INSERT OR IGNORE INTO player_matches SELECT * FROM player_df")
        after = con.execute("SELECT COUNT(*) FROM player_matches").fetchone()[0]
        stats['player_matches_inserted'] = after - before
        print(f"\n\nInserted {stats['player_matches_inserted']} new rows across player_matches\n\n")

    if trends_df is not None:
        to_csv(trends_df, "trends_df")
        before = con.execute("SELECT COUNT(*) FROM player_trends").fetchone()[0]
        con.execute("INSERT OR IGNORE INTO player_trends SELECT * FROM trends_df")
        after = con.execute("SELECT COUNT(*) FROM player_trends").fetchone()[0]
        stats['player_trends_inserted'] = after - before
        print(f"\n\nInserted {stats['player_trends_inserted']} new rows across player_trends\n\n")

    if hero_trends_df is not None:
        to_csv(hero_trends_df,"hero_trends_df")
        before = con.execute("SELECT COUNT(*) FROM hero_trends").fetchone()[0]
        con.execute("INSERT OR IGNORE INTO hero_trends SELECT * FROM hero_trends_df")
        after = con.execute("SELECT COUNT(*) FROM hero_trends").fetchone()[0]
        stats['hero_trends'] = after - before
        print(f"\n\nInserted {stats['hero_trends']} new rows across player_trends\n\n")
        
    total_inserted = sum(stats.values())
    print(f"Inserted {total_inserted} new rows across tables: {stats}")

def split_dfs_for_insertion(con, full_df):
    """
    Reviews if columns in full_df match any of the sets in schema_map, then splits into a df.

    match = can insert to matches table
    player = can insert into match_player table
    trend = player_trends table
    """

    schema_map = {   
        "match_columns" : [
        'match_id', 'start_time', 'game_mode', 'match_mode',
        'match_duration_s', 'objectives_mask_team0', 'objectives_mask_team1', 'match_result'
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
        'h_win_pct_3', 'h_win_pct_5', 'h_streak_3', 'h_streak_5'
    ]
    }

    segment = {}
    for name, required_cols in schema_map.items():
        if all(col in full_df.columns for col in required_cols):
            segment[name] = full_df[required_cols]
    return segment

def normalize_match_json(json):
    df = pd.json_normalize(json, record_path="players", meta=["match_id", "winning_team"])
    return df

def match_data_outcome_add(df)-> pd.DataFrame:
    """compares winning team to team, calcs "won" bool
    
    requires normalized match_player data
    i.e. example[winning_team: team0, team: team1] = example[won: False]"""
    #print(f"Match_data_outcome_add =\n\n\n {df}")
    df["won"] = df["team"] == df["winning_team"]
    return df

def get_all_histories(matches_df):
    all_matches_player_histories = pd.DataFrame()
    """for all matches, get players_match_histories and stats"""
    #print(f"\n\n get_all_histories, matches_df\n\n {matches_df}")
    for idx,match_row in matches_df.iterrows():
        """pulls match history of all players in match"""
        #print(f"****\n\n match_row type = {type(match_row)} =\n\n{match_row}")
        match_players_histories = get_player_match_history(match_row)
        all_matches_player_histories = pd.concat([all_matches_player_histories, match_players_histories], ignore_index=True)
    
    all_matches_player_histories['start_time'] = pd.to_datetime(all_matches_player_histories['start_time'], unit='s')
    return all_matches_player_histories

def old_get_players_match_histories(match_players_df)->pd.DataFrame:
    """
    creates statistics for player based on match history
    """

    player_count = 0
    print(f"match_players_df  get_players_match_history =\n\n\n{match_players_df} ")
    
    print(f"\n**pulling account_id from data m_id = {'match_id'}**\n")
    
    for p_id in match_players_df['account_id']:
        player_count +=1
        p_m_history = get_player_match_history(p_id)
        match_players_histories = pd.concat([match_players_histories,p_m_history],ignore_index=True)
    
    return match_players_histories

def get_player_match_history(match):
    """for player, calculate player stats."""
    p_id = match['account_id']
    #print(f"\n\n***DEBUG GET_PLAYER_MATCH_STATS - p_id = {p_id}")
    player_history = fetch_player_match_history(p_id)
    #print(f"\n\n***DEBUG GET_PLAYER_MATCH_STATS match_history_outcome_add - player_history =\n\n {player_history}")
    player_history = match_history_outcome_add(player_history)
    #print(f"\n\n***DEBUG GET_PLAYER_MATCH_STATS win_loss_history - player_history =\n\n {player_history}")
    player_history = win_loss_history(player_history)
    #print(f"\n\n***DEBUG GET_PLAYER_MATCH_STATS  calculate_player_stats- player_history =\n\n {player_history}")
    player_history = calculate_player_stats(player_history)
    #print(f"\n\n***DEBUG GET_PLAYER_MATCH_STATS - done =\n\n {player_history}")
    return player_history

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

def old_filter_match_data(df)-> pd.DataFrame:
    return df[MATCH_FILTERS]

def old_filter_account_data(df)-> pd.DataFrame:
    return df[PLAYER_FILTERS]

def old_filter_player_hero_data(df):
    ## p_hero total games played as p_hero_total_games (int)
    ## p_hero w/l over last 1 months as p_hero_1m_wl (int)
    ## p_hero w/l over last week as p_hero_1w_wl (int)

    return

def old_calculate_player_hero_stats(df):
    df['win_percentage'] = (df['wins'].replace(0,1)/df['matches_played'].replcae(0,1)*100).round(2)
    df['p_h_total_matches'] = df['match_id'].count()

    return

#Extracts pd.DataFrame(players) from pd.DataFrame(matches), appends match_id to each player row
def old_split_players_and_matches(df)-> pd.DataFrame:
    account_data = []

    #Extract nested dictionary from df
    for _, row in df.iterrows():
        match_id = row['match_id']
        players = pd.DataFrame(row['players'])
        if not players:
            continue ## *** Good logging catch**
        players['match_id'] = match_id
        account_data.append(players)
    players = pd.concat(account_data, ignore_index=True)

    return df, players
