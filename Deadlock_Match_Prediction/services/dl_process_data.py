import pandas as pd
import numpy as np
from services.config import MATCH_FILTERS, PLAYER_FILTERS



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
        df['hero_pickrate'] = ((df['matches'])/matches*100).round(2)
        return df

    #adds win_percentage to df
    def hero_win_percentage(df)-> pd.DataFrame:
        df['win_percentage'] = (df['wins'].replace(0,1)/df['matches'].replace(0,1)*100).round(2)
        return df

    sum_matches = get_total_matches(m_hero_df) 
    m_hero_df = get_hero_pickrate(sum_matches, m_hero_df)
    m_hero_df = hero_win_percentage(m_hero_df)
    return m_hero_df

#Extracts pd.DataFrame(players) from pd.DataFrame(matches), appends match_id to each player row
def split_players_from_matches(df)-> pd.DataFrame:
    match_data = pd.DataFrame()
    account_data = []

    #Extract nested dictionary from df
    for _, row in df.iterrows():
        match_id = row['match_id']
        players = pd.DataFrame(row['players'])
        players['match_id'] = match_id
        account_data.append(players)
    players = pd.concat(account_data, ignore_index=True)


    """if debug ==True:
        print(f"\n\n**SPLIT DATA, PLAYERS**  : \n\n")
        for i, (key) in enumerate(players.items()):
            if i < 5:
                print(f"Key= {key}\n\n")

    #drop nested dictionary from df
    match_data = df.drop(columns=['players'])

    if debug ==True:
        print(f"**\n\nSPLIT DATA, MATCH: \n\n")
        for i, (key) in enumerate(match_data.items()):
            if i < 5:
                print(f"Key= {key}\n")"""
    
    return df, players

def match_data_outcome_add(df,json)-> pd.DataFrame:
    """normalizes match data to compare winning team to player team"""
    df = pd.json_normalize(json, record_path="players", meta=["match_id", "winning_team"])
    df["won"] = df["team"] == df["winning_team"]
    return df

def match_history_outcome_add(df)-> pd.DataFrame:
    """calculates and adds which matches the player won in df, adds as new column"""
    df["won"] = df["player_team"] == df["match_result"]
    return df

def filter_match_data(df)-> pd.DataFrame:
    return df[MATCH_FILTERS]

def filter_account_data(df)-> pd.DataFrame:
    return df[PLAYER_FILTERS]

def filter_player_hero_data(df):
    ## p_hero total games played as p_hero_total_games (int)
    ## p_hero w/l over last 1 months as p_hero_1m_wl (int)
    ## p_hero w/l over last week as p_hero_1w_wl (int)

    return

def calculate_player_hero_stats(df):
    df['win_percentage'] = (df['wins'].replace(0,1)/df['matches_played'].replcae(0,1)*100).round(2)
    return

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