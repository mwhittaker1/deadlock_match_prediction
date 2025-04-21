import pandas as pd
from config import MATCH_FILTERS, PLAYER_FILTERS



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
    def hero_win_percentage(df):
        df['win_percentage'] = (df['wins'].replace(0,1)/df['matches'].replace(0,1)*100).round(2)
        return df

    sum_matches = get_total_matches(m_hero_df) 
    m_hero_df = get_hero_pickrate(sum_matches, m_hero_df)
    m_hero_df = hero_win_percentage(m_hero_df)
    return m_hero_df

#Extracts pd.DataFrame(players) from pd.DataFrame(matches), appends match_id to each player row
def split_players_from_matches(df):
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

def filter_match_data(df):
    return df[MATCH_FILTERS]

def filter_account_data(df):
    return df[PLAYER_FILTERS]

def filter_player_hero_data(df):
    ## p_hero total games played as p_hero_total_games (int)
    ## p_hero w/l over last 1 months as p_hero_1m_wl (int)
    ## p_hero w/l over last week as p_hero_1w_wl (int)

    return

def calculate_player_hero_stats(df):
    ## player w/l over last 3 games as player_3g_wl (int)
    ## player w/l over last month as player_1m_wl (int)
    return