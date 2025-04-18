import pandas as pd
import random

from config import MATCH_FILTERS, PLAYER_FILTERS

#receive raw_match_data
def split_data(df):
    match_data = pd.DataFrame()
    account_data = []

    #Extract nested dictionary from df
    for _, row in df.iterrows():
        match_id = row['match_id']
        players = pd.DataFrame(row['players'])
        players['match_id'] = match_id
        account_data.append(players)
    players = pd.concat(account_data, ignore_index=True)


    if debug ==True:
        print(f"**SPLIT DATA, PLAYERS**  : \n\n")
        for i, (key) in enumerate(players.items()):
            if i < 5:
                print(f"Key= {key}\n")

    #drop nested dictionary from df
    match_data = df.drop(columns=['players'])

    if debug ==True:
        print(f"**SPLIT DATA, MATCH: \n")
        for i, (key) in enumerate(match_data.items()):
            if i < 5:
                print(f"Key= {key}")
    
    return match_data, players

def filter_match_data(df):
    return df[MATCH_FILTERS]

def filter_account_data(df):
    return df[PLAYER_FILTERS]

debug = True

def main():
    main_debug = True
    from Deadlock_Data_Fetch import fetch_active_match_data
    print(f"\n******fetching data**** \n")
    raw_match_data = fetch_active_match_data()

    print(f"\n******spliting data**** \n")
    match_data, account_data = split_data(raw_match_data)

    print(f"\n******filtering match data**** \n")
    match_data = filter_match_data(match_data)

    print(f"\n******filtering account data**** \n")
    account_data = filter_account_data(account_data)

    if main_debug == True or debug == True:
        print(f"\nPost filtered match data: \n")
        for i, (key, value) in enumerate(match_data.items()):
            if i < 15:
                print(f"Key= {key}")

        print(f"\nPost filtered account data: \n")
        for i, (key, value) in enumerate(account_data.items()):
            if i < 15:
                print(f"Key= {key}")
    

if __name__ == main():
    main()



