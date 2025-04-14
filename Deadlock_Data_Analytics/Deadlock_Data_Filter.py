import pandas as pd
import random

from config import MATCH_FILTERS

#receive raw_match_data
def split_data(df):
    match_data = pd.DataFrame
    account_data = pd.DataFrame

    #Extract nested dictionary from df
    account_data = df[['players']]
    
    if debug ==True:
        for i, (key, value) in enumerate(account_data.items()):
            if i < 5:
                print(f"player data: \n\nKey= {key} value= {value}\n")

    #drop nested dictionary from df
    match_data = df.drop(columns=['players'])

    if debug ==True:
        print(f"unfiltered match data: \n")
        for i, (key, value) in enumerate(match_data.items()):
            if i < 15:
                print(f"Key= {key}")
    
    return match_data, account_data

def filter_match_data(match_dict):
    filtered_match_data = {key: match_dict[key] for key in MATCH_FILTERS}
    return filtered_match_data

debug = True

def main():
    from Deadlock_Data_Fetch import fetch_match_data
    raw_match_data = fetch_match_data()
    match_data, account_data = split_data(raw_match_data)
    match_data = filter_match_data(match_data)
    if debug ==True:
        print(f"\nPost filtered match data: \n")
        for i, (key, value) in enumerate(match_data.items()):
            if i < 15:
                print(f"Key= {key}")


if __name__ == main():
    main()



