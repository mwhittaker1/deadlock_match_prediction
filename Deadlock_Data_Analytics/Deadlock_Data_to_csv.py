import csv
import pandas as pd

def to_csv(dict):
    
    filtered_matches=dict['matches']
    match_players=dict['match_players"']
    df_filtered_matches = pd.DataFrame(filtered_matches)
    df_match_players = pd.DataFrame(match_players)

    df_filtered_matches.tocsv('filtered_matches.csv', index=False)
    df_match_players.tocsv('match_players.csv', index=False)
    print(".csv's created")



            
