import pandas as pd
from data_orchestrator import orchestrate_match_data, orchestrate_match_history
from services.dl_fetch_data import fetch_match_data
from services.utility_functions import to_csv,to_xlsx


def dev_build_training_data(days,min_average_badge):
    """Collects a set of matches, for each match find players data
    
    in fetch_match_data, define number of matches to pull, max days to consider, and min_badge
    for each match from fetch_match_data, fetch all 12 players history, flatten, and add stats
    no return, saves output to csv and xlsx."""

    print(f"\n\n***Starting Build Training Data ****\n\n")
    training_data = pd.DataFrame()
    df_training_matches, json_training_matches = fetch_match_data(1,10,100) #limit, max days, min badge
    print(f"Starting row iteration of matches, expecting row count= {len(df_training_matches)}")
    match_count=0
    for _,row in df_training_matches.iterrows():
        match_count+=1
        match_id = row['match_id']
        print(f"\n\n*DEBUG* match count={match_count} match_id = {match_id}")

        match = orchestrate_match_data(days,min_average_badge,match_id) #can also pass m_id directly)
        print(f"\n\n*DEBUG* expecting flattened match, match =\n {match}")

        single_match_players_history = orchestrate_match_history(match)
    training_data = pd.concat([training_data, single_match_players_history], ignore_index=True)

    print(f"Training data to xlsx, csv!")
    to_xlsx(training_data,"p_id_match_history")
    to_csv(training_data,"p_id_match_history")
    return

if __name__ == "__main__":
    dev_build_training_data(5,100) # max days, min badge