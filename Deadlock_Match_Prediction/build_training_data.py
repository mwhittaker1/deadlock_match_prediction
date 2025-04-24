import pandas as pd
from data_orchestrator import orchestrate_match_data, orchestrate_match_history
from services.dl_process_data import match_history_outcome_add, win_loss_history
from services.utility_functions import to_csv,to_xlsx
### fetch set of matches for training ###
### Build full dataset for each match ###
## for each hero in match -> hero_stats ##
## for each player, player_match_history -> player_hero_match_history
# df,match_id, account_id, hero_id, player_7d_wl_pct, player_30d_wl_pct, hero_7d_wl_pct, hero_30d_wl_pct, player_hero_total_matches, player_hero_pick_%
#



def dev_build_training_data(limit,days,min_average_badge):
    training_data = pd.DataFrame()
    """Collect dataset for training, {limit} 1-5000"""
    limit = 1
    print(f"\nDEBUG, limit = {limit}, days = {days}")

    match = orchestrate_match_data(limit,days,min_average_badge)
    """Get a {limit} matches of data """
    
    last_m_id = 0
    # iterate over matches, than players
    for m_id in match['match_id'].unique():
        """m_id is not unqiue, checks for next non-duplicate"""
        match_count = 0
        if last_m_id != m_id:
            match_count +=1
            last_m_id = m_id
            single_match_players_history = pd.DataFrame()
            player_count = 0
            #print(f"\n**pulling account_id from data m_id = {match['match_id']}**\n")
            for current_p_id in match['account_id']:
                player_count +=1
                h_id = match['hero_id']
                #print(f"\n\n *** from orchestrate_match_hero_data. p_id = {current_p_id}***")
                p_id_match_history = orchestrate_match_history(current_p_id)
                single_match_players_history = pd.concat([single_match_players_history,p_id_match_history],ignore_index=True)
                print(f"player count for match {m_id} = {player_count}")
            print(f"match count for all matches = {match_count}")
            training_data = pd.concat([training_data, single_match_players_history], ignore_index=True)
    print(f"Training data to xlsx, csv!")
    to_xlsx(training_data,"p_id_match_history")
    to_csv(training_data,"p_id_match_history")
    return

dev_build_training_data(1,1,100)