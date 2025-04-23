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
    """Collect dataset for training, {limit} 1-5000"""
    limit = 1
    print(f"\nDEBUG, limit = {limit}, days = {days}")
    match = orchestrate_match_data(limit,days,min_average_badge)
    """Get a {limit} matches of data """
    
    # iterate over matches, than players
    for m_id in match['match_id']:
        """m_id is not unqiue, checks for next non-duplicate"""
        last_m_id = 0
        if last_m_id != m_id:
            last_m_id = m_id
            print(f"\n**pulling account_id from data m_id = {match['match_id']}**\n")
            for current_p_id in match['account_id']:
                h_id = match['hero_id']
                print(f"\n\n *** from orchestrate_match_hero_data. p_id = {current_p_id}***")
                p_id_match_history = orchestrate_match_history(current_p_id)
                print("1\n")
                p_id_match_history = match_history_outcome_add(p_id_match_history)
                print("2\n")
                p_id_match_history = win_loss_history(p_id_match_history)
                print("3\n")
                p_id_match_history = win_loss_history(p_id_match_history)
                print(f"\n\nprinting to csv, xlsx")
    to_xlsx(p_id_match_history,"p_id_match_history")
    to_csv(p_id_match_history,"p_id_match_history")
    return

dev_build_training_data(1,1,100)